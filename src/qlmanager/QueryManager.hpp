#pragma once

#include "../recmanager/RecordManager.hpp"
#include "../ixmanager/IndexManager.hpp"
#include "../sysmanager/SystemManager.hpp"
#include "constants.h"
#include <string>
#include <iomanip>
#include <vector>
#include <algorithm>

class QueryManager
{
private:
    SystemManager *sm;
    IndexManager *im;
    RecordManager *rm;

public:
    QueryManager() {}
    QueryManager(SystemManager *_sm, IndexManager *_im, RecordManager *_rm)
    {
        sm = _sm;
        im = _im;
        rm = _rm;
    }
    ~QueryManager()
    {
        sm = nullptr;
        im = nullptr;
        rm = nullptr;
    }

    bool select(const std::vector<RelAttr> &selAttrs, const std::vector<std::string> &relations, const std::vector<Condition> &conditions)
    {
        if (!sm->dbOpened)
        {
            std::cout << "No database used." << std::endl;
            return false;
        }
        if (relations.size() == 1)
        {
            std::string tableName = relations[0];
            // get allAttrs
            std::vector<std::string> allAttrName;
            std::vector<int> allOffsets;
            std::vector<AttrType> allTypes;
            std::vector<int> allTypeLens;
            sm->getAllAttr(tableName, allAttrName, allOffsets, allTypes, allTypeLens);
            // get selAttrs
            std::vector<std::string> attrName;
            std::vector<int> offsets;
            std::vector<AttrType> types;
            std::vector<int> typeLens;
            if (selAttrs.size() == 0)
            {
                attrName = allAttrName;
                offsets = allOffsets;
                types = allTypes;
                typeLens = allTypeLens;
            }
            else
            {
                for (auto attr : selAttrs)
                {
                    int offset;
                    AttrType type;
                    int typeLen;
                    sm->getAttrInfo(tableName, attr.attrName, offset, type, typeLen);
                    attrName.push_back(attr.attrName);
                    offsets.push_back(offset);
                    types.push_back(type);
                    typeLens.push_back(typeLen);
                }
            }
            // get value
            std::vector<Record> results;
            FileHandle fh;
            rm->OpenFile(sm->openedDbName + "/" + tableName, fh);
            FileScan fs;
            printRecHeader(attrName);
            if (conditions.size() == 0)
            {
                fs.openScan(fh, AttrType::ANY, -1, -1, CompOp::NO, nullptr);
                Record rec;
                while (fs.getNextRec(rec))
                    results.push_back(rec);
                fs.closeScan();
            }
            else
            {
                std::vector<CompareCondition> conds;
                for (auto cond : conditions)
                {
                    auto it = std::find(allAttrName.begin(), allAttrName.end(), cond.lhs.attrName);
                    auto idx = std::distance(allAttrName.begin(), it);
                    CompareCondition cc;
                    cc.op = cond.op;
                    cc.offset = allOffsets[idx];
                    cc.type = allTypes[idx];
                    cc.len = allTypeLens[idx];
                    if (cond.op == CompOp::IN)
                        for (auto val : cond.rhsValues)
                            cc.vals.push_back(val.pData);
                    else
                    {
                        if (cond.bRhsIsAttr == 1)
                        {
                            cc.rhsAttr = true;
                            auto _it = std::find(allAttrName.begin(), allAttrName.end(), cond.rhs.attrName);
                            auto _idx = std::distance(allAttrName.begin(), _it);
                            cc.rhsOffset = allOffsets[_idx];
                        }
                        else
                        {
                            cc.rhsAttr = false;
                            cc.val = cond.rhsValue.pData;
                        }
                    }
                    cc.attrIdx = idx;
                    conds.push_back(cc);
                }

                fs.openScan(fh, conds);
                Record rec;
                while (fs.getNextRec(rec))
                    results.push_back(rec);
                fs.closeScan();
            }

            // print
            for (auto rec : results)
            {
                DataType tmp;
                rec.getData(tmp);
                printRecord(tmp, allAttrName, attrName, offsets, types, typeLens);
                std::cout << std::endl;
            }
            printRecEnd(attrName);
            std::cout << results.size() << " record(s) selected." << std::endl;

            rm->CloseFile(sm->openedDbName + "/" + tableName);
        }
        return true;
    }

    bool insert(const std::string &tableName, const std::vector<Value> &values)
    {
        if (!sm->dbOpened)
        {
            std::cout << "No database used." << std::endl;
            return false;
        }
        if (!sm->checkTableExists(tableName))
        {
            std::cout << "Table " << tableName << " not exists." << std::endl;
            return false;
        }
        std::vector<std::string> attrName;
        std::vector<int> offsets;
        std::vector<bool> nulls;
        std::vector<AttrType> types;
        std::vector<int> typeLens;
        std::vector<bool> defaultValids;
        std::vector<defaultValue> defaults;

        sm->getAllAttr(tableName, attrName, offsets, types, typeLens, nulls, defaultValids, defaults);

        int tupleLength = offsets.back() + typeLens.back();
        DataType newData = new char[tupleLength];
        memset(newData, 0, tupleLength);
        SlotMap nullMap(newData, attrName.size());
        for (auto i = 0; i < values.size(); i++)
        {
            if (values[i].type == AttrType::NONE)
            {
                if (!nulls[i])
                {
                    std::cout << "Attribute " << attrName[i] << " cannot be NULL." << std::endl;
                    delete[] newData;
                    return false;
                }
                nullMap.set(i);
            }
            else if (values[i].type != types[i])
            {
                std::cout << "Type mismatched at attribute " << attrName[i] << '.' << std::endl;
                delete[] newData;
                return false;
            }
            if (values[i].len > typeLens[i])
            {
                std::cout << "WARNING: too long attribute " << attrName[i] << " ignored." << std::endl;
                delete[] newData;
                return false;
            }
            memcpy(newData + offsets[i], &values[i].pData, typeLens[i]);
        }
        for (auto i = values.size(); i < attrName.size(); i++)
        {
            if (defaultValids[i])
                memcpy(newData + offsets[i], &defaults[i], typeLens[i]);
        }

        FileHandle fh;
        rm->OpenFile(sm->openedDbName + "/" + tableName, fh);
        RID rid;
        fh.insertRec(newData, rid);
        rm->CloseFile(sm->openedDbName + "/" + tableName);
        delete[] newData;
        return true;
    }

    bool deleta(const std::string &relName, const std::vector<Condition> &conditions) {}

    bool update(const std::string &relName, const RelAttr &updAttr, bool bIsValue, const RelAttr &rhs, const Value &rhsValue, const std::vector<Condition> &conditions) {}

    bool printRecHeader(const std::vector<std::string> &attrName)
    {
        for (auto attr : attrName)
        {
            std::cout << "+" << std::string(attr.size() + 2, '-');
        }
        std::cout << "+" << std::endl;
        for (auto attr : attrName)
        {
            std::cout << "| " << attr << " ";
        }
        std::cout << "|" << std::endl;
        for (auto attr : attrName)
        {
            std::cout << "+" << std::string(attr.size() + 2, '-');
        }
        std::cout << "+" << std::endl;
        return true;
    }

    bool printRecEnd(const std::vector<std::string> &attrName)
    {
        for (auto attr : attrName)
        {
            std::cout << "+" << std::string(attr.size() + 2, '-');
        }
        std::cout << "+" << std::endl;
        return true;
    }

    bool printRecord(const DataType &rec, const std::vector<std::string> &allAttrName, const std::vector<std::string> &attrName, const std::vector<int> &offsets, const std::vector<AttrType> &types, const std::vector<int> typeLens)
    {
        SlotMap nullMap(rec, allAttrName.size());
        for (int i = 0; i < attrName.size(); i++)
        {
            auto it = std::find(allAttrName.begin(), allAttrName.end(), attrName[i]);
            auto idx = std::distance(allAttrName.begin(), it);
            std::cout << "|" << std::left << std::setw(attrName[i].size() + 2);
            if (nullMap.test(idx))
                std::cout << "NULL";
            else
            {
                switch (types[i])
                {
                case AttrType::INT:
                {
                    int *val = new int;
                    memcpy(val, rec + offsets[i], sizeof(int));
                    std::cout << *val;
                    break;
                }
                case AttrType::FLOAT:
                {
                    float *val = new float;
                    memcpy(val, rec + offsets[i], sizeof(float));
                    std::cout << *val;
                    break;
                }
                case AttrType::VARCHAR:
                {
                    char *val = new char[typeLens[i]];
                    memcpy(val, rec + offsets[i], typeLens[i]);
                    std::cout << val;
                    break;
                }
                default:
                    std::cout << "NULL";
                    break;
                }
            }
        }
        std::cout << "|";
        return true;
    }
};
