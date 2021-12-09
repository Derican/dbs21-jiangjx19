#pragma once

#include "../recmanager/RecordManager.hpp"
#include "../ixmanager/IndexManager.hpp"
#include "../ixmanager/IndexScan.hpp"
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

    bool select(const std::vector<RelAttr> &selAttrs, const std::vector<std::string> &relations, const std::vector<Condition> &conditions, std::vector<Record> &results, const bool final)
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
            FileHandle fh;
            rm->OpenFile(sm->openedDbName + "/" + tableName, fh);
            FileScan fs;

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

                // check primary key and index
                bool use_indexNo = false;
                std::vector<int> used_indexNo;
                CompOp used_op = CompOp::NO;
                std::vector<int> used_keys;
                checkPrimaryKeyAndIndex(tableName, conds, use_indexNo, used_indexNo, used_op, used_keys);

                if (use_indexNo)
                {
                    IndexHandle ih;
                    IndexScan is;
                    im->openIndex(sm->openedDbName + "/" + tableName, used_indexNo, ih);
                    is.openScan(ih, used_op, used_keys);
                    RID rid;
                    Record rec;
                    while (is.getNextEntry(rid))
                    {
                        fh.getRec(rid, rec);
                        results.push_back(rec);
                    }
                    is.closeScan();
                    im->closeIndex(sm->openedDbName + "/" + tableName, used_indexNo);
                }
                else
                {
                    fs.openScan(fh, conds);
                    Record rec;
                    while (fs.getNextRec(rec))
                        results.push_back(rec);
                    fs.closeScan();
                }
            }

            // print
            if (final)
            {
                printRecHeader(attrName);
                for (auto rec : results)
                {
                    DataType tmp;
                    rec.getData(tmp);
                    printRecord(tmp, allAttrName, attrName, offsets, types, typeLens);
                    std::cout << std::endl;
                }
                printRecEnd(attrName);
                std::cout << results.size() << " record(s) selected." << std::endl;
            }

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
        FileScan fs;
        IndexHandle ih;
        IndexScan is;
        RID rid(-1, -1);
        // check primary key
        std::vector<int> primayKey;
        sm->getPrimaryKey(tableName, primayKey);
        if (primayKey.size() > 0)
        {
            std::vector<int> keys;
            for (auto off : primayKey)
            {
                int tmp;
                memcpy(&tmp, newData + off, 4);
                keys.push_back(tmp);
            }
            im->openIndex(sm->openedDbName + "/" + tableName, primayKey, ih);
            is.openScan(ih, CompOp::E, keys);
            while (is.getNextEntry(rid))
            {
                break;
            }
            is.closeScan();

            if (rid.valid())
            {
                std::cout << "Duplicate primary key." << std::endl;
                im->closeIndex(sm->openedDbName + "/" + tableName, primayKey);
                return false;
            }
            im->closeIndex(sm->openedDbName + "/" + tableName, primayKey);
        }

        // check all indexes
        std::vector<std::vector<int>> indexNo;
        sm->getAllIndex(tableName, indexNo);
        // if (indexNo.size() > 0)
        // {
        //     for (auto index : indexNo)
        //     {
        //         std::vector<int> keys;
        //         for (auto off : index)
        //         {
        //             int tmp;
        //             memcpy(&tmp, newData + off, 4);
        //             keys.push_back(tmp);
        //         }
        //         im->openIndex(sm->openedDbName + "/" + tableName, index, ih);
        //         is.openScan(ih, CompOp::E, keys);
        //         while (is.getNextEntry(rid))
        //         {
        //             break;
        //         }
        //         is.closeScan();

        //         if (rid.valid())
        //         {
        //             std::cout << "Duplicate index." << std::endl;
        //             im->closeIndex(sm->openedDbName + "/" + tableName, index);
        //             return false;
        //         }
        //         im->closeIndex(sm->openedDbName + "/" + tableName, index);
        //     }
        // }

        // finally insert
        rm->OpenFile(sm->openedDbName + "/" + tableName, fh);
        fh.insertRec(newData, rid);
        rm->CloseFile(sm->openedDbName + "/" + tableName);
        if (primayKey.size() > 0)
        {
            std::vector<int> keys;
            for (auto off : primayKey)
            {
                int tmp;
                memcpy(&tmp, newData + off, 4);
                keys.push_back(tmp);
            }
            im->openIndex(sm->openedDbName + "/" + tableName, primayKey, ih);
            ih.insertEntry(keys, rid);
            im->closeIndex(sm->openedDbName + "/" + tableName, primayKey);
        }
        if (indexNo.size() > 0)
        {
            for (auto index : indexNo)
            {
                std::vector<int> keys;
                for (auto off : index)
                {
                    int tmp;
                    memcpy(&tmp, newData + off, 4);
                    keys.push_back(tmp);
                }
                im->openIndex(sm->openedDbName + "/" + tableName, index, ih);
                ih.insertEntry(keys, rid);
                im->closeIndex(sm->openedDbName + "/" + tableName, index);
            }
        }
        delete[] newData;
        return true;
    }

    bool deleta(const std::string &tableName, const std::vector<Condition> &conditions)
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

        // get allAttrs
        std::vector<std::string> allAttrName;
        std::vector<int> allOffsets;
        std::vector<AttrType> allTypes;
        std::vector<int> allTypeLens;
        sm->getAllAttr(tableName, allAttrName, allOffsets, allTypes, allTypeLens);
        // get value
        FileHandle fh;
        rm->OpenFile(sm->openedDbName + "/" + tableName, fh);
        FileScan fs;

        // translate conditions
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

        // get all indexes
        std::vector<int> primayKey;
        sm->getPrimaryKey(tableName, primayKey);
        std::vector<std::vector<int>> indexNo;
        sm->getAllIndex(tableName, indexNo);

        // check primary key and index
        bool use_indexNo = false;
        std::vector<int> used_indexNo;
        CompOp used_op = CompOp::NO;
        std::vector<int> used_keys;
        checkPrimaryKeyAndIndex(tableName, conds, use_indexNo, used_indexNo, used_op, used_keys);

        // scan for results
        std::vector<RID> results;
        IndexHandle ih;
        IndexScan is;
        Record rec;
        if (use_indexNo)
        {
            im->openIndex(sm->openedDbName + "/" + tableName, used_indexNo, ih);
            is.openScan(ih, used_op, used_keys);
            RID rid;
            while (is.getNextEntry(rid))
            {
                results.push_back(rid);
            }
            is.closeScan();
            im->closeIndex(sm->openedDbName + "/" + tableName, used_indexNo);
        }
        else
        {
            fs.openScan(fh, conds);
            while (fs.getNextRec(rec))
            {
                RID rid;
                rec.getRID(rid);
                results.push_back(rid);
            }
            fs.closeScan();
        }

        // check primary key
        if (primayKey.size() > 0)
        {
            im->openIndex(sm->openedDbName + "/" + tableName, primayKey, ih);
            for (auto ri : results)
            {
                DataType newData;
                fh.getRec(ri, rec);
                rec.getData(newData);
                std::vector<int> keys;
                for (auto off : primayKey)
                {
                    int tmp;
                    memcpy(&tmp, newData + off, 4);
                    keys.push_back(tmp);
                }
                ih.deleteEntry(keys, ri);
            }
            im->closeIndex(sm->openedDbName + "/" + tableName, primayKey);
        }

        // check indexes
        if (indexNo.size() > 0)
        {
            for (auto index : indexNo)
            {
                im->openIndex(sm->openedDbName + "/" + tableName, index, ih);
                for (auto ri : results)
                {
                    DataType newData;
                    fh.getRec(ri, rec);
                    rec.getData(newData);
                    std::vector<int> keys;
                    for (auto off : index)
                    {
                        int tmp;
                        memcpy(&tmp, newData + off, 4);
                        keys.push_back(tmp);
                    }
                    ih.deleteEntry(keys, ri);
                }
                im->closeIndex(sm->openedDbName + "/" + tableName, index);
            }
        }

        // delete in file
        for (auto ri : results)
            fh.deleteRec(ri);

        rm->CloseFile(sm->openedDbName + "/" + tableName);
        return true;
    }

    bool update(const std::string &tableName, const std::vector<Condition> &sets, const std::vector<Condition> &conditions)
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

        // get allAttrs
        std::vector<std::string> allAttrName;
        std::vector<int> allOffsets;
        std::vector<AttrType> allTypes;
        std::vector<int> allTypeLens;
        std::vector<bool> nulls;
        std::vector<bool> defaultValids;
        std::vector<defaultValue> defaults;

        sm->getAllAttr(tableName, allAttrName, allOffsets, allTypes, allTypeLens, nulls, defaultValids, defaults);
        // get value
        FileHandle fh;
        rm->OpenFile(sm->openedDbName + "/" + tableName, fh);
        FileScan fs;

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
        {
            RID rid;
            rec.getRID(rid);
            DataType tmp;
            rec.getData(tmp);

            SlotMap nullMap(tmp, allAttrName.size());
            for (auto set_c : sets)
            {
                auto it = std::find(allAttrName.begin(), allAttrName.end(), set_c.lhs.attrName);
                auto idx = std::distance(allAttrName.begin(), it);

                if (set_c.rhsValue.type == AttrType::NONE)
                {
                    if (!nulls[idx])
                    {
                        std::cout << "Attribute " << allAttrName[idx] << " cannot be NULL." << std::endl;
                        return false;
                    }
                    nullMap.set(idx);
                }
                else if (set_c.rhsValue.type != allTypes[idx])
                {
                    std::cout << "Type mismatched at attribute " << allAttrName[idx] << '.' << std::endl;
                    return false;
                }
                else
                    nullMap.remove(idx);
                if (set_c.rhsValue.len > allTypeLens[idx])
                {
                    std::cout << "WARNING: too long attribute " << allAttrName[idx] << " ignored." << std::endl;
                    return false;
                }

                memcpy(tmp + allOffsets[idx], &set_c.rhsValue.pData, allTypeLens[idx]);
            }
            fh.updateRec(rec);
        }
        fs.closeScan();
        rm->CloseFile(sm->openedDbName + "/" + tableName);
        return true;
    }

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

    bool checkPrimaryKeyAndIndex(const std::string &tableName, const std::vector<CompareCondition> &conds, bool &use_indexNo, std::vector<int> &used_indexNo, CompOp &used_op, std::vector<int> &used_keys)
    {
        std::vector<std::vector<int>> indexNo;
        sm->getPrimaryKeyAndIndex(tableName, indexNo);
        if (indexNo.size() > 0)
        {
            for (auto index : indexNo)
            {
                bool use_index = true;
                CompOp tmp_op = CompOp::NO;
                std::vector<int> tmp_keys;
                if (index.size() != conds.size())
                    continue;
                for (auto ind : index)
                {
                    bool flag = false;
                    for (auto cond : conds)
                    {
                        if (cond.offset == ind)
                        {
                            if (tmp_op == CompOp::NO)
                                tmp_op = cond.op;
                            if (tmp_op <= 4 && tmp_op == cond.op)
                            {
                                flag = true;
                                tmp_keys.push_back(cond.val.Int);
                                break;
                            }
                        }
                    }
                    if (!flag)
                    {
                        use_index = false;
                        break;
                    }
                }
                if (use_index && index.size() > used_indexNo.size())
                {
                    use_indexNo = true;
                    used_indexNo = index;
                    used_op = tmp_op;
                    used_keys = tmp_keys;
                }
            }
        }
    }
};
