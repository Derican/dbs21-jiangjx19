#pragma once

#include "../recmanager/RecordManager.hpp"
#include "../ixmanager/IndexManager.hpp"
#include "../sysmanager/SystemManager.hpp"
#include "constants.h"
#include <string>
#include <vector>

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
        if (relations.size() == 1)
        {
            std::string tableName = relations[0];
            // get selAttrs offset
            std::vector<std::string> attrName;
            std::vector<int> offsets;
            std::vector<AttrType> types;
            std::vector<int> typeLens;
            if (selAttrs.size() == 1 && selAttrs[0].attrName == "*")
            {
                sm->getAllAttr(tableName, attrName, offsets, types, typeLens);
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
            rm->OpenFile(tableName, fh);
            FileScan fs;
            if (conditions.size() == 0)
            {
                fs.openScan(fh, AttrType::ANY, -1, -1, CompOp::NO, nullptr);
                Record rec;
                while (fs.getNextRec(rec))
                {
                    DataType tmp;
                    rec.getData(tmp);
                    for (int i = 0; i < attrName.size(); i++)
                    {
                        switch (types[i])
                        {
                        case AttrType::INT:
                        {
                            int *val = new int;
                            memcpy(val, tmp + offsets[i], sizeof(int) / 4);
                            std::cout << *val << ' ';
                            break;
                        }
                        case AttrType::FLOAT:
                        {
                            float *val = new float;
                            memcpy(val, tmp + offsets[i], sizeof(float) / 4);
                            std::cout << *val << ' ';
                            break;
                        }
                        case AttrType::VARCHAR:
                        {
                            char *val = new char[offsets[i]];
                            memcpy(val, tmp + offsets[i], (offsets[i] + 1) / 4 - 1);
                            std::cout << val << ' ';
                            break;
                        }
                        default:
                            break;
                        }
                    }
                    std::cout << std::endl;
                }
            }
        }
    }

    bool insert(const std::string &relName, const std::vector<Value> &values) {}

    bool deleta(const std::string &relName, const std::vector<Condition> &conditions) {}

    bool update(const std::string &relName, const RelAttr &updAttr, bool bIsValue, const RelAttr &rhs, const Value &rhsValue, const std::vector<Condition> &conditions) {}
};
