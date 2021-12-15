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
                        if (cond.op == CompOp::IN)
                            for (auto val : cond.rhsValues)
                                cc.vals.push_back(val.pData);
                        else
                            cc.val = cond.rhsValue.pData;
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
        else if (relations.size() == 2)
        {
            std::string leftTableName = relations[0];
            std::string rightTableName = relations[1];
            // get allAttrs
            std::vector<std::string> allAttrName;
            std::vector<std::string> leftAllAttrName;
            std::vector<int> leftAllOffsets;
            std::vector<AttrType> leftAllTypes;
            std::vector<int> leftAllTypeLens;
            std::vector<std::string> rightAllAttrName;
            std::vector<int> rightAllOffsets;
            std::vector<AttrType> rightAllTypes;
            std::vector<int> rightAllTypeLens;
            sm->getAllAttr(leftTableName, leftAllAttrName, leftAllOffsets, leftAllTypes, leftAllTypeLens);
            sm->getAllAttr(rightTableName, rightAllAttrName, rightAllOffsets, rightAllTypes, rightAllTypeLens);
            allAttrName.insert(allAttrName.end(), leftAllAttrName.begin(), leftAllAttrName.end());
            allAttrName.insert(allAttrName.end(), rightAllAttrName.begin(), rightAllAttrName.end());
            int leftTupleLen = leftAllOffsets.back() + leftAllTypeLens.back();
            int rightTupleLen = rightAllOffsets.back() + rightAllTypeLens.back();

            // get selAttrs
            std::vector<std::string> attrName;
            std::vector<int> offsets;
            std::vector<AttrType> types;
            std::vector<int> typeLens;
            if (selAttrs.size() == 0)
            {
                for (int i = 0; i < leftAllAttrName.size(); i++)
                {
                    if (std::find(rightAllAttrName.begin(), rightAllAttrName.end(), leftAllAttrName[i]) != rightAllAttrName.end())
                        attrName.push_back(leftTableName + "." + leftAllAttrName[i]);
                    else
                        attrName.push_back(leftAllAttrName[i]);
                }
                for (int i = 0; i < rightAllAttrName.size(); i++)
                {
                    if (std::find(leftAllAttrName.begin(), leftAllAttrName.end(), rightAllAttrName[i]) != leftAllAttrName.end())
                        attrName.push_back(rightTableName + "." + rightAllAttrName[i]);
                    else
                        attrName.push_back(rightAllAttrName[i]);
                }
                offsets.insert(offsets.end(), leftAllOffsets.begin(), leftAllOffsets.end());
                for (auto off : rightAllOffsets)
                {
                    offsets.push_back(off + leftTupleLen);
                }
                types.insert(types.end(), leftAllTypes.begin(), leftAllTypes.end());
                types.insert(types.end(), rightAllTypes.begin(), rightAllTypes.end());
                typeLens.insert(typeLens.end(), leftAllTypeLens.begin(), leftAllTypeLens.end());
                typeLens.insert(typeLens.end(), rightAllTypeLens.begin(), rightAllTypeLens.end());
            }
            else
            {
                for (auto attr : selAttrs)
                {
                    int offset;
                    AttrType type;
                    int typeLen;
                    sm->getAttrInfo(attr.relName, attr.attrName, offset, type, typeLen);
                    attrName.push_back(attr.relName + "." + attr.attrName);
                    if (std::find(rightAllAttrName.begin(), rightAllAttrName.end(), attr.attrName) != rightAllAttrName.end())
                        offsets.push_back(leftTupleLen + offset);
                    else
                        offsets.push_back(offset);
                    types.push_back(type);
                    typeLens.push_back(typeLen);
                }
            }

            // get value
            FileHandle lfh, rfh;
            rm->OpenFile(sm->openedDbName + "/" + leftTableName, lfh);
            rm->OpenFile(sm->openedDbName + "/" + rightTableName, rfh);
            RID defaultRID(-1, -1);
            FileScan lfs, rfs;
            DataType joinedData = new char[leftTupleLen + rightTupleLen];
            memset(joinedData, 0, leftTupleLen + rightTupleLen);

            if (conditions.size() == 0)
            {
                lfs.openScan(lfh, AttrType::ANY, -1, -1, CompOp::NO, nullptr);
                Record rec;
                Record lrec;
                while (lfs.getNextRec(lrec))
                {
                    DataType ldata;
                    lrec.getData(ldata);
                    rfs.openScan(rfh, AttrType::ANY, -1, -1, CompOp::NO, nullptr);
                    Record rrec;
                    while (rfs.getNextRec(rrec))
                    {
                        DataType rdata;
                        rrec.getData(rdata);
                        memcpy(joinedData, ldata, leftTupleLen);
                        memcpy(joinedData + leftTupleLen, rdata, rightTupleLen);
                        rec.set(defaultRID, joinedData, leftTupleLen + rightTupleLen);
                        results.push_back(rec);
                    }
                    rfs.closeScan();
                }
                lfs.closeScan();
            }
            else
            {
                std::vector<CompareCondition> lconds, rconds, lrconds, rlconds;
                for (auto cond : conditions)
                {
                    if (cond.lhs.relName == relations[0])
                    {
                        CompareCondition cc;
                        auto it = std::find(leftAllAttrName.begin(), leftAllAttrName.end(), cond.lhs.attrName);
                        if (it == leftAllAttrName.end())
                        {
                            std::cout << "Invalid attribute " << cond.lhs.relName << '.' << cond.lhs.attrName << std::endl;
                            return false;
                        }
                        auto idx = std::distance(leftAllAttrName.begin(), it);
                        if (cond.bRhsIsAttr == 1)
                        {
                            if (cond.rhs.relName == relations[0])
                            {
                                cc.rhsAttr = true;
                                auto _it = std::find(leftAllAttrName.begin(), leftAllAttrName.end(), cond.rhs.attrName);
                                if (_it == leftAllAttrName.end())
                                {
                                    std::cout << "Invalid attribute " << cond.rhs.relName << '.' << cond.rhs.attrName << std::endl;
                                    return false;
                                }
                                auto _idx = std::distance(leftAllAttrName.begin(), _it);
                                cc.rhsOffset = leftAllOffsets[_idx];

                                cc.op = cond.op;
                                cc.offset = leftAllOffsets[idx];
                                cc.type = leftAllTypes[idx];
                                cc.len = leftAllTypeLens[idx];
                                cc.attrIdx = idx;
                                lconds.push_back(cc);
                            }
                            else if (cond.rhs.relName == relations[1])
                            {
                                cc.rhsAttr = true;
                                auto _it = std::find(rightAllAttrName.begin(), rightAllAttrName.end(), cond.rhs.attrName);
                                if (_it == rightAllAttrName.end())
                                {
                                    std::cout << "Invalid attribute " << cond.rhs.relName << '.' << cond.rhs.attrName << std::endl;
                                    return false;
                                }
                                auto _idx = std::distance(rightAllAttrName.begin(), _it);

                                cc.rhsOffset = rightAllOffsets[_idx];
                                cc.op = cond.op;
                                cc.offset = leftAllOffsets[idx];
                                cc.type = leftAllTypes[idx];
                                cc.len = leftAllTypeLens[idx];
                                cc.attrIdx = idx;
                                lrconds.push_back(cc);

                                cc.rhsOffset = leftAllOffsets[idx];
                                cc.op = negOp(cond.op);
                                cc.offset = rightAllOffsets[_idx];
                                cc.type = rightAllTypes[_idx];
                                cc.len = rightAllTypeLens[_idx];
                                cc.attrIdx = _idx;
                                rlconds.push_back(cc);
                            }
                            else
                            {
                                std::cout << "Invalid attribute " << cond.rhs.relName << '.' << cond.rhs.attrName << std::endl;
                                return false;
                            }
                        }
                        else
                        {
                            cc.op = cond.op;
                            cc.offset = leftAllOffsets[idx];
                            cc.type = leftAllTypes[idx];
                            cc.len = leftAllTypeLens[idx];
                            cc.rhsAttr = false;
                            if (cond.op == CompOp::IN)
                                for (auto val : cond.rhsValues)
                                    cc.vals.push_back(val.pData);
                            else
                            {
                                cc.val = cond.rhsValue.pData;
                            }
                            cc.attrIdx = idx;
                            lconds.push_back(cc);
                        }
                    }
                    else if (cond.lhs.relName == relations[1])
                    {
                        CompareCondition cc;
                        auto it = std::find(rightAllAttrName.begin(), rightAllAttrName.end(), cond.lhs.attrName);
                        if (it == rightAllAttrName.end())
                        {
                            std::cout << "Invalid attribute " << cond.lhs.relName << '.' << cond.lhs.attrName << std::endl;
                            return false;
                        }
                        auto idx = std::distance(rightAllAttrName.begin(), it);
                        if (cond.bRhsIsAttr == 1)
                        {
                            if (cond.rhs.relName == relations[1])
                            {
                                cc.rhsAttr = true;
                                auto _it = std::find(rightAllAttrName.begin(), rightAllAttrName.end(), cond.rhs.attrName);
                                if (_it == rightAllAttrName.end())
                                {
                                    std::cout << "Invalid attribute " << cond.rhs.relName << '.' << cond.rhs.attrName << std::endl;
                                    return false;
                                }
                                auto _idx = std::distance(rightAllAttrName.begin(), _it);
                                cc.rhsOffset = rightAllOffsets[_idx];

                                cc.op = cond.op;
                                cc.offset = rightAllOffsets[idx];
                                cc.type = rightAllTypes[idx];
                                cc.len = rightAllTypeLens[idx];
                                cc.attrIdx = idx;
                                rconds.push_back(cc);
                            }
                            else if (cond.rhs.relName == relations[0])
                            {
                                cc.rhsAttr = true;
                                auto _it = std::find(leftAllAttrName.begin(), leftAllAttrName.end(), cond.rhs.attrName);
                                if (_it == leftAllAttrName.end())
                                {
                                    std::cout << "Invalid attribute " << cond.rhs.relName << '.' << cond.rhs.attrName << std::endl;
                                    return false;
                                }
                                auto _idx = std::distance(leftAllAttrName.begin(), _it);

                                cc.rhsOffset = rightAllOffsets[idx];
                                cc.op = negOp(cond.op);
                                cc.offset = leftAllOffsets[_idx];
                                cc.type = leftAllTypes[_idx];
                                cc.len = leftAllTypeLens[_idx];
                                cc.attrIdx = _idx;
                                lrconds.push_back(cc);

                                cc.rhsOffset = leftAllOffsets[_idx];
                                cc.op = cond.op;
                                cc.offset = rightAllOffsets[idx];
                                cc.type = rightAllTypes[idx];
                                cc.len = rightAllTypeLens[idx];
                                cc.attrIdx = idx;
                                rlconds.push_back(cc);
                            }
                            else
                            {
                                std::cout << "Invalid attribute " << cond.rhs.relName << '.' << cond.rhs.attrName << std::endl;
                                return false;
                            }
                        }
                        else
                        {
                            cc.op = cond.op;
                            cc.offset = rightAllOffsets[idx];
                            cc.type = rightAllTypes[idx];
                            cc.len = rightAllTypeLens[idx];
                            cc.rhsAttr = false;
                            if (cond.op == CompOp::IN)
                                for (auto val : cond.rhsValues)
                                    cc.vals.push_back(val.pData);
                            else
                                cc.val = cond.rhsValue.pData;
                            cc.attrIdx = idx;
                            rconds.push_back(cc);
                        }
                    }
                    else
                    {
                        std::cout << "Invalid attribute " << cond.lhs.relName << '.' << cond.lhs.attrName << std::endl;
                        return false;
                    }
                }

                // check primary key and index
                bool luse_indexNo = false;
                std::vector<int> lused_indexNo;
                CompOp lused_op = CompOp::NO;
                std::vector<int> lused_keys;
                bool ruse_indexNo = false;
                std::vector<int> rused_indexNo;
                CompOp rused_op = CompOp::NO;
                std::vector<int> rused_keys;

                checkPrimaryKeyAndIndex(leftTableName, lconds, luse_indexNo, lused_indexNo, lused_op, lused_keys);
                checkPrimaryKeyAndIndex(rightTableName, rconds, ruse_indexNo, rused_indexNo, rused_op, rused_keys);

                Record rec;
                if (luse_indexNo)
                {
                    IndexHandle lih;
                    IndexScan lis;
                    im->openIndex(sm->openedDbName + "/" + leftTableName, lused_indexNo, lih);
                    lis.openScan(lih, lused_op, lused_keys);
                    RID lrid;
                    Record lrec;
                    DataType ldata;
                    while (lis.getNextEntry(lrid))
                    {
                        lfh.getRec(lrid, lrec);
                        lrec.getData(ldata);

                        std::vector<CompareCondition> rconds_ext = rconds;
                        for (auto rlc : rlconds)
                        {
                            CompareCondition rc = rlc;
                            memcpy(&rc.val, ldata + rc.rhsOffset, rc.len);
                            rc.rhsAttr = false;
                            rconds_ext.push_back(rc);
                        }

                        checkPrimaryKeyAndIndex(rightTableName, rconds_ext, ruse_indexNo, rused_indexNo, rused_op, rused_keys);

                        Record rec;
                        if (ruse_indexNo)
                        {
                            IndexHandle rih;
                            IndexScan ris;
                            im->openIndex(sm->openedDbName + "/" + rightTableName, rused_indexNo, rih);
                            ris.openScan(rih, rused_op, rused_keys);
                            RID rrid;
                            Record rrec;
                            DataType rdata;
                            while (ris.getNextEntry(rrid))
                            {
                                rfh.getRec(rrid, rrec);
                                rrec.getData(rdata);
                                memcpy(joinedData, ldata, leftTupleLen);
                                memcpy(joinedData + leftTupleLen, rdata, rightTupleLen);
                                rec.set(defaultRID, joinedData, leftTupleLen + rightTupleLen);
                                results.push_back(rec);
                            }
                            ris.closeScan();
                            im->closeIndex(sm->openedDbName + "/" + rightTableName, rused_indexNo);
                        }
                        else
                        {
                            rfs.openScan(rfh, rconds_ext);
                            Record rrec;
                            DataType rdata;
                            while (rfs.getNextRec(rrec))
                            {
                                rrec.getData(rdata);
                                memcpy(joinedData, ldata, leftTupleLen);
                                memcpy(joinedData + leftTupleLen, rdata, rightTupleLen);
                                rec.set(defaultRID, joinedData, leftTupleLen + rightTupleLen);
                                results.push_back(rec);
                            }
                            rfs.closeScan();
                        }
                    }
                    lis.closeScan();
                    im->closeIndex(sm->openedDbName + "/" + leftTableName, lused_indexNo);
                }
                else if (ruse_indexNo)
                {
                    IndexHandle rih;
                    IndexScan ris;
                    im->openIndex(sm->openedDbName + "/" + rightTableName, rused_indexNo, rih);
                    ris.openScan(rih, rused_op, rused_keys);
                    RID rrid;
                    Record rrec;
                    DataType rdata;
                    while (ris.getNextEntry(rrid))
                    {
                        rfh.getRec(rrid, rrec);
                        rrec.getData(rdata);

                        std::vector<CompareCondition> lconds_ext = lconds;
                        for (auto lrc : lrconds)
                        {
                            CompareCondition lc = lrc;
                            memcpy(&lc.val, rdata + lc.rhsOffset, lc.len);
                            lc.rhsAttr = false;
                            lconds_ext.push_back(lc);
                        }

                        checkPrimaryKeyAndIndex(rightTableName, lconds_ext, ruse_indexNo, rused_indexNo, rused_op, rused_keys);

                        Record rec;
                        if (luse_indexNo)
                        {
                            IndexHandle lih;
                            IndexScan lis;
                            im->openIndex(sm->openedDbName + "/" + leftTableName, lused_indexNo, lih);
                            lis.openScan(lih, lused_op, lused_keys);
                            RID lrid;
                            Record lrec;
                            DataType ldata;
                            while (lis.getNextEntry(lrid))
                            {
                                lfh.getRec(lrid, lrec);
                                lrec.getData(ldata);
                                memcpy(joinedData, ldata, leftTupleLen);
                                memcpy(joinedData + leftTupleLen, rdata, rightTupleLen);
                                rec.set(defaultRID, joinedData, leftTupleLen + rightTupleLen);
                                results.push_back(rec);
                            }
                            ris.closeScan();
                            im->closeIndex(sm->openedDbName + "/" + leftTableName, lused_indexNo);
                        }
                        else
                        {
                            lfs.openScan(lfh, lconds_ext);
                            Record lrec;
                            DataType ldata;
                            while (lfs.getNextRec(lrec))
                            {
                                lrec.getData(ldata);
                                memcpy(joinedData, ldata, leftTupleLen);
                                memcpy(joinedData + leftTupleLen, rdata, rightTupleLen);
                                rec.set(defaultRID, joinedData, leftTupleLen + rightTupleLen);
                                results.push_back(rec);
                            }
                            rfs.closeScan();
                        }
                    }
                    ris.closeScan();
                    im->closeIndex(sm->openedDbName + "/" + rightTableName, rused_indexNo);
                }
                else
                {
                    lfs.openScan(lfh, lconds);
                    Record rec;
                    Record lrec;
                    DataType ldata;
                    while (lfs.getNextRec(lrec))
                    {
                        lrec.getData(ldata);

                        std::vector<CompareCondition> rconds_ext = rconds;
                        for (auto rlc : rlconds)
                        {
                            CompareCondition rc = rlc;
                            memcpy(&rc.val, ldata + rc.rhsOffset, rc.len);
                            rc.rhsAttr = false;
                            rconds_ext.push_back(rc);
                        }

                        bool ruse_indexNo = false;
                        std::vector<int> rused_indexNo;
                        CompOp rused_op = CompOp::NO;
                        std::vector<int> rused_keys;
                        checkPrimaryKeyAndIndex(rightTableName, rconds_ext, ruse_indexNo, rused_indexNo, rused_op, rused_keys);

                        Record rec;
                        if (ruse_indexNo)
                        {
                            IndexHandle rih;
                            IndexScan ris;
                            im->openIndex(sm->openedDbName + "/" + rightTableName, rused_indexNo, rih);
                            ris.openScan(rih, rused_op, rused_keys);
                            RID rrid;
                            Record rrec;
                            DataType rdata;
                            while (ris.getNextEntry(rrid))
                            {
                                rfh.getRec(rrid, rrec);
                                rrec.getData(rdata);
                                memcpy(joinedData, ldata, leftTupleLen);
                                memcpy(joinedData + leftTupleLen, rdata, rightTupleLen);
                                rec.set(defaultRID, joinedData, leftTupleLen + rightTupleLen);
                                results.push_back(rec);
                            }
                            ris.closeScan();
                            im->closeIndex(sm->openedDbName + "/" + rightTableName, rused_indexNo);
                        }
                        else
                        {
                            rfs.openScan(rfh, rconds_ext);
                            Record rrec;
                            DataType rdata;
                            while (rfs.getNextRec(rrec))
                            {
                                rrec.getData(rdata);
                                memcpy(joinedData, ldata, leftTupleLen);
                                memcpy(joinedData + leftTupleLen, rdata, rightTupleLen);
                                rec.set(defaultRID, joinedData, leftTupleLen + rightTupleLen);
                                results.push_back(rec);
                            }
                            rfs.closeScan();
                        }
                    }
                    lfs.closeScan();
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

            rm->CloseFile(sm->openedDbName + "/" + leftTableName);
            rm->CloseFile(sm->openedDbName + "/" + rightTableName);
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
                if (updateRecordData(newData, allAttrName, nulls, allOffsets, allTypes, allTypeLens, sets))
                {
                    ih.deleteEntry(keys, ri);
                    keys.clear();
                    for (auto off : primayKey)
                    {
                        int tmp;
                        memcpy(&tmp, newData + off, 4);
                        keys.push_back(tmp);
                    }
                    ih.insertEntry(keys, ri);
                }
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
                    if (updateRecordData(newData, allAttrName, nulls, allOffsets, allTypes, allTypeLens, sets))
                    {
                        ih.deleteEntry(keys, ri);
                        keys.clear();
                        for (auto off : index)
                        {
                            int tmp;
                            memcpy(&tmp, newData + off, 4);
                            keys.push_back(tmp);
                        }
                        ih.insertEntry(keys, ri);
                    }
                }
                im->closeIndex(sm->openedDbName + "/" + tableName, index);
            }
        }

        // update in file
        for (auto ri : results)
        {
            DataType newData;
            fh.getRec(ri, rec);
            rec.getData(newData);
            if (updateRecordData(newData, allAttrName, nulls, allOffsets, allTypes, allTypeLens, sets))
                fh.updateRec(rec);
        }

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
                        if (cond.offset == ind && cond.rhsAttr == false)
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
    bool updateRecordData(DataType tmp, std::vector<std::string> &allAttrName, std::vector<bool> &nulls, std::vector<int> &allOffsets, std::vector<AttrType> &allTypes, std::vector<int> &allTypeLens, std::vector<Condition> sets)
    {
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
        return true;
    }
};
