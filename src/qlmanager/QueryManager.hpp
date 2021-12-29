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

    bool select(const std::vector<RelAttr> &selAttrs, const std::vector<std::string> &relations, std::vector<Condition> &conditions, std::vector<Record> &results, const bool final)
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
            rm->openFile(sm->openedDbName + "/" + tableName, fh);
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
                if (optimizeConditions(conditions))
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
                            if (cond.op == CompOp::IN || cond.op >= 10)
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
                    std::vector<CompareCondition> otherConds;
                    checkPrimaryKeyAndIndex(tableName, conds, use_indexNo, used_indexNo, used_op, used_keys, otherConds);

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
                            DataType tmp;
                            rec.getData(tmp);
                            if (fs.compareMultiple(tmp, otherConds))
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

            rm->closeFile(sm->openedDbName + "/" + tableName);
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
            rm->openFile(sm->openedDbName + "/" + leftTableName, lfh);
            rm->openFile(sm->openedDbName + "/" + rightTableName, rfh);
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
                if (optimizeConditions(conditions))
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
                                if (cond.op == CompOp::IN || cond.op >= 10)
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
                                if (cond.op == CompOp::IN || cond.op >= 10)
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
                    std::vector<CompareCondition> lotherConds;
                    std::vector<CompareCondition> rotherConds;

                    checkPrimaryKeyAndIndex(leftTableName, lconds, luse_indexNo, lused_indexNo, lused_op, lused_keys, lotherConds);
                    checkPrimaryKeyAndIndex(rightTableName, rconds, ruse_indexNo, rused_indexNo, rused_op, rused_keys, rotherConds);

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

                            if (!lfs.compareMultiple(ldata, lotherConds))
                                continue;

                            std::vector<CompareCondition> rconds_ext = rconds;
                            for (auto rlc : rlconds)
                            {
                                CompareCondition rc = rlc;
                                memcpy(&rc.val, ldata + rc.rhsOffset, rc.len);
                                rc.rhsAttr = false;
                                rconds_ext.push_back(rc);
                            }

                            checkPrimaryKeyAndIndex(rightTableName, rconds_ext, ruse_indexNo, rused_indexNo, rused_op, rused_keys, rotherConds);

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
                                    if (!rfs.compareMultiple(rdata, rotherConds))
                                        continue;
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

                            if (!rfs.compareMultiple(rdata, rotherConds))
                                continue;

                            std::vector<CompareCondition> lconds_ext = lconds;
                            for (auto lrc : lrconds)
                            {
                                CompareCondition lc = lrc;
                                memcpy(&lc.val, rdata + lc.rhsOffset, lc.len);
                                lc.rhsAttr = false;
                                lconds_ext.push_back(lc);
                            }

                            checkPrimaryKeyAndIndex(leftTableName, lconds_ext, luse_indexNo, lused_indexNo, lused_op, lused_keys, lotherConds);

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
                                    if (!lfs.compareMultiple(ldata, lotherConds))
                                        continue;
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
                            checkPrimaryKeyAndIndex(rightTableName, rconds_ext, ruse_indexNo, rused_indexNo, rused_op, rused_keys, rotherConds);

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
                                    if (!rfs.compareMultiple(rdata, rotherConds))
                                        continue;
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

            rm->closeFile(sm->openedDbName + "/" + leftTableName);
            rm->closeFile(sm->openedDbName + "/" + rightTableName);
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

        // get table info
        std::vector<std::string> attrName;
        std::vector<int> offsets;
        std::vector<bool> nulls;
        std::vector<AttrType> types;
        std::vector<int> typeLens;
        std::vector<bool> defaultValids;
        std::vector<defaultValue> defaults;

        sm->getAllAttr(tableName, attrName, offsets, types, typeLens, nulls, defaultValids, defaults);

        // get new data
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
                delete[] newData;
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

        // check unique
        std::vector<std::vector<int>> uniqueNo;
        sm->getUnique(tableName, uniqueNo);
        bool flag = false;
        for (auto uniq : uniqueNo)
        {
            rm->openFile(sm->openedDbName + "/" + tableName, fh);
            DataType data;
            Record rec;
            fs.openScan(fh, AttrType::ANY, 4, 0, CompOp::NO, nullptr);
            while (fs.getNextRec(rec))
            {
                flag = true;
                rec.getData(data);
                for (auto i = 0; i < uniq.size() && flag; i++)
                {
                    auto it = std::find(offsets.begin(), offsets.end(), uniq[i]);
                    auto idx = std::distance(offsets.begin(), it);
                    if (memcmp(newData + uniq[i], data + uniq[i], typeLens[idx]) != 0)
                        flag = false;
                }
                if (flag)
                    break;
            }
            fs.closeScan();
            rm->closeFile(sm->openedDbName + "/" + tableName);
            if (flag)
                break;
        }
        if (flag)
        {
            std::cout << "Duplicate value." << std::endl;
            delete[] newData;
            return false;
        }

        // check foreign key
        std::vector<std::string> refTableName, foreignNames;
        std::vector<std::vector<int>> foreignNo, refForeignNo;
        sm->getForeign(tableName, refTableName, foreignNo, refForeignNo, foreignNames);
        flag = true;
        for (int i = 0; i < refTableName.size() && flag; i++)
        {
            flag = false;
            std::vector<int> keys;
            for (int j = 0; j < foreignNo[i].size(); j++)
            {
                int k;
                memcpy(&k, newData + foreignNo[i][j], 4);
                keys.push_back(k);
            }
            IndexHandle ih;
            im->openIndex(sm->openedDbName + "/" + refTableName[i], refForeignNo[i], ih);
            flag = ih.searchEntry(keys);
            im->closeIndex(sm->openedDbName + "/" + refTableName[i], refForeignNo[i]);
            if (!flag)
            {
                std::cout << "ERROR: Foreign key (";
                for (auto k : keys)
                    std::cout << k << " ";
                std::cout << ") doesn't match any in the reference table " << refTableName[i] << std::endl;
                return false;
            }
        }

        // finally insert
        rm->openFile(sm->openedDbName + "/" + tableName, fh);
        fh.insertRec(newData, rid);
        rm->closeFile(sm->openedDbName + "/" + tableName);
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
        rm->openFile(sm->openedDbName + "/" + tableName, fh);
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
            if (cond.op == CompOp::IN || cond.op >= 10)
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
        std::vector<CompareCondition> otherConds;
        checkPrimaryKeyAndIndex(tableName, conds, use_indexNo, used_indexNo, used_op, used_keys, otherConds);

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
                fh.getRec(rid, rec);
                DataType tmp;
                rec.getData(tmp);
                if (!fs.compareMultiple(tmp, otherConds))
                    continue;
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

        // check ref
        std::vector<std::string> foreignTable;
        std::vector<std::vector<int>> refIndexNo, foreignIndexNo;
        sm->getReference(tableName, foreignTable, refIndexNo, foreignIndexNo);
        for (int i = 0; i < foreignTable.size(); i++)
        {
            im->openIndex(sm->openedDbName + "/" + foreignTable[i], foreignIndexNo[i], ih);
            std::vector<RID> foreignRID;
            for (auto ri : results)
            {
                DataType newData;
                fh.getRec(ri, rec);
                rec.getData(newData);
                std::vector<int> keys;
                for (auto off : refIndexNo[i])
                {
                    int tmp;
                    memcpy(&tmp, newData + off, 4);
                    keys.push_back(tmp);
                }
                is.openScan(ih, CompOp::E, keys);
                RID fr;
                while (is.getNextEntry(fr))
                    foreignRID.push_back(fr);
                is.closeScan();
            }
            im->closeIndex(sm->openedDbName + "/" + foreignTable[i], foreignIndexNo[i]);
            deleta(foreignTable[i], foreignRID);
        }

        // delete in file
        for (auto ri : results)
            fh.deleteRec(ri);

        rm->closeFile(sm->openedDbName + "/" + tableName);
        return true;
    }

    bool deleta(const std::string &tableName, const std::vector<RID> &results)
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

        FileHandle fh;
        rm->openFile(sm->openedDbName + "/" + tableName, fh);
        FileScan fs;
        Record rec;
        IndexHandle ih;
        IndexScan is;

        // get all indexes
        std::vector<int> primayKey;
        sm->getPrimaryKey(tableName, primayKey);
        std::vector<std::vector<int>> indexNo;
        sm->getAllIndex(tableName, indexNo);

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

        // check ref
        std::vector<std::string> foreignTable;
        std::vector<std::vector<int>> refIndexNo, foreignIndexNo;
        sm->getReference(tableName, foreignTable, refIndexNo, foreignIndexNo);
        for (int i = 0; i < foreignTable.size(); i++)
        {
            im->openIndex(sm->openedDbName + "/" + foreignTable[i], foreignIndexNo[i], ih);
            for (auto ri : results)
            {
                DataType newData;
                fh.getRec(ri, rec);
                rec.getData(newData);
                std::vector<int> keys;
                for (auto off : refIndexNo[i])
                {
                    int tmp;
                    memcpy(&tmp, newData + off, 4);
                    keys.push_back(tmp);
                }
                std::vector<RID> foreignRID;
                is.openScan(ih, CompOp::E, keys);
                RID fr;
                while (is.getNextEntry(fr))
                    foreignRID.push_back(fr);
                is.closeScan();
                deleta(foreignTable[i], foreignRID);
            }
            im->closeIndex(sm->openedDbName + "/" + foreignTable[i], foreignIndexNo[i]);
        }

        // delete in file
        for (auto ri : results)
            fh.deleteRec(ri);

        rm->closeFile(sm->openedDbName + "/" + tableName);
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
        int tupleLen = allOffsets.back() + allTypeLens.back();
        // get value
        FileHandle fh;
        rm->openFile(sm->openedDbName + "/" + tableName, fh);
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
            if (cond.op == CompOp::IN || cond.op >= 10)
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
        std::vector<CompareCondition> otherConds;
        checkPrimaryKeyAndIndex(tableName, conds, use_indexNo, used_indexNo, used_op, used_keys, otherConds);

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
                fh.getRec(rid, rec);
                DataType tmp;
                rec.getData(tmp);
                if (!fs.compareMultiple(tmp, otherConds))
                    continue;
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

        // check unique
        std::vector<std::vector<int>> uniqueNo;
        sm->getUnique(tableName, uniqueNo);
        for (auto ri : results)
        {
            bool flag = false;
            DataType newData;
            fh.getRec(ri, rec);
            rec.getData(newData);
            if (!updateRecordData(newData, allAttrName, nulls, allOffsets, allTypes, allTypeLens, sets))
                continue;
            for (auto uniq : uniqueNo)
            {
                DataType data;
                Record rec;
                fs.openScan(fh, AttrType::ANY, 4, 0, CompOp::NO, nullptr);
                while (fs.getNextRec(rec))
                {
                    flag = true;
                    rec.getData(data);
                    for (auto i = 0; i < uniq.size() && flag; i++)
                    {
                        auto it = std::find(allOffsets.begin(), allOffsets.end(), uniq[i]);
                        auto idx = std::distance(allOffsets.begin(), it);
                        if (memcmp(newData + uniq[i], data + uniq[i], allTypeLens[idx]) != 0)
                            flag = false;
                    }
                    if (flag)
                        break;
                }
                fs.closeScan();
                if (flag)
                    break;
            }
            if (flag)
            {
                std::cout << "Duplicate value." << std::endl;
                rm->closeFile(sm->openedDbName + "/" + tableName);
                return false;
            }
        }

        // check ref
        std::vector<std::string> refTableName, foreignNames;
        std::vector<std::vector<int>> foreignNo, refForeignNo;
        sm->getForeign(tableName, refTableName, foreignNo, refForeignNo, foreignNames);
        bool flag = true;
        for (auto ri : results)
        {
            DataType newData;
            fh.getRec(ri, rec);
            rec.getData(newData);
            if (!updateRecordData(newData, allAttrName, nulls, allOffsets, allTypes, allTypeLens, sets))
                continue;
            for (int i = 0; i < refTableName.size() && flag; i++)
            {
                flag = false;
                std::vector<int> keys;
                for (int j = 0; j < foreignNo[i].size(); j++)
                {
                    int k;
                    memcpy(&k, newData + foreignNo[i][j], 4);
                    keys.push_back(k);
                }
                IndexHandle ih;
                im->openIndex(sm->openedDbName + "/" + refTableName[i], refForeignNo[i], ih);
                flag = ih.searchEntry(keys);
                im->closeIndex(sm->openedDbName + "/" + refTableName[i], refForeignNo[i]);
                if (!flag)
                {
                    std::cout << "ERROR: Foreign key (";
                    for (auto k : keys)
                        std::cout << k << " ";
                    std::cout << ") doesn't match any in the reference table " << refTableName[i] << std::endl;
                    rm->closeFile(sm->openedDbName + "/" + tableName);
                    return false;
                }
            }
        }

        // check and update primary key
        if (primayKey.size() > 0)
        {
            im->openIndex(sm->openedDbName + "/" + tableName, primayKey, ih);
            for (auto ri : results)
            {
                DataType newData;
                fh.getRec(ri, rec);
                rec.getData(newData);
                std::vector<int> keys, newKeys;
                for (auto off : primayKey)
                {
                    int tmp;
                    memcpy(&tmp, newData + off, 4);
                    keys.push_back(tmp);
                }
                if (updateRecordData(newData, allAttrName, nulls, allOffsets, allTypes, allTypeLens, sets))
                {
                    for (auto off : primayKey)
                    {
                        int tmp;
                        memcpy(&tmp, newData + off, 4);
                        newKeys.push_back(tmp);
                    }
                    if (keys != newKeys && ih.searchEntry(newKeys))
                    {
                        std::cout << "Duplicate primary key." << std::endl;
                        im->closeIndex(sm->openedDbName + "/" + tableName, primayKey);
                        return false;
                    }
                    ih.deleteEntry(keys, ri);
                    ih.insertEntry(newKeys, ri);
                }
            }
            im->closeIndex(sm->openedDbName + "/" + tableName, primayKey);
        }

        // update indexes
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

        // update foreign
        std::vector<std::string> foreignTable;
        std::vector<std::vector<int>> refIndexNo, foreignIndexNo;
        sm->getReference(tableName, foreignTable, refIndexNo, foreignIndexNo);
        for (auto ri : results)
        {
            DataType newData;
            fh.getRec(ri, rec);
            rec.getData(newData);
            DataType oldData = new char[tupleLen];
            memcpy(oldData, newData, tupleLen);
            if (!updateRecordData(newData, allAttrName, nulls, allOffsets, allTypes, allTypeLens, sets))
            {
                delete[] oldData;
                continue;
            }
            for (int i = 0; i < foreignTable.size(); i++)
            {
                std::vector<std::string> allFornAttrName;
                std::vector<int> allFornOffsets;
                std::vector<AttrType> allFornTypes;
                std::vector<int> allFornTypeLens;
                sm->getAllAttr(foreignTable[i], allFornAttrName, allFornOffsets, allFornTypes, allFornTypeLens);

                std::vector<Condition> fornSets, fornConds;
                for (int j = 0; j < foreignIndexNo[i].size(); j++)
                {
                    Condition cond;
                    auto it = std::find(allFornOffsets.begin(), allFornOffsets.end(), foreignIndexNo[i][j]);
                    auto idx = std::distance(allFornOffsets.begin(), it);
                    cond.lhs = RelAttr(foreignTable[i], allFornAttrName[idx]);
                    cond.op = CompOp::E;
                    cond.bRhsIsAttr = 0;
                    memcpy(&cond.rhsValue.pData, oldData + refIndexNo[i][j], 4);
                    cond.rhsValue.len = 4;
                    cond.rhsValue.type = AttrType::INT;
                    fornConds.push_back(cond);

                    memcpy(&cond.rhsValue.pData, newData + refIndexNo[i][j], 4);
                    fornSets.push_back(cond);
                }
                update(foreignTable[i], fornSets, fornConds);
            }
            delete[] oldData;
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

        rm->closeFile(sm->openedDbName + "/" + tableName);
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
                    delete val;
                    break;
                }
                case AttrType::FLOAT:
                {
                    float *val = new float;
                    memcpy(val, rec + offsets[i], sizeof(float));
                    std::cout << *val;
                    delete val;
                    break;
                }
                case AttrType::VARCHAR:
                {
                    char *val = new char[typeLens[i] + 1];
                    memset(val, 0, typeLens[i] + 1);
                    memcpy(val, rec + offsets[i], typeLens[i]);
                    std::cout << val;
                    delete[] val;
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

    bool checkPrimaryKeyAndIndex(const std::string &tableName, const std::vector<CompareCondition> &conds, bool &use_indexNo, std::vector<int> &used_indexNo, CompOp &used_op, std::vector<int> &used_keys, std::vector<CompareCondition> &otherConds)
    {
        use_indexNo = false;
        used_indexNo.clear();
        used_keys.clear();
        otherConds.clear();

        std::vector<std::vector<int>> indexNo;
        sm->getPrimaryKeyAndIndex(tableName, indexNo);
        if (indexNo.size() > 0)
        {
            for (auto index : indexNo)
            {
                if (index.size() > conds.size())
                    continue;
                std::vector<CompareCondition> sortedConds(index.size());
                std::vector<bool> sortedCondsOccupied(index.size(), false);
                std::vector<CompareCondition> tmp_other;
                for (auto cond : conds)
                {
                    auto it = std::find(index.begin(), index.end(), cond.offset);
                    auto idx = std::distance(index.begin(), it);
                    if (it == index.end() || cond.rhsAttr || (cond.op > 4 && cond.op < 10) || sortedCondsOccupied[idx])
                        tmp_other.push_back(cond);
                    else
                    {
                        sortedConds[idx] = cond;
                        sortedCondsOccupied[idx] = true;
                    }
                }
                size_t i = 0;
                CompOp tmp_op = CompOp::E;
                std::vector<int> tmp_keys, tmp_keys_a, tmp_keys_b;
                bool stop = false;
                while (i < sortedConds.size() && !stop)
                {
                    stop = true;
                    if (!sortedCondsOccupied[i])
                        break;
                    if (tmp_op != E && tmp_op != sortedConds[i].op)
                        break;
                    if (sortedConds[i].op == CompOp::E)
                    {
                        stop = false;
                        tmp_keys_a.push_back(sortedConds[i].val.Int);
                        tmp_keys_b.push_back(sortedConds[i].val.Int);
                    }
                    else if (sortedConds[i].op < 4)
                    {
                        tmp_keys_a.push_back(sortedConds[i].val.Int);
                        tmp_keys_b.push_back(sortedConds[i].val.Int);
                    }
                    else
                    {
                        tmp_keys_a.push_back(sortedConds[i].vals[0].Int);
                        tmp_keys_b.push_back(sortedConds[i].vals[1].Int);
                    }
                    tmp_op = sortedConds[i].op;
                    i++;
                }
                tmp_keys.insert(tmp_keys.end(), tmp_keys_a.begin(), tmp_keys_a.end());
                if (tmp_op >= 10)
                    tmp_keys.insert(tmp_keys.end(), tmp_keys_b.begin(), tmp_keys_b.end());

                if (i == index.size() && index.size() > used_indexNo.size())
                {
                    use_indexNo = true;
                    used_indexNo = index;
                    used_op = tmp_op;
                    used_keys = tmp_keys;
                    otherConds = tmp_other;
                }
            }
        }
        return true;
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
    bool loadFromFile(const std::string &filename, const std::string &tableName)
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

        // get all attr
        std::vector<std::string> allAttrName;
        std::vector<int> allOffsets;
        std::vector<AttrType> allTypes;
        std::vector<int> allTypeLens;
        sm->getAllAttr(tableName, allAttrName, allOffsets, allTypes, allTypeLens);

        // set file istream
        std::string dummy;
        std::ifstream ifs(filename);
        std::getline(ifs, dummy);
        std::vector<std::string> dumNames;
        SplitString(dummy, dumNames, "'");
        if (dumNames != allAttrName)
        {
            std::cout << "Incompatitable attributes." << std::endl;
            return false;
        }

        std::string lor;
        while (std::getline(ifs, lor))
        {
            std::vector<std::string> valStrings;
            SplitString(lor, valStrings, "'");
            if (valStrings.size() != allAttrName.size())
            {
                std::cout << "Broken line at: " << lor << std::endl;
                continue;
            }

            std::vector<Value> values;
            for (auto i = 0; i < valStrings.size(); i++)
            {
                Value val;
                if (valStrings[i] == "NULL")
                {
                    val.type = AttrType::NONE;
                }
                else
                {
                    switch (allTypes[i])
                    {
                    case AttrType::INT:
                    {
                        val.type = AttrType::INT;
                        val.len = 4;
                        val.pData.Int = std::stoi(valStrings[i]);
                        break;
                    }
                    case AttrType::FLOAT:
                    {
                        val.type = AttrType::FLOAT;
                        val.len = 4;
                        val.pData.Float = std::stof(valStrings[i]);
                        break;
                    }
                    case AttrType::VARCHAR:
                    {
                        val.type = AttrType::VARCHAR;
                        val.len = valStrings[i].size();
                        if (val.len > allTypeLens[i])
                        {
                            std::cout << "WARNING: Too long VARCHAR at: " << lor << std::endl;
                            val.len = allTypeLens[i];
                        }
                        memcpy(&val.pData, valStrings[i].c_str(), val.len);
                        break;
                    }
                    default:
                        break;
                    }
                }
                values.push_back(val);
            }
            insert(tableName, values);
        }
        ifs.close();
        return true;
    }
    bool optimizeConditions(std::vector<Condition> &conditions)
    {
        std::vector<Condition> newConditions;
        for (size_t i = 0; i < conditions.size(); i++)
        {
            if (conditions[i].bRhsIsAttr)
            {
                newConditions.push_back(conditions[i]);
                continue;
            }
            size_t j = 0;
            for (j = 0; j < newConditions.size(); j++)
            {
                if (newConditions[j].lhs == conditions[i].lhs && conditions[i].bRhsIsAttr == false)
                {
                    auto t = newConditions[j];
                    auto s = conditions[i];
                    switch (s.op)
                    {
                    case CompOp::E:
                    {
                        switch (t.op)
                        {
                        case CompOp::E:
                            if (s.rhsValue != t.rhsValue)
                                return false;
                            break;
                        case CompOp::L:
                            if (s.rhsValue >= t.rhsValue)
                                return false;
                            t.op = CompOp::E;
                            t.rhsValue = s.rhsValue;
                            break;
                        case CompOp::LE:
                            if (s.rhsValue > t.rhsValue)
                                return false;
                            t.op = CompOp::E;
                            t.rhsValue = s.rhsValue;
                            break;
                        case CompOp::G:
                            if (s.rhsValue <= t.rhsValue)
                                return false;
                            t.op = CompOp::E;
                            t.rhsValue = s.rhsValue;
                            break;
                        case CompOp::GE:
                            if (s.rhsValue < t.rhsValue)
                                return false;
                            t.op = CompOp::E;
                            t.rhsValue = s.rhsValue;
                            break;
                        case CompOp::NE:
                            if (s.rhsValue == t.rhsValue)
                                return false;
                            t.op = CompOp::E;
                            t.rhsValue = s.rhsValue;
                            break;
                        case CompOp::IN:
                        {
                            bool found = false;
                            for (auto v : t.rhsValues)
                                if (v == s.rhsValue)
                                {
                                    found = true;
                                    break;
                                }
                            if (!found)
                                return false;
                            t.op = CompOp::E;
                            t.rhsValue = s.rhsValue;
                            break;
                        }
                        case CompOp::ISNULL:
                            return false;
                            break;
                        case CompOp::ISNOTNULL:
                            break;
                        case CompOp::BETWEEN:
                            if (s.rhsValue <= t.rhsValues[0] || s.rhsValue >= t.rhsValues[1])
                                return false;
                            break;
                        case CompOp::BETWEENL:
                            if (s.rhsValue < t.rhsValues[0] || s.rhsValue >= t.rhsValues[1])
                                return false;
                            break;
                        case CompOp::BETWEENR:
                            if (s.rhsValue <= t.rhsValues[0] || s.rhsValue > t.rhsValues[1])
                                return false;
                            break;
                        case CompOp::BETWEENLR:
                            if (s.rhsValue < t.rhsValues[0] || s.rhsValue > t.rhsValues[1])
                                return false;
                            break;
                        default:
                            break;
                        }
                    }
                    break;
                    case CompOp::L:
                    {
                        switch (t.op)
                        {
                        case CompOp::E:
                            if (s.rhsValue <= t.rhsValue)
                                return false;
                            break;
                        case CompOp::L:
                            t.rhsValue = std::min(t.rhsValue, s.rhsValue);
                            break;
                        case CompOp::LE:
                            if (s.rhsValue <= t.rhsValue)
                            {
                                t.op = CompOp::L;
                                t.rhsValue = s.rhsValue;
                            }
                            break;
                        case CompOp::G:
                            if (s.rhsValue <= t.rhsValue)
                                return false;
                            t.op = CompOp::BETWEEN;
                            t.rhsValues.push_back(t.rhsValue);
                            t.rhsValues.push_back(s.rhsValue);
                            break;
                        case CompOp::GE:
                            if (s.rhsValue <= t.rhsValue)
                                return false;
                            t.op = CompOp::BETWEENL;
                            t.rhsValues.push_back(t.rhsValue);
                            t.rhsValues.push_back(s.rhsValue);
                            break;
                        case CompOp::NE:
                            if (s.rhsValue <= t.rhsValue)
                            {
                                t = s;
                                break;
                            }
                            newConditions.push_back(conditions[i]);
                            break;
                        case CompOp::IN:
                        {
                            s.rhsValues.clear();
                            for (auto v : t.rhsValues)
                                if (v < s.rhsValue)
                                    s.rhsValues.push_back(v);
                            if (s.rhsValues.size() == 0)
                                return false;
                            t.rhsValues = s.rhsValues;
                            break;
                        }
                        case CompOp::ISNULL:
                            return false;
                            break;
                        case CompOp::ISNOTNULL:
                            break;
                        case CompOp::BETWEEN:
                        case CompOp::BETWEENL:
                            if (s.rhsValue <= t.rhsValues[0])
                                return false;
                            else if (s.rhsValue > t.rhsValues[0] && s.rhsValue <= t.rhsValues[1])
                                t.rhsValues[1] = s.rhsValue;
                            break;
                        case CompOp::BETWEENR:
                            if (s.rhsValue <= t.rhsValues[0])
                                return false;
                            else if (s.rhsValue > t.rhsValues[0] && s.rhsValue <= t.rhsValues[1])
                            {
                                t.op = CompOp::BETWEEN;
                                t.rhsValues[1] = s.rhsValue;
                            }
                            break;
                        case CompOp::BETWEENLR:
                            if (s.rhsValue <= t.rhsValues[0])
                                return false;
                            else if (s.rhsValue > t.rhsValues[0] && s.rhsValue <= t.rhsValues[1])
                            {
                                t.op = CompOp::BETWEENL;
                                t.rhsValues[1] = s.rhsValue;
                            }
                            break;
                        default:
                            break;
                        }
                    }
                    break;
                    case CompOp::LE:
                    {
                        switch (t.op)
                        {
                        case CompOp::E:
                            if (s.rhsValue < t.rhsValue)
                                return false;
                            break;
                        case CompOp::L:
                            if (s.rhsValue < t.rhsValue)
                            {
                                t.op = CompOp::LE;
                                t.rhsValue = s.rhsValue;
                            }
                            break;
                        case CompOp::LE:
                            t.rhsValue = std::min(t.rhsValue, s.rhsValue);
                            break;
                        case CompOp::G:
                            if (s.rhsValue <= t.rhsValue)
                                return false;
                            t.op = CompOp::BETWEENR;
                            t.rhsValues.push_back(t.rhsValue);
                            t.rhsValues.push_back(s.rhsValue);
                            break;
                        case CompOp::GE:
                            if (s.rhsValue < t.rhsValue)
                                return false;
                            t.op = CompOp::BETWEENLR;
                            t.rhsValues.push_back(t.rhsValue);
                            t.rhsValues.push_back(s.rhsValue);
                            break;
                        case CompOp::NE:
                            if (s.rhsValue < t.rhsValue)
                            {
                                t = s;
                                break;
                            }
                            newConditions.push_back(conditions[i]);
                            break;
                        case CompOp::IN:
                        {
                            s.rhsValues.clear();
                            for (auto v : t.rhsValues)
                                if (v <= s.rhsValue)
                                    s.rhsValues.push_back(v);
                            if (s.rhsValues.size() == 0)
                                return false;
                            t.rhsValues = s.rhsValues;
                            break;
                        }
                        case CompOp::ISNULL:
                            return false;
                            break;
                        case CompOp::ISNOTNULL:
                            break;
                        case CompOp::BETWEEN:
                            if (s.rhsValue <= t.rhsValues[0])
                                return false;
                            else if (s.rhsValue > t.rhsValues[0] && s.rhsValue < t.rhsValues[1])
                            {
                                t.op = CompOp::BETWEENR;
                                t.rhsValues[1] = s.rhsValue;
                            }
                            break;
                        case CompOp::BETWEENL:
                            if (s.rhsValue < t.rhsValues[0])
                                return false;
                            else if (s.rhsValue == t.rhsValues[0])
                            {
                                t.op = CompOp::E;
                                t.rhsValue = s.rhsValue;
                            }
                            else if (s.rhsValue > t.rhsValues[0] && s.rhsValue < t.rhsValues[1])
                            {
                                t.op = CompOp::BETWEENLR;
                                t.rhsValues[1] = s.rhsValue;
                            }
                            break;
                        case CompOp::BETWEENR:
                            if (s.rhsValue <= t.rhsValues[0])
                                return false;
                            else if (s.rhsValue > t.rhsValues[0] && s.rhsValue < t.rhsValues[1])
                                t.rhsValues[1] = s.rhsValue;
                            break;
                        case CompOp::BETWEENLR:
                            if (s.rhsValue < t.rhsValues[0])
                                return false;
                            else if (s.rhsValue == t.rhsValues[0])
                            {
                                t.op = CompOp::E;
                                t.rhsValue = s.rhsValue;
                            }
                            else if (s.rhsValue > t.rhsValues[0] && s.rhsValue < t.rhsValues[1])
                                t.rhsValues[1] = s.rhsValue;
                            break;
                        default:
                            break;
                        }
                    }
                    break;
                    case CompOp::G:
                    {
                        switch (t.op)
                        {
                        case CompOp::E:
                            if (s.rhsValue >= t.rhsValue)
                                return false;
                            break;
                        case CompOp::L:
                            if (s.rhsValue >= t.rhsValue)
                                return false;
                            t.op = CompOp::BETWEEN;
                            t.rhsValues.push_back(s.rhsValue);
                            t.rhsValues.push_back(t.rhsValue);
                            break;
                        case CompOp::LE:
                            if (s.rhsValue >= t.rhsValue)
                                return false;
                            t.op = CompOp::BETWEENR;
                            t.rhsValues.push_back(s.rhsValue);
                            t.rhsValues.push_back(t.rhsValue);
                            break;
                        case CompOp::G:
                            t.rhsValue = std::max(t.rhsValue, s.rhsValue);
                            break;
                        case CompOp::GE:
                            if (s.rhsValue >= t.rhsValue)
                            {
                                t.op = CompOp::G;
                                t.rhsValue = s.rhsValue;
                            }
                            break;
                        case CompOp::NE:
                            if (s.rhsValue >= t.rhsValue)
                            {
                                t = s;
                                break;
                            }
                            newConditions.push_back(conditions[i]);
                            break;
                        case CompOp::IN:
                        {
                            s.rhsValues.clear();
                            for (auto v : t.rhsValues)
                                if (v > s.rhsValue)
                                    s.rhsValues.push_back(v);
                            if (s.rhsValues.size() == 0)
                                return false;
                            t.rhsValues = s.rhsValues;
                            break;
                        }
                        case CompOp::ISNULL:
                            return false;
                            break;
                        case CompOp::ISNOTNULL:
                            break;
                        case CompOp::BETWEEN:
                            if (s.rhsValue >= t.rhsValues[1])
                                return false;
                            else if (s.rhsValue > t.rhsValues[0] && s.rhsValue < t.rhsValues[1])
                                t.rhsValues[0] = s.rhsValue;
                            break;
                        case CompOp::BETWEENL:
                            if (s.rhsValue >= t.rhsValues[1])
                                return false;
                            else if (s.rhsValue >= t.rhsValues[0] && s.rhsValue < t.rhsValues[1])
                            {
                                t.op = CompOp::BETWEEN;
                                t.rhsValues[0] = s.rhsValue;
                            }
                            break;
                        case CompOp::BETWEENR:
                            if (s.rhsValue >= t.rhsValues[1])
                                return false;
                            else if (s.rhsValue > t.rhsValues[0] && s.rhsValue < t.rhsValues[1])
                                t.rhsValues[0] = s.rhsValue;
                            break;
                        case CompOp::BETWEENLR:
                            if (s.rhsValue >= t.rhsValues[1])
                                return false;
                            else if (s.rhsValue >= t.rhsValues[0] && s.rhsValue < t.rhsValues[1])
                            {
                                t.op = CompOp::BETWEENR;
                                t.rhsValues[0] = s.rhsValue;
                            }
                            break;
                        default:
                            break;
                        }
                    }
                    break;
                    case CompOp::GE:
                    {
                        switch (t.op)
                        {
                        case CompOp::E:
                            if (s.rhsValue > t.rhsValue)
                                return false;
                            break;
                        case CompOp::L:
                            if (s.rhsValue >= t.rhsValue)
                                return false;
                            t.op = CompOp::BETWEENL;
                            t.rhsValues.push_back(s.rhsValue);
                            t.rhsValues.push_back(t.rhsValue);
                            break;
                        case CompOp::LE:
                            if (s.rhsValue > t.rhsValue)
                                return false;
                            if (s.rhsValue == t.rhsValue)
                            {
                                t.op = CompOp::E;
                                t.rhsValue = s.rhsValue;
                                break;
                            }
                            t.op = CompOp::BETWEENLR;
                            t.rhsValues.push_back(s.rhsValue);
                            t.rhsValues.push_back(t.rhsValue);
                            break;
                        case CompOp::G:
                            if (s.rhsValue > t.rhsValue)
                            {
                                t.op = CompOp::GE;
                                t.rhsValue = s.rhsValue;
                            }
                            break;
                        case CompOp::GE:
                            t.rhsValue = std::max(t.rhsValue, s.rhsValue);
                            break;
                        case CompOp::NE:
                            if (s.rhsValue > t.rhsValue)
                            {
                                t = s;
                                break;
                            }
                            newConditions.push_back(conditions[i]);
                            break;
                        case CompOp::IN:
                        {
                            s.rhsValues.clear();
                            for (auto v : t.rhsValues)
                                if (v >= s.rhsValue)
                                    s.rhsValues.push_back(v);
                            if (s.rhsValues.size() == 0)
                                return false;
                            t.rhsValues = s.rhsValues;
                            break;
                        }
                        case CompOp::ISNULL:
                            return false;
                            break;
                        case CompOp::ISNOTNULL:
                            break;
                        case CompOp::BETWEEN:
                            if (s.rhsValue >= t.rhsValues[1])
                                return false;
                            else if (s.rhsValue > t.rhsValues[0] && s.rhsValue < t.rhsValues[1])
                            {
                                t.op = CompOp::BETWEENL;
                                t.rhsValues[0] = s.rhsValue;
                            }
                            break;
                        case CompOp::BETWEENL:
                            if (s.rhsValue >= t.rhsValues[1])
                                return false;
                            else if (s.rhsValue > t.rhsValues[0] && s.rhsValue < t.rhsValues[1])
                                t.rhsValues[0] = s.rhsValue;
                            break;
                        case CompOp::BETWEENR:
                            if (s.rhsValue > t.rhsValues[1])
                                return false;
                            else if (s.rhsValue == t.rhsValues[1])
                            {
                                t.op = CompOp::E;
                                t.rhsValue = s.rhsValue;
                            }
                            else if (s.rhsValue > t.rhsValues[0] && s.rhsValue < t.rhsValues[1])
                            {
                                t.op = CompOp::BETWEENLR;
                                t.rhsValues[0] = s.rhsValue;
                            }
                            break;
                        case CompOp::BETWEENLR:
                            if (s.rhsValue > t.rhsValues[1])
                                return false;
                            else if (s.rhsValue == t.rhsValues[1])
                            {
                                t.op = CompOp::E;
                                t.rhsValue = s.rhsValue;
                            }
                            else if (s.rhsValue > t.rhsValues[0] && s.rhsValue < t.rhsValues[1])
                                t.rhsValues[0] = s.rhsValue;
                            break;
                        default:
                            break;
                        }
                    }
                    break;
                    case CompOp::NE:
                    {
                        switch (t.op)
                        {
                        case CompOp::E:
                            if (s.rhsValue == t.rhsValue)
                                return false;
                            break;
                        case CompOp::L:
                            if (s.rhsValue < t.rhsValue)
                                return false;
                            newConditions.push_back(s);
                            break;
                        case CompOp::LE:
                            if (s.rhsValue <= t.rhsValue)
                                return false;
                            newConditions.push_back(s);
                            break;
                        case CompOp::G:
                            if (s.rhsValue > t.rhsValue)
                                return false;
                            newConditions.push_back(s);
                            break;
                        case CompOp::GE:
                            if (s.rhsValue >= t.rhsValue)
                                return false;
                            newConditions.push_back(s);
                            break;
                        case CompOp::NE:
                            if (s.rhsValue != t.rhsValue)
                                newConditions.push_back(s);
                            break;
                        case CompOp::IN:
                        {
                            s.rhsValues.clear();
                            for (auto v : t.rhsValues)
                                if (v != s.rhsValue)
                                    s.rhsValues.push_back(v);
                            if (s.rhsValues.size() == 0)
                                return false;
                            t.rhsValues = s.rhsValues;
                            break;
                        }
                        case CompOp::ISNULL:
                            return false;
                            break;
                        case CompOp::ISNOTNULL:
                            break;
                        case CompOp::BETWEEN:
                            if (s.rhsValue > t.rhsValues[0] && s.rhsValue < t.rhsValues[1])
                                newConditions.push_back(s);
                            break;
                        case CompOp::BETWEENL:
                            if (s.rhsValue == t.rhsValues[0])
                            {
                                t.op = CompOp::BETWEEN;
                                break;
                            }
                            if (s.rhsValue > t.rhsValues[0] || s.rhsValue < t.rhsValues[1])
                                newConditions.push_back(s);
                            break;
                        case CompOp::BETWEENR:
                            if (s.rhsValue == t.rhsValues[1])
                            {
                                t.op = CompOp::BETWEEN;
                                break;
                            }
                            if (s.rhsValue > t.rhsValues[0] || s.rhsValue < t.rhsValues[1])
                                newConditions.push_back(s);
                            break;
                        case CompOp::BETWEENLR:
                            if (s.rhsValue == t.rhsValues[0])
                            {
                                t.op = CompOp::BETWEENR;
                                break;
                            }
                            if (s.rhsValue == t.rhsValues[1])
                            {
                                t.op = CompOp::BETWEENL;
                                break;
                            }
                            if (s.rhsValue > t.rhsValues[0] || s.rhsValue < t.rhsValues[1])
                                newConditions.push_back(s);
                            break;
                        default:
                            break;
                        }
                    }
                    break;
                    case CompOp::IN:
                    {
                        switch (t.op)
                        {
                        case CompOp::E:
                        {
                            bool found = false;
                            for (auto v : s.rhsValues)
                                if (v == t.rhsValue)
                                {
                                    found = true;
                                    break;
                                }
                            if (!found)
                                return false;
                            break;
                        }
                        case CompOp::L:
                        {
                            t.rhsValues.clear();
                            for (auto v : s.rhsValues)
                                if (v < t.rhsValue)
                                    t.rhsValues.push_back(v);
                            if (t.rhsValues.size() == 0)
                                return false;
                            t.op = CompOp::IN;
                            break;
                        }
                        case CompOp::LE:
                        {
                            t.rhsValues.clear();
                            for (auto v : s.rhsValues)
                                if (v <= t.rhsValue)
                                    t.rhsValues.push_back(v);
                            if (t.rhsValues.size() == 0)
                                return false;
                            t.op = CompOp::IN;
                            break;
                        }
                        case CompOp::G:
                        {
                            t.rhsValues.clear();
                            for (auto v : s.rhsValues)
                                if (v > t.rhsValue)
                                    t.rhsValues.push_back(v);
                            if (t.rhsValues.size() == 0)
                                return false;
                            t.op = CompOp::IN;
                            break;
                        }
                        case CompOp::GE:
                        {
                            t.rhsValues.clear();
                            for (auto v : s.rhsValues)
                                if (v >= t.rhsValue)
                                    t.rhsValues.push_back(v);
                            if (t.rhsValues.size() == 0)
                                return false;
                            t.op = CompOp::IN;
                            break;
                        }
                        case CompOp::NE:
                        {
                            t.rhsValues.clear();
                            for (auto v : s.rhsValues)
                                if (v != t.rhsValue)
                                    t.rhsValues.push_back(v);
                            if (t.rhsValues.size() == 0)
                                return false;
                            t.op = CompOp::IN;
                            break;
                        }
                        case CompOp::IN:
                        {
                            std::vector<Value> tmp;
                            std::sort(t.rhsValues.begin(), t.rhsValues.end());
                            std::sort(s.rhsValues.begin(), s.rhsValues.end());
                            std::set_intersection(t.rhsValues.begin(), t.rhsValues.end(), s.rhsValues.begin(), s.rhsValues.end(), std::back_inserter(tmp));
                            std::sort(tmp.begin(), tmp.end());
                            auto it = std::unique(tmp.begin(), tmp.end());
                            if (it != tmp.end())
                                tmp.erase(it, tmp.end());
                            if (tmp.size() == 0)
                                return false;
                            t.rhsValues = tmp;
                            break;
                        }
                        case CompOp::ISNULL:
                            return false;
                            break;
                        case CompOp::ISNOTNULL:
                            break;
                        case CompOp::BETWEEN:
                        {
                            std::vector<Value> tmp;
                            for (auto v : s.rhsValues)
                                if (v > t.rhsValues[0] && v < t.rhsValues[1])
                                    tmp.push_back(v);
                            t.op = CompOp::IN;
                            t.rhsValues = tmp;
                            break;
                        }
                        case CompOp::BETWEENL:
                        {
                            std::vector<Value> tmp;
                            for (auto v : s.rhsValues)
                                if (v >= t.rhsValues[0] && v < t.rhsValues[1])
                                    tmp.push_back(v);
                            t.op = CompOp::IN;
                            t.rhsValues = tmp;
                            break;
                        }
                        case CompOp::BETWEENR:
                        {
                            std::vector<Value> tmp;
                            for (auto v : s.rhsValues)
                                if (v > t.rhsValues[0] && v <= t.rhsValues[1])
                                    tmp.push_back(v);
                            t.op = CompOp::IN;
                            t.rhsValues = tmp;
                            break;
                        }
                        case CompOp::BETWEENLR:
                        {
                            std::vector<Value> tmp;
                            for (auto v : s.rhsValues)
                                if (v >= t.rhsValues[0] && v <= t.rhsValues[1])
                                    tmp.push_back(v);
                            t.op = CompOp::IN;
                            t.rhsValues = tmp;
                            break;
                        }
                        default:
                            break;
                        }
                    }
                    break;
                    case CompOp::ISNULL:
                    {
                        switch (t.op)
                        {
                        case CompOp::E:
                        case CompOp::L:
                        case CompOp::LE:
                        case CompOp::G:
                        case CompOp::GE:
                        case CompOp::NE:
                        case CompOp::IN:
                        case CompOp::ISNOTNULL:
                        case CompOp::BETWEEN:
                        case CompOp::BETWEENL:
                        case CompOp::BETWEENR:
                        case CompOp::BETWEENLR:
                            return false;
                            break;
                        case CompOp::ISNULL:
                            break;
                        default:
                            break;
                        }
                    }
                    break;
                    case CompOp::ISNOTNULL:
                    {
                        switch (t.op)
                        {
                        case CompOp::E:
                        case CompOp::L:
                        case CompOp::LE:
                        case CompOp::G:
                        case CompOp::GE:
                        case CompOp::NE:
                        case CompOp::IN:
                        case CompOp::ISNOTNULL:
                        case CompOp::BETWEEN:
                        case CompOp::BETWEENL:
                        case CompOp::BETWEENR:
                        case CompOp::BETWEENLR:
                            break;
                        case CompOp::ISNULL:
                            return false;
                            break;
                        default:
                            break;
                        }
                    }
                    break;
                    case CompOp::BETWEEN:
                    {
                        switch (t.op)
                        {
                        case CompOp::E:
                            if (t.rhsValue <= s.rhsValues[0] || t.rhsValue >= s.rhsValues[1])
                                return false;
                            break;
                        case CompOp::L:
                            if (t.rhsValue <= s.rhsValues[0])
                                return false;
                            else if (t.rhsValue > s.rhsValues[0] && t.rhsValue <= s.rhsValues[1])
                                s.rhsValues[1] = t.rhsValue;
                            t = s;
                            break;
                        case CompOp::LE:
                            if (t.rhsValue <= s.rhsValues[0])
                                return false;
                            else if (t.rhsValue > s.rhsValues[0] && t.rhsValue < s.rhsValues[1])
                            {
                                s.op = CompOp::BETWEENR;
                                s.rhsValues[1] = t.rhsValue;
                            }
                            t = s;
                            break;
                        case CompOp::G:
                            if (t.rhsValue >= s.rhsValues[1])
                                return false;
                            else if (t.rhsValue > s.rhsValues[0] && t.rhsValue < s.rhsValues[1])
                                s.rhsValues[0] = t.rhsValue;
                            t = s;
                            break;
                        case CompOp::GE:
                            if (t.rhsValue >= s.rhsValues[1])
                                return false;
                            else if (t.rhsValue > s.rhsValues[0] && t.rhsValue < s.rhsValues[1])
                            {
                                s.op = CompOp::BETWEENL;
                                s.rhsValues[0] = t.rhsValue;
                            }
                            t = s;
                            break;
                        case CompOp::NE:
                            if (t.rhsValue > s.rhsValues[0] && t.rhsValue < s.rhsValues[1])
                                newConditions.push_back(conditions[i]);
                            break;
                        case CompOp::IN:
                        {
                            std::vector<Value> tmp;
                            for (auto v : t.rhsValues)
                                if (v > s.rhsValues[0] && v < s.rhsValues[1])
                                    tmp.push_back(v);
                            t.rhsValues = tmp;
                            break;
                        }
                        case CompOp::ISNULL:
                            return false;
                            break;
                        case CompOp::ISNOTNULL:
                            break;
                        case CompOp::BETWEEN:
                            if (s.rhsValues[0] >= t.rhsValues[1] || s.rhsValues[1] <= t.rhsValues[0])
                                return false;
                            if (s.rhsValues[0] > t.rhsValues[0])
                                if (s.rhsValues[1] > t.rhsValues[1])
                                {
                                    t.op = CompOp::BETWEEN;
                                    t.rhsValues[0] = s.rhsValues[0];
                                }
                                else
                                {
                                    t.op = CompOp::BETWEEN;
                                    t.rhsValues[0] = s.rhsValues[0];
                                    t.rhsValues[1] = s.rhsValues[1];
                                }
                            else if (s.rhsValues[1] > t.rhsValues[1])
                            {
                                t.op = CompOp::BETWEEN;
                            }
                            else
                            {
                                t.op = CompOp::BETWEEN;
                                t.rhsValues[1] = s.rhsValues[1];
                            }
                            break;
                        case CompOp::BETWEENL:
                            if (s.rhsValues[0] >= t.rhsValues[1] || s.rhsValues[1] <= t.rhsValues[0])
                                return false;
                            if (s.rhsValues[0] > t.rhsValues[0])
                                if (s.rhsValues[1] > t.rhsValues[1])
                                {
                                    t.op = CompOp::BETWEEN;
                                    t.rhsValues[0] = s.rhsValues[0];
                                }
                                else
                                {
                                    t.op = CompOp::BETWEEN;
                                    t.rhsValues[0] = s.rhsValues[0];
                                    t.rhsValues[1] = s.rhsValues[1];
                                }
                            else if (s.rhsValues[1] > t.rhsValues[1])
                            {
                                t.op = CompOp::BETWEENL;
                            }
                            else
                            {
                                t.op = CompOp::BETWEENL;
                                t.rhsValues[1] = s.rhsValues[1];
                            }
                            break;
                        case CompOp::BETWEENR:
                            if (s.rhsValues[0] >= t.rhsValues[1] || s.rhsValues[1] <= t.rhsValues[0])
                                return false;
                            if (s.rhsValues[0] > t.rhsValues[0])
                                if (s.rhsValues[1] > t.rhsValues[1])
                                {
                                    t.op = CompOp::BETWEENR;
                                    t.rhsValues[0] = s.rhsValues[0];
                                }
                                else
                                {
                                    t.op = CompOp::BETWEEN;
                                    t.rhsValues[0] = s.rhsValues[0];
                                    t.rhsValues[1] = s.rhsValues[1];
                                }
                            else if (s.rhsValues[1] > t.rhsValues[1])
                            {
                                t.op = CompOp::BETWEENR;
                            }
                            else
                            {
                                t.op = CompOp::BETWEEN;
                                t.rhsValues[1] = s.rhsValues[1];
                            }
                            break;
                        case CompOp::BETWEENLR:
                            if (s.rhsValues[0] >= t.rhsValues[1] || s.rhsValues[1] <= t.rhsValues[0])
                                return false;
                            if (s.rhsValues[0] > t.rhsValues[0])
                                if (s.rhsValues[1] > t.rhsValues[1])
                                {
                                    t.op = CompOp::BETWEENR;
                                    t.rhsValues[0] = s.rhsValues[0];
                                }
                                else
                                {
                                    t.op = CompOp::BETWEEN;
                                    t.rhsValues[0] = s.rhsValues[0];
                                    t.rhsValues[1] = s.rhsValues[1];
                                }
                            else if (s.rhsValues[1] > t.rhsValues[1])
                            {
                                t.op = CompOp::BETWEENLR;
                            }
                            else
                            {
                                t.op = CompOp::BETWEENL;
                                t.rhsValues[1] = s.rhsValues[1];
                            }
                            break;
                        default:
                            break;
                        }
                    }
                    break;
                    case CompOp::BETWEENL:
                    {
                        switch (t.op)
                        {
                        case CompOp::E:
                            if (t.rhsValue < s.rhsValues[0] || t.rhsValue >= s.rhsValues[1])
                                return false;
                            break;
                        case CompOp::L:
                            if (t.rhsValue <= s.rhsValues[0])
                                return false;
                            else if (t.rhsValue > s.rhsValues[0] && t.rhsValue <= s.rhsValues[1])
                                s.rhsValues[1] = t.rhsValue;
                            t = s;
                            break;
                        case CompOp::LE:
                            if (t.rhsValue < s.rhsValues[0])
                                return false;
                            else if (t.rhsValue == s.rhsValues[0])
                            {
                                s.op = CompOp::E;
                                s.rhsValue = t.rhsValue;
                            }
                            else if (t.rhsValue > s.rhsValues[0] && t.rhsValue < s.rhsValues[1])
                            {
                                s.op = CompOp::BETWEENLR;
                                s.rhsValues[1] = t.rhsValue;
                            }
                            t = s;
                            break;
                        case CompOp::G:
                            if (t.rhsValue >= s.rhsValues[1])
                                return false;
                            else if (t.rhsValue >= s.rhsValues[0] && t.rhsValue < s.rhsValues[1])
                            {
                                s.op = CompOp::BETWEEN;
                                s.rhsValues[0] = t.rhsValue;
                            }
                            t = s;
                            break;
                        case CompOp::GE:
                            if (t.rhsValue >= s.rhsValues[1])
                                return false;
                            else if (t.rhsValue > s.rhsValues[0] && t.rhsValue < s.rhsValues[1])
                                s.rhsValues[0] = t.rhsValue;
                            t = s;
                            break;
                        case CompOp::NE:
                            if (t.rhsValue == s.rhsValues[0])
                            {
                                t.op = CompOp::BETWEEN;
                                t.rhsValues = s.rhsValues;
                                break;
                            }
                            if (t.rhsValue > s.rhsValues[0] && t.rhsValue < s.rhsValues[1])
                                newConditions.push_back(conditions[i]);
                            break;
                        case CompOp::IN:
                        {
                            std::vector<Value> tmp;
                            for (auto v : t.rhsValues)
                                if (v >= s.rhsValues[0] && v < s.rhsValues[1])
                                    tmp.push_back(v);
                            t.rhsValues = tmp;
                            break;
                        }
                        case CompOp::ISNULL:
                            return false;
                            break;
                        case CompOp::ISNOTNULL:
                            break;
                        case CompOp::BETWEEN:
                            if (s.rhsValues[0] >= t.rhsValues[1] || s.rhsValues[1] <= t.rhsValues[0])
                                return false;
                            if (s.rhsValues[0] > t.rhsValues[0])
                                if (s.rhsValues[1] > t.rhsValues[1])
                                {
                                    t.op = CompOp::BETWEENL;
                                    t.rhsValues[0] = s.rhsValues[0];
                                }
                                else
                                {
                                    t.op = CompOp::BETWEENL;
                                    t.rhsValues[0] = s.rhsValues[0];
                                    t.rhsValues[1] = s.rhsValues[1];
                                }
                            else if (s.rhsValues[1] > t.rhsValues[1])
                            {
                                t.op = CompOp::BETWEEN;
                            }
                            else
                            {
                                t.op = CompOp::BETWEEN;
                                t.rhsValues[1] = s.rhsValues[1];
                            }
                            break;
                        case CompOp::BETWEENL:
                            if (s.rhsValues[0] >= t.rhsValues[1] || s.rhsValues[1] <= t.rhsValues[0])
                                return false;
                            if (s.rhsValues[0] > t.rhsValues[0])
                                if (s.rhsValues[1] > t.rhsValues[1])
                                {
                                    t.op = CompOp::BETWEENL;
                                    t.rhsValues[0] = s.rhsValues[0];
                                }
                                else
                                {
                                    t.op = CompOp::BETWEENL;
                                    t.rhsValues[0] = s.rhsValues[0];
                                    t.rhsValues[1] = s.rhsValues[1];
                                }
                            else if (s.rhsValues[1] > t.rhsValues[1])
                            {
                                t.op = CompOp::BETWEENL;
                            }
                            else
                            {
                                t.op = CompOp::BETWEENL;
                                t.rhsValues[1] = s.rhsValues[1];
                            }
                            break;
                        case CompOp::BETWEENR:
                            if (s.rhsValues[0] > t.rhsValues[1] || s.rhsValues[1] <= t.rhsValues[0])
                                return false;
                            if (s.rhsValues[0] == t.rhsValues[1])
                            {
                                t.op = CompOp::E;
                                t.rhsValue = s.rhsValues[0];
                                break;
                            }
                            if (s.rhsValues[0] > t.rhsValues[0])
                                if (s.rhsValues[1] > t.rhsValues[1])
                                {
                                    t.op = CompOp::BETWEENLR;
                                    t.rhsValues[0] = s.rhsValues[0];
                                }
                                else
                                {
                                    t.op = CompOp::BETWEENL;
                                    t.rhsValues[0] = s.rhsValues[0];
                                    t.rhsValues[1] = s.rhsValues[1];
                                }
                            else if (s.rhsValues[1] > t.rhsValues[1])
                            {
                                t.op = CompOp::BETWEENR;
                            }
                            else
                            {
                                t.op = CompOp::BETWEEN;
                                t.rhsValues[1] = s.rhsValues[1];
                            }
                            break;
                        case CompOp::BETWEENLR:
                            if (s.rhsValues[0] > t.rhsValues[1] || s.rhsValues[1] <= t.rhsValues[0])
                                return false;
                            if (s.rhsValues[0] == t.rhsValues[1])
                            {
                                t.op = CompOp::E;
                                t.rhsValue = s.rhsValues[0];
                                break;
                            }
                            if (s.rhsValues[0] > t.rhsValues[0])
                                if (s.rhsValues[1] > t.rhsValues[1])
                                {
                                    t.op = CompOp::BETWEENLR;
                                    t.rhsValues[0] = s.rhsValues[0];
                                }
                                else
                                {
                                    t.op = CompOp::BETWEENL;
                                    t.rhsValues[0] = s.rhsValues[0];
                                    t.rhsValues[1] = s.rhsValues[1];
                                }
                            else if (s.rhsValues[1] > t.rhsValues[1])
                            {
                                t.op = CompOp::BETWEENLR;
                            }
                            else
                            {
                                t.op = CompOp::BETWEENL;
                                t.rhsValues[1] = s.rhsValues[1];
                            }
                            break;
                        default:
                            break;
                        }
                    }
                    break;
                    case CompOp::BETWEENR:
                    {
                        switch (t.op)
                        {
                        case CompOp::E:
                            if (t.rhsValue <= s.rhsValues[0] || t.rhsValue > s.rhsValues[1])
                                return false;
                            break;
                        case CompOp::L:
                            if (t.rhsValue <= s.rhsValues[0])
                                return false;
                            else if (t.rhsValue > s.rhsValues[0] && t.rhsValue <= s.rhsValues[1])
                            {
                                s.op = CompOp::BETWEEN;
                                s.rhsValues[1] = t.rhsValue;
                            }
                            t = s;
                            break;
                        case CompOp::LE:
                            if (t.rhsValue <= s.rhsValues[0])
                                return false;
                            else if (t.rhsValue > s.rhsValues[0] && t.rhsValue < s.rhsValues[1])
                                s.rhsValues[1] = t.rhsValue;
                            t = s;
                            break;
                        case CompOp::G:
                            if (t.rhsValue >= s.rhsValues[1])
                                return false;
                            else if (t.rhsValue > s.rhsValues[0] && t.rhsValue < s.rhsValues[1])
                                s.rhsValues[0] = t.rhsValue;
                            t = s;
                            break;
                        case CompOp::GE:
                            if (t.rhsValue > s.rhsValues[1])
                                return false;
                            else if (t.rhsValue == s.rhsValues[1])
                            {
                                s.op = CompOp::E;
                                s.rhsValue = t.rhsValue;
                            }
                            else if (t.rhsValue > s.rhsValues[0] && t.rhsValue < s.rhsValues[1])
                            {
                                s.op = CompOp::BETWEENLR;
                                s.rhsValues[0] = t.rhsValue;
                            }
                            t = s;
                            break;
                        case CompOp::NE:
                            if (t.rhsValue == s.rhsValues[1])
                            {
                                t.op = CompOp::BETWEEN;
                                t.rhsValues = s.rhsValues;
                                break;
                            }
                            if (t.rhsValue > s.rhsValues[0] && t.rhsValue < s.rhsValues[1])
                                newConditions.push_back(conditions[i]);
                            break;
                        case CompOp::IN:
                        {
                            std::vector<Value> tmp;
                            for (auto v : t.rhsValues)
                                if (v > s.rhsValues[0] && v <= s.rhsValues[1])
                                    tmp.push_back(v);
                            t.rhsValues = tmp;
                            break;
                        }
                        case CompOp::ISNULL:
                            return false;
                            break;
                        case CompOp::ISNOTNULL:
                            break;
                        case CompOp::BETWEEN:
                            if (s.rhsValues[0] >= t.rhsValues[1] || s.rhsValues[1] <= t.rhsValues[0])
                                return false;
                            if (s.rhsValues[0] > t.rhsValues[0])
                                if (s.rhsValues[1] > t.rhsValues[1])
                                {
                                    t.op = CompOp::BETWEEN;
                                    t.rhsValues[0] = s.rhsValues[0];
                                }
                                else
                                {
                                    t.op = CompOp::BETWEENR;
                                    t.rhsValues[0] = s.rhsValues[0];
                                    t.rhsValues[1] = s.rhsValues[1];
                                }
                            else if (s.rhsValues[1] > t.rhsValues[1])
                            {
                                t.op = CompOp::BETWEEN;
                            }
                            else
                            {
                                t.op = CompOp::BETWEENR;
                                t.rhsValues[1] = s.rhsValues[1];
                            }
                            break;
                        case CompOp::BETWEENL:
                            if (s.rhsValues[0] >= t.rhsValues[1] || s.rhsValues[1] < t.rhsValues[0])
                                return false;
                            if (s.rhsValues[1] == t.rhsValues[0])
                            {
                                t.op = CompOp::E;
                                t.rhsValue = s.rhsValues[1];
                                break;
                            }
                            if (s.rhsValues[0] > t.rhsValues[0])
                                if (s.rhsValues[1] > t.rhsValues[1])
                                {
                                    t.op = CompOp::BETWEEN;
                                    t.rhsValues[0] = s.rhsValues[0];
                                }
                                else
                                {
                                    t.op = CompOp::BETWEENR;
                                    t.rhsValues[0] = s.rhsValues[0];
                                    t.rhsValues[1] = s.rhsValues[1];
                                }
                            else if (s.rhsValues[1] > t.rhsValues[1])
                            {
                                t.op = CompOp::BETWEENL;
                            }
                            else
                            {
                                t.op = CompOp::BETWEENLR;
                                t.rhsValues[1] = s.rhsValues[1];
                            }
                            break;
                        case CompOp::BETWEENR:
                            if (s.rhsValues[0] > t.rhsValues[1] || s.rhsValues[1] <= t.rhsValues[0])
                                return false;
                            if (s.rhsValues[0] > t.rhsValues[0])
                                if (s.rhsValues[1] > t.rhsValues[1])
                                {
                                    t.op = CompOp::BETWEENR;
                                    t.rhsValues[0] = s.rhsValues[0];
                                }
                                else
                                {
                                    t.op = CompOp::BETWEENR;
                                    t.rhsValues[0] = s.rhsValues[0];
                                    t.rhsValues[1] = s.rhsValues[1];
                                }
                            else if (s.rhsValues[1] > t.rhsValues[1])
                            {
                                t.op = CompOp::BETWEENR;
                            }
                            else
                            {
                                t.op = CompOp::BETWEENR;
                                t.rhsValues[1] = s.rhsValues[1];
                            }
                            break;
                        case CompOp::BETWEENLR:
                            if (s.rhsValues[0] > t.rhsValues[1] || s.rhsValues[1] <= t.rhsValues[0])
                                return false;
                            if (s.rhsValues[1] == t.rhsValues[0])
                            {
                                t.op = CompOp::E;
                                t.rhsValue = s.rhsValues[1];
                                break;
                            }
                            if (s.rhsValues[0] >= t.rhsValues[0])
                                if (s.rhsValues[1] > t.rhsValues[1])
                                {
                                    t.op = CompOp::BETWEENR;
                                    t.rhsValues[0] = s.rhsValues[0];
                                }
                                else
                                {
                                    t.op = CompOp::BETWEENR;
                                    t.rhsValues[0] = s.rhsValues[0];
                                    t.rhsValues[1] = s.rhsValues[1];
                                }
                            else if (s.rhsValues[1] > t.rhsValues[1])
                            {
                                t.op = CompOp::BETWEENLR;
                            }
                            else
                            {
                                t.op = CompOp::BETWEENLR;
                                t.rhsValues[1] = s.rhsValues[1];
                            }
                            break;
                        default:
                            break;
                        }
                    }
                    break;
                    case CompOp::BETWEENLR:
                    {
                        switch (t.op)
                        {
                        case CompOp::E:
                            if (t.rhsValue < s.rhsValues[0] || t.rhsValue > s.rhsValues[1])
                                return false;
                            break;
                        case CompOp::L:
                            if (t.rhsValue <= s.rhsValues[0])
                                return false;
                            else if (t.rhsValue > s.rhsValues[0] && t.rhsValue <= s.rhsValues[1])
                            {
                                s.op = CompOp::BETWEENL;
                                s.rhsValues[1] = t.rhsValue;
                            }
                            t = s;
                            break;
                        case CompOp::LE:
                            if (t.rhsValue < s.rhsValues[0])
                                return false;
                            else if (t.rhsValue == s.rhsValues[0])
                            {
                                s.op = CompOp::E;
                                s.rhsValue = t.rhsValue;
                            }
                            else if (t.rhsValue > s.rhsValues[0] && t.rhsValue < s.rhsValues[1])
                                s.rhsValues[1] = t.rhsValue;
                            t = s;
                            break;
                        case CompOp::G:
                            if (t.rhsValue >= s.rhsValues[1])
                                return false;
                            else if (t.rhsValue >= s.rhsValues[0] && t.rhsValue < s.rhsValues[1])
                            {
                                s.op = CompOp::BETWEENR;
                                s.rhsValues[0] = t.rhsValue;
                            }
                            t = s;
                            break;
                        case CompOp::GE:
                            if (t.rhsValue > s.rhsValues[1])
                                return false;
                            else if (t.rhsValue == s.rhsValues[1])
                            {
                                s.op = CompOp::E;
                                s.rhsValue = t.rhsValue;
                            }
                            else if (t.rhsValue > s.rhsValues[0] && t.rhsValue < s.rhsValues[1])
                                s.rhsValues[0] = t.rhsValue;
                            t = s;
                            break;
                        case CompOp::NE:
                            if (t.rhsValue == s.rhsValues[0])
                            {
                                t.op = CompOp::BETWEENR;
                                t.rhsValues = s.rhsValues;
                                break;
                            }
                            if (t.rhsValue == s.rhsValues[1])
                            {
                                t.op = CompOp::BETWEENL;
                                t.rhsValues = s.rhsValues;
                                break;
                            }
                            if (t.rhsValue > s.rhsValues[0] && t.rhsValue < s.rhsValues[1])
                                newConditions.push_back(conditions[i]);
                            break;
                        case CompOp::IN:
                        {
                            std::vector<Value> tmp;
                            for (auto v : t.rhsValues)
                                if (v >= s.rhsValues[0] && v <= s.rhsValues[1])
                                    tmp.push_back(v);
                            t.rhsValues = tmp;
                            break;
                        }
                        case CompOp::ISNULL:
                            return false;
                            break;
                        case CompOp::ISNOTNULL:
                            break;
                        case CompOp::BETWEEN:
                            if (s.rhsValues[0] >= t.rhsValues[1] || s.rhsValues[1] <= t.rhsValues[0])
                                return false;
                            if (s.rhsValues[0] > t.rhsValues[0])
                                if (s.rhsValues[1] >= t.rhsValues[1])
                                {
                                    t.op = CompOp::BETWEENL;
                                    t.rhsValues[0] = s.rhsValues[0];
                                }
                                else
                                {
                                    t.op = CompOp::BETWEENLR;
                                    t.rhsValues[0] = s.rhsValues[0];
                                    t.rhsValues[1] = s.rhsValues[1];
                                }
                            else if (s.rhsValues[1] >= t.rhsValues[1])
                            {
                                t.op = CompOp::BETWEEN;
                            }
                            else
                            {
                                t.op = CompOp::BETWEENR;
                                t.rhsValues[1] = s.rhsValues[1];
                            }
                            break;
                        case CompOp::BETWEENL:
                            if (s.rhsValues[0] >= t.rhsValues[1] || s.rhsValues[1] < t.rhsValues[0])
                                return false;
                            if (s.rhsValues[1] == t.rhsValues[0])
                            {
                                t.op = CompOp::E;
                                t.rhsValue = s.rhsValues[1];
                                break;
                            }
                            if (s.rhsValues[0] > t.rhsValues[0])
                                if (s.rhsValues[1] >= t.rhsValues[1])
                                {
                                    t.op = CompOp::BETWEENL;
                                    t.rhsValues[0] = s.rhsValues[0];
                                }
                                else
                                {
                                    t.op = CompOp::BETWEENLR;
                                    t.rhsValues[0] = s.rhsValues[0];
                                    t.rhsValues[1] = s.rhsValues[1];
                                }
                            else if (s.rhsValues[1] >= t.rhsValues[1])
                            {
                                t.op = CompOp::BETWEENL;
                            }
                            else
                            {
                                t.op = CompOp::BETWEENLR;
                                t.rhsValues[1] = s.rhsValues[1];
                            }
                            break;
                        case CompOp::BETWEENR:
                            if (s.rhsValues[0] > t.rhsValues[1] || s.rhsValues[1] <= t.rhsValues[0])
                                return false;
                            if (s.rhsValues[1] == t.rhsValues[0])
                            {
                                t.op = CompOp::E;
                                t.rhsValue = s.rhsValues[1];
                                break;
                            }
                            if (s.rhsValues[0] > t.rhsValues[0])
                                if (s.rhsValues[1] > t.rhsValues[1])
                                {
                                    t.op = CompOp::BETWEENLR;
                                    t.rhsValues[0] = s.rhsValues[0];
                                }
                                else
                                {
                                    t.op = CompOp::BETWEENLR;
                                    t.rhsValues[0] = s.rhsValues[0];
                                    t.rhsValues[1] = s.rhsValues[1];
                                }
                            else if (s.rhsValues[1] > t.rhsValues[1])
                            {
                                t.op = CompOp::BETWEENR;
                            }
                            else
                            {
                                t.op = CompOp::BETWEENR;
                                t.rhsValues[1] = s.rhsValues[1];
                            }
                            break;
                        case CompOp::BETWEENLR:
                            if (s.rhsValues[0] > t.rhsValues[1] || s.rhsValues[1] < t.rhsValues[0])
                                return false;
                            if (s.rhsValues[1] == t.rhsValues[0])
                            {
                                t.op = CompOp::E;
                                t.rhsValue = s.rhsValues[1];
                                break;
                            }
                            if (s.rhsValues[0] == t.rhsValues[1])
                            {
                                t.op = CompOp::E;
                                t.rhsValue = s.rhsValues[0];
                                break;
                            }
                            if (s.rhsValues[0] >= t.rhsValues[0])
                                if (s.rhsValues[1] > t.rhsValues[1])
                                {
                                    t.op = CompOp::BETWEENLR;
                                    t.rhsValues[0] = s.rhsValues[0];
                                }
                                else
                                {
                                    t.op = CompOp::BETWEENLR;
                                    t.rhsValues[0] = s.rhsValues[0];
                                    t.rhsValues[1] = s.rhsValues[1];
                                }
                            else if (s.rhsValues[1] > t.rhsValues[1])
                            {
                                t.op = CompOp::BETWEENLR;
                            }
                            else
                            {
                                t.op = CompOp::BETWEENLR;
                                t.rhsValues[1] = s.rhsValues[1];
                            }
                            break;
                        default:
                            break;
                        }
                    }
                    break;
                    default:
                        break;
                    }
                    newConditions[j] = t;
                    break;
                }
            }
            if (j == newConditions.size())
                newConditions.push_back(conditions[i]);
        }
        conditions = newConditions;
        return true;
    }
};
