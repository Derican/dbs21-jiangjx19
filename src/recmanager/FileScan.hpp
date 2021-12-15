#pragma once

#include "constants.h"
#include "FileHandle.hpp"

#include <vector>

class FileScan
{
private:
    FileHandle handle;
    int fileID;
    FileHeader fh;
    BufPageManager *bpm;
    AttrType type;
    int len;
    int offset;
    CompOp op;
    void *val;
    int pageID, slotID;
    DataType curPage;
    std::vector<CompareCondition> conditions;
    bool multiCondition;

public:
    FileScan()
    {
        pageID = 1;
        slotID = 0;
        curPage = nullptr;
    }
    ~FileScan() {}
    bool openScan(const FileHandle &fileHandle, AttrType type, int len, int offset, CompOp op, void *val)
    {
        handle = fileHandle;
        fileHandle.getFileID(fileID);
        fileHandle.getFileHeader(fh);
        fileHandle.getBufPageManager(bpm);
        this->type = type;
        this->len = len;
        this->offset = offset;
        this->op = op;
        this->val = val;
        pageID = 1;
        slotID = 0;
        curPage = nullptr;
        multiCondition = false;
    }
    bool openScan(const FileHandle &fileHandle, std::vector<CompareCondition> conditions)
    {
        handle = fileHandle;
        fileHandle.getFileID(fileID);
        fileHandle.getFileHeader(fh);
        fileHandle.getBufPageManager(bpm);
        this->conditions = conditions;
        pageID = 1;
        slotID = 0;
        curPage = nullptr;
        multiCondition = true;
    }
    bool getNextRec(Record &rec)
    {
        int index;
        for (; pageID < fh.numPages; pageID++)
        {
            curPage = reinterpret_cast<DataType>(bpm->getPage(fileID, pageID, index));
            SlotMap slotMap(curPage, fh.capacity);
            for (; slotID < fh.capacity; slotID++)
            {
                if (slotMap.test(slotID) && compare(&curPage[fh.slotMapSize + fh.slotSize * slotID]))
                {
                    handle.getRec(RID(pageID, slotID), rec);
                    slotID++;
                    return true;
                }
            }
        }
        return false;
    }
    bool closeScan() {}
    bool compare(DataType src)
    {
        if (multiCondition)
            return compareMultiple(src);
        else
            return compareSingle(src, op, type, offset, len, val);
    }
    bool compareSingle(DataType src, CompOp op, AttrType type, int offset, int len, void *val, int attrIdx = -1)
    {
        SlotMap nullMap(src, fh.slotSize);
        if (op == CompOp::NO)
            return true;
        if (op == CompOp::ISNULL)
            return nullMap.test(offset);
        if (op == CompOp::ISNOTNULL)
            return !nullMap.test(offset);
        if (multiCondition && nullMap.test(attrIdx))
            return false;
        switch (type)
        {
        case AttrType::INT:
        {
            int *lhs = reinterpret_cast<int *>(src + offset);
            int *rhs = reinterpret_cast<int *>(val);
            switch (op)
            {
            case CompOp::E:
                return *lhs == *rhs;
                break;
            case CompOp::L:
                return *lhs < *rhs;
                break;
            case CompOp::LE:
                return *lhs <= *rhs;
                break;
            case CompOp::G:
                return *lhs > *rhs;
                break;
            case CompOp::GE:
                return *lhs >= *rhs;
                break;
            case CompOp::NE:
                return *lhs != *rhs;
                break;

            default:
                return false;
                break;
            }
        }
        case AttrType::FLOAT:
        {
            float *lhs = reinterpret_cast<float *>(src + offset);
            float *rhs = reinterpret_cast<float *>(val);
            switch (op)
            {
            case CompOp::E:
                return *lhs == *rhs;
                break;
            case CompOp::L:
                return *lhs < *rhs;
                break;
            case CompOp::LE:
                return *lhs <= *rhs;
                break;
            case CompOp::G:
                return *lhs > *rhs;
                break;
            case CompOp::GE:
                return *lhs >= *rhs;
                break;
            case CompOp::NE:
                return *lhs != *rhs;
                break;

            default:
                return false;
                break;
            }
        }
        case AttrType::VARCHAR:
        {
            char *lhs = src + offset;
            char *rhs = reinterpret_cast<char *>(val);
            switch (op)
            {
            case CompOp::E:
                return strcmp(lhs, rhs) == 0;
                break;
            case CompOp::NE:
                return strcmp(lhs, rhs) != 0;
                break;
            default:
                return false;
                break;
            }
        }
        default:
            return false;
            break;
        }
    }
    bool compareSingle(DataType src, CompOp op, AttrType type, int offset, int len, int rhsOffset)
    {
        switch (type)
        {
        case AttrType::INT:
        {
            int *lhs = reinterpret_cast<int *>(src + offset);
            int *rhs = reinterpret_cast<int *>(src + rhsOffset);
            switch (op)
            {
            case CompOp::E:
                return *lhs == *rhs;
                break;
            case CompOp::L:
                return *lhs < *rhs;
                break;
            case CompOp::LE:
                return *lhs <= *rhs;
                break;
            case CompOp::G:
                return *lhs > *rhs;
                break;
            case CompOp::GE:
                return *lhs >= *rhs;
                break;
            case CompOp::NE:
                return *lhs != *rhs;
                break;

            default:
                return false;
                break;
            }
        }
        case AttrType::FLOAT:
        {
            float *lhs = reinterpret_cast<float *>(src + offset);
            float *rhs = reinterpret_cast<float *>(src + rhsOffset);
            switch (op)
            {
            case CompOp::E:
                return *lhs == *rhs;
                break;
            case CompOp::L:
                return *lhs < *rhs;
                break;
            case CompOp::LE:
                return *lhs <= *rhs;
                break;
            case CompOp::G:
                return *lhs > *rhs;
                break;
            case CompOp::GE:
                return *lhs >= *rhs;
                break;
            case CompOp::NE:
                return *lhs != *rhs;
                break;

            default:
                return false;
                break;
            }
        }
        case AttrType::VARCHAR:
        {
            char *lhs = src + offset;
            char *rhs = src + rhsOffset;
            switch (op)
            {
            case CompOp::E:
                return strcmp(lhs, rhs) == 0;
                break;
            case CompOp::NE:
                return strcmp(lhs, rhs) != 0;
                break;
            default:
                return false;
                break;
            }
        }
        default:
            return false;
            break;
        }
    }
    bool compareMultiple(DataType src)
    {
        for (auto cond : conditions)
        {
            if (cond.op == CompOp::IN)
            {
                bool flag = false;
                for (auto val : cond.vals)
                    if (compareSingle(src, CompOp::E, cond.type, cond.offset, cond.len, reinterpret_cast<void *>(&val), cond.attrIdx))
                    {
                        flag = true;
                        break;
                    }
                if (!flag)
                    return false;
            }
            else
            {
                if (cond.rhsAttr)
                {
                    if (!compareSingle(src, cond.op, cond.type, cond.offset, cond.len, cond.rhsOffset))
                        return false;
                }
                else if (!compareSingle(src, cond.op, cond.type, cond.offset, cond.len, reinterpret_cast<void *>(&cond.val), cond.attrIdx))
                    return false;
            }
        }
        return true;
    }
};
