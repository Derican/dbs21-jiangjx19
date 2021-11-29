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
        multiCondition = true;
    }
    bool getNextRec(Record &rec)
    {
        int index;
        for (; pageID < fh.numPages; pageID++)
        {
            if (curPage == nullptr)
                curPage = reinterpret_cast<DataType>(bpm->getPage(fileID, pageID, index));
            SlotMap slotMap(curPage, fh.slotMapSize);
            for (; slotID < fh.capacity; slotID++)
            {
                if (slotMap.test(slotID) && compare(&curPage[fh.slotMapSize + fh.slotSize * slotID]))
                {
                    handle.getRec(RID(pageID, slotID), rec);
                    slotID++;
                    return true;
                }
            }
            curPage = nullptr;
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
    bool compareSingle(DataType src, CompOp op, AttrType type, int offset, int len, void *val)
    {
        if (op == CompOp::NO)
            return true;
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
                    if (compareSingle(src, CompOp::E, cond.type, cond.offset, cond.len, reinterpret_cast<void *>(&val)))
                    {
                        flag = true;
                        break;
                    }
                if (!flag)
                    return false;
            }
            else if (!compareSingle(src, cond.op, cond.type, cond.offset, cond.len, reinterpret_cast<void *>(&cond.val)))
                return false;
        }
        return true;
    }
};
