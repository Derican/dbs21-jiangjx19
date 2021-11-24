#pragma once

#include "constants.h"
#include "FileHandle.hpp"

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
};
