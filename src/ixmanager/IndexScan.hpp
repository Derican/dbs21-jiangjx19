#pragma once

#include "IndexHandle.hpp"
#include "../recmanager/constants.h"

class IndexScan
{
private:
    IndexHandle handle;
    int fileID;
    IndexHeader ih;
    BufPageManager *bpm;
    CompOp op;
    void *val;
    int pageID, slotID;
    BufType curPage;

public:
    IndexScan()
    {
        pageID = 1;
        slotID = 0;
        curPage = nullptr;
    }
    ~IndexScan() {}

    bool openScan(const IndexHandle &indexHandle, CompOp op, void *value)
    {
        handle = indexHandle;
        indexHandle.getFileID(fileID);
        indexHandle.getIndexHeader(ih);
        indexHandle.getBufPageManager(bpm);
        this->op = op;
        this->val = value;
    }

    bool getNextEntry(RID &rid) {}

    bool closeScan() {}
};