#pragma once

#include <memory.h>
#include "../recmanager/RID.hpp"
#include "constants.h"
#include "../fileio/FileManager.h"
#include "../bufmanager/BufPageManager.h"

class IndexHandle
{
private:
    int fileID;
    BufPageManager *bpm;
    IndexHeader ih;

public:
    IndexHandle() {}
    IndexHandle(int _fileID, BufPageManager *_bpm)
    {
        fileID = _fileID;
        bpm = _bpm;
        int index;
        BufType b = bpm->getPage(fileID, 0, index);
        memcpy(&ih, b, sizeof(IndexHeader));
        bpm->access(index);
    }
    ~IndexHandle()
    {
        bpm = nullptr;
    }

    bool getFileID(int &_fileID) const
    {
        _fileID = fileID;
        return true;
    }

    bool getIndexHeader(IndexHeader &_ih) const
    {
        _ih = ih;
        return true;
    }

    bool getBufPageManager(BufPageManager *&_bpm) const
    {
        _bpm = bpm;
        return true;
    }

    bool insertEntry(const DataType pData, const RID &rid)
    {
        return true;
    }

    bool deleteEntry(const DataType pData, const RID &rid)
    {
        return true;
    }

    bool forcePages() {}
};