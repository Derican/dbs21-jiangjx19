#pragma once

#include <string>
#include <memory.h>
#include "../recmanager/constants.h"
#include "IndexHandle.hpp"
#include "../fileio/FileManager.h"
#include "../bufmanager/BufPageManager.h"
#include "constants.h"

class IndexManager
{
private:
    FileManager *fm;
    BufPageManager *bpm;
    int openedID;

public:
    IndexManager() {}
    IndexManager(FileManager *_fm, BufPageManager *_bpm)
    {
        fm = _fm;
        bpm = _bpm;
    }
    ~IndexManager()
    {
        fm = nullptr;
        bpm = nullptr;
    }

    bool createIndex(const std::string filename, int indexNo, AttrType attrType, int attrLength)
    {
        string fn_ix = filename + '.' + to_string(indexNo);
        fm->createFile(fn_ix.c_str());
        int fileID;
        fm->openFile(fn_ix.c_str(), fileID);
        int pageID = 0;
        int index;
        BufType b = bpm->allocPage(fileID, pageID, index);
        DataType d = reinterpret_cast<DataType>(b);
        IndexHeader ih;
        ih.numPages = 1;
        if (attrType == AttrType::INT)
            ih.pairSize = 4 + sizeof(RID);
        else
            return false;
        ih.rootPage = -1;
        ih.height = 0;
        ih.type = attrType;
        ih.typeLen = attrLength;
        memcpy(d, &ih, sizeof(IndexHeader));
        bpm->markDirty(index);
        return true;
    }

    bool destroyIndex(const std::string filename, int indexNo)
    {
        string fn_ix = filename + '.' + to_string(indexNo);
    }

    bool openIndex(const std::string filename, int indexNo, IndexHandle &indexHandle)
    {
        if (openedID >= 0)
            return false;
        string fn_ix = filename + '.' + to_string(indexNo);
        int fileID;
        fm->openFile(fn_ix.c_str(), fileID);
        openedID = fileID;
        indexHandle = IndexHandle(fileID, bpm);
        return true;
    }

    bool closeIndex(IndexHandle &indexHandle)
    {
        if (openedID < 0)
            return false;
        fm->closeFile(openedID);
        openedID = -1;
        return true;
    }
};