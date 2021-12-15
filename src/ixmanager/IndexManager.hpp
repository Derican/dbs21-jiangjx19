#pragma once

#include <string>
#include <memory.h>
#include <map>
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
    std::map<std::string, int> openedMap;

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

    bool createIndex(const std::string filename, std::vector<int> &indexNo, AttrType attrType, int attrLength)
    {
        string fn_ix = filename;
        for (auto in : indexNo)
            fn_ix += '.' + to_string(in);
        fm->createFile(fn_ix.c_str());
        int fileID;
        fm->openFile(fn_ix.c_str(), fileID);
        int pageID = 0;
        int index;
        BufType b = bpm->allocPage(fileID, pageID, index);
        DataType d = reinterpret_cast<DataType>(b);
        IndexHeader ih;
        ih.numPages = 1;
        if (attrType != AttrType::INT)
        {
            std::cout << "Not support for non-int index." << std::endl;
            return false;
        }
        ih.rootPage = -1;
        ih.height = 0;
        ih.type = attrType;
        ih.num_attrs = attrLength / 4;
        memcpy(d, &ih, sizeof(IndexHeader));
        bpm->markDirty(index);
        bpm->writeBack(index);
        return true;
    }

    bool destroyIndex(const std::string filename, std::vector<int> &indexNo)
    {
        string fn_ix = filename;
        for (auto in : indexNo)
            fn_ix += '.' + to_string(in);
        remove(fn_ix.c_str());
        return true;
    }

    bool openIndex(const std::string filename, std::vector<int> &indexNo, IndexHandle &indexHandle)
    {
        string fn_ix = filename;
        for (auto in : indexNo)
            fn_ix += '.' + to_string(in);
        auto it = openedMap.find(fn_ix);
        if (it != openedMap.end())
            return false;
        int fileID;
        fm->openFile(fn_ix.c_str(), fileID);
        openedMap[fn_ix] = fileID;
        indexHandle = IndexHandle(fileID, bpm);
        return true;
    }

    bool closeIndex(const std::string filename, std::vector<int> &indexNo)
    {
        string fn_ix = filename;
        for (auto in : indexNo)
            fn_ix += '.' + to_string(in);
        auto it = openedMap.find(fn_ix);
        if (it == openedMap.end())
            return false;
        bpm->close();
        fm->closeFile(it->second);
        openedMap.erase(it);
        return true;
    }
};