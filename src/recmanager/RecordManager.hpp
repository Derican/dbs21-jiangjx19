#pragma once

#include <memory.h>
#include <string>
#include <map>
#include "constants.h"
#include "FileHandle.hpp"
#include "../fileio/FileManager.h"
#include "../bufmanager/BufPageManager.h"

class RecordManager
{
private:
    FileManager *fm;
    BufPageManager *bpm;
    std::map<std::string, int> openedMap;

public:
    RecordManager() {}
    RecordManager(FileManager *_fm, BufPageManager *_bpm)
    {
        fm = _fm;
        bpm = _bpm;
    }
    ~RecordManager()
    {
        fm = nullptr;
        bpm = nullptr;
    }

    bool createFile(const std::string filename, int slotSize)
    {
        fm->createFile(filename.c_str());
        int fileID;
        fm->openFile(filename.c_str(), fileID);
        int pageID = 0;
        int index;
        BufType b = bpm->allocPage(fileID, pageID, index, false);
        DataType d = reinterpret_cast<DataType>(b);
        FileHeader fh;
        fh.firstFree = 1;
        fh.numPages = 1;
        fh.slotSize = slotSize;
        fh.slotMapSize = PAGE_SIZE / (1 + slotSize * 8) + 1;
        fh.capacity = (PAGE_SIZE - fh.slotMapSize) / fh.slotSize;
        memcpy(d, &fh, sizeof(fh));
        bpm->markDirty(index);
        bpm->close();
        fm->closeFile(fileID);
        return true;
    }
    bool destroyFile(const std::string filename)
    {
        remove(filename.c_str());
        return true;
    }
    bool openFile(const std::string filename, FileHandle &fileHandle)
    {
        auto it = openedMap.find(filename);
        if (it != openedMap.end())
            return false;
        int fileID;
        fm->openFile(filename.c_str(), fileID);
        openedMap[filename] = fileID;
        fileHandle = FileHandle(fileID, bpm);
        return true;
    }
    bool closeFile(const std::string filename)
    {
        auto it = openedMap.find(filename);
        if (it == openedMap.end())
            return false;
        bpm->close();
        fm->closeFile(it->second);
        openedMap.erase(it);
        return true;
    }
};
