#pragma once

#include <memory.h>
#include <string>
#include "constants.h"
#include "FileHandle.hpp"
#include "../fileio/FileManager.h"
#include "../bufmanager/BufPageManager.h"

class RecordManager
{
private:
    FileManager *fm;
    BufPageManager *bpm;
    int openedID;

public:
    RecordManager() {}
    RecordManager(FileManager *_fm, BufPageManager *_bpm)
    {
        fm = _fm;
        bpm = _bpm;
        openedID = -1;
    }
    ~RecordManager()
    {
        fm = nullptr;
        bpm = nullptr;
    }

    bool CreateFile(const std::string filename, int slotSize)
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
    bool DestroyFile(const std::string filename)
    {
        return true;
    }
    bool OpenFile(const std::string filename, FileHandle &fileHandle)
    {
        if (openedID >= 0)
            return false;
        int fileID;
        fm->openFile(filename.c_str(), fileID);
        openedID = fileID;
        fileHandle = FileHandle(fileID, bpm);
        return true;
    }
    bool CloseFile(const std::string filename)
    {
        if (openedID < 0)
            return false;
        bpm->close();
        fm->closeFile(openedID);
        openedID = -1;
        return true;
    }
};
