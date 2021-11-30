#pragma once

#include <memory.h>
#include "constants.h"
#include "RID.hpp"
#include "Record.hpp"
#include "../bufmanager/BufPageManager.h"

class FileHandle
{
private:
    int fileID;
    BufPageManager *bpm;
    FileHeader fh;

public:
    FileHandle() {}
    FileHandle(int _fileID, BufPageManager *_bpm)
    {
        fileID = _fileID;
        bpm = _bpm;
        int index;
        BufType b = bpm->getPage(fileID, 0, index);
        DataType d = reinterpret_cast<DataType>(b);
        memcpy(&fh, d, sizeof(fh));
        bpm->access(index);
    }
    ~FileHandle()
    {
        bpm = nullptr;
    }
    bool getFileID(int &_fileID) const
    {
        _fileID = fileID;
        return true;
    }
    bool getFileHeader(FileHeader &_fh) const
    {
        _fh = fh;
        return true;
    }
    bool getBufPageManager(BufPageManager *&_bpm) const
    {
        _bpm = bpm;
        return true;
    }
    bool getRec(const RID &rid, Record &rec) const
    {
        int index, pageID, slotID;
        rid.getPageID(pageID);
        rid.getSlotID(slotID);
        BufType b = bpm->getPage(fileID, pageID, index);
        DataType d = reinterpret_cast<DataType>(b);
        rec.set(rid, &d[fh.slotMapSize + fh.slotSize * slotID], fh.slotSize);
        bpm->access(index);
        return true;
    }
    bool insertRec(const DataType pData, RID &rid)
    {
        int index, pageID, slotID;
        getNextFreeSlot(rid);
        rid.getPageID(pageID);
        rid.getSlotID(slotID);
        BufType b = bpm->getPage(fileID, pageID, index);
        DataType d = reinterpret_cast<DataType>(b);
        memcpy(&d[fh.slotMapSize + fh.slotSize * slotID], pData, fh.slotSize);
        SlotMap slotMap(d, fh.capacity);
        slotMap.set(slotID);
        bpm->markDirty(index);
        bpm->writeBack(index);
        return true;
    }
    bool deleteRec(const RID &rid)
    {
        int index, pageID, slotID;
        rid.getPageID(pageID);
        rid.getSlotID(slotID);

        if (fh.firstFree > pageID)
        {
            fh.firstFree = pageID;
            BufType b = bpm->getPage(fileID, 0, index);
            DataType d = reinterpret_cast<DataType>(b);
            memcpy(d, &fh, sizeof(fh));
            bpm->markDirty(index);
            bpm->writeBack(index);
        }

        BufType b = bpm->getPage(fileID, pageID, index);
        DataType d = reinterpret_cast<DataType>(b);
        SlotMap slotMap(d, fh.capacity);
        slotMap.remove(slotID);
        bpm->markDirty(index);
        bpm->writeBack(index);
        return true;
    }
    bool updateRec(const Record &rec)
    {
        RID rid;
        rec.getRID(rid);
        DataType pData;
        rec.getData(pData);
        int index, pageID, slotID;
        rid.getPageID(pageID);
        rid.getSlotID(slotID);
        BufType b = bpm->getPage(fileID, pageID, index);
        DataType d = reinterpret_cast<DataType>(b);
        memcpy(&d[fh.slotMapSize + fh.slotSize * slotID], pData, fh.slotSize);
        SlotMap slotMap(d, fh.capacity);
        bpm->markDirty(index);
        return true;
    }
    bool forcePage(const int pageId) {}
    bool getNextFreeSlot(RID &rid)
    {
        int pageID = fh.firstFree;
        if (fh.firstFree >= fh.numPages)
            getNewPage(pageID);
        int index;
        BufType b = bpm->getPage(fileID, pageID, index);
        DataType d = reinterpret_cast<DataType>(b);
        SlotMap slotMap(d, fh.capacity);
        for (int i = 0; i < fh.capacity; i++)
        {
            if (!slotMap.test(i))
            {
                rid.setPageID(pageID);
                rid.setSlotID(i);
                return true;
            }
        }
        return false;
    }
    bool getNewPage(int &pageID)
    {
        pageID = fh.numPages;
        int index;
        bpm->allocPage(fileID, pageID, index, false);
        fh.numPages++;
        fh.firstFree = pageID;
        BufType b = bpm->getPage(fileID, 0, index);
        DataType d = reinterpret_cast<DataType>(b);
        memcpy(d, &fh, sizeof(fh));
        bpm->markDirty(index);
        bpm->writeBack(index);

        b = bpm->getPage(fileID, pageID, index);
        d = reinterpret_cast<DataType>(b);
        memset(d, 0, fh.slotMapSize);
        bpm->markDirty(index);
        bpm->writeBack(index);
        return true;
    }
};