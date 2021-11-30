#pragma once

#include "RID.hpp"
#include "../utils/pagedef.h"

class Record
{
private:
    RID rid;
    int recSize;
    DataType pData;

public:
    Record()
    {
        pData = nullptr;
    }
    Record(RID _rid, const DataType _pData, int _recSize)
    {
        rid = _rid;
        recSize = _recSize;
        pData = new char[recSize];
        memcpy(pData, _pData, recSize);
    }
    Record(const Record &_rec)
    {
        rid = _rec.rid;
        recSize = _rec.recSize;
        pData = new char[recSize];
        memcpy(pData, _rec.pData, recSize);
    }
    ~Record()
    {
        if (pData)
            delete[] pData;
    }
    bool getData(DataType &_pData) const
    {
        _pData = pData;
        return true;
    }
    bool getRID(RID &_rid) const
    {
        _rid = rid;
        return true;
    }
    bool set(const RID &_rid, const DataType &_pData, const int _recSize)
    {
        rid = _rid;
        if (pData == nullptr)
            pData = new char[_recSize];
        else if (_recSize != recSize)
        {
            delete[] pData;
            pData = new char[_recSize];
        }
        recSize = _recSize;

        memcpy(pData, _pData, recSize);
    }
};
