#pragma once

#include "RID.hpp"
#include "../utils/pagedef.h"

class Record
{
private:
    RID rid;
    DataType pData;

public:
    Record() {}
    Record(RID _rid, DataType _pData)
    {
        rid = _rid;
        pData = _pData;
    }
    ~Record()
    {
        pData = nullptr;
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
};
