#pragma once

#include <string>
#include "../recmanager/constants.h"

struct AttrInfo
{
    std::string attrName;
    AttrType attrType;
    int attrLength;
};

struct RelCat
{
    char relName[100];
    int tupleLength;
    int attrCount;
    int indexCount;
};

struct AttrCat
{
    char relName[100];
    char attrName[100];
    int offset;
    AttrType type;
    int typeLen;
    int indexNo;
};

#define DBNAME_MAX_BYTES 100
