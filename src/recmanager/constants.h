#pragma once

#include "../utils/pagedef.h"

enum AttrType
{
    ANY,
    INT,
    FLOAT,
    VARCHAR
};

const std::string Type2String[] = {"ANY", "INT", "FLOAT", "VARCHAR"};

enum CompOp
{
    L,
    LE,
    GE,
    G,
    E,
    NE,
    NO
};

struct FileHeader
{
    int firstFree;
    int numPages;
    int slotSize;
    int slotMapSize;
    int capacity;
};

class SlotMap
{
public:
    DataType map;
    int size;
    SlotMap(DataType _map, int _size) : map(_map), size(_size) {}
    ~SlotMap()
    {
        map = nullptr;
    }
    bool test(int k)
    {
        if (k >= size)
            return false;
        return (map[k >> 3] >> (k & 0x7)) & 1;
    }
    bool set(int k)
    {
        if (k >= size)
            return false;
        map[k >> 3] |= 1 << (k & 0x7);
        return true;
    }
    bool remove(int k)
    {
        if (k >= size)
            return false;
        map[k >> 3] &= ~(1 << (k & 0x7));
        return true;
    }
};