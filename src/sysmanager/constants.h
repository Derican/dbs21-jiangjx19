#pragma once

#include <string>
#include <vector>
#include <iostream>
#include <iomanip>
#include <memory.h>
#include "../recmanager/constants.h"

#define DBNAME_MAX_BYTES 100
#define RELNAME_MAX_BYTES 100
#define ATTRNAME_MAX_BYTES 100
#define VARCHAR_MAX_BYTES 100

union defaultValue
{
    int Int;
    float Float;
    char String[VARCHAR_MAX_BYTES];
};

struct AttrInfo
{
    std::string attrName;
    AttrType attrType;
    int attrLength;
    int nullable;
    int defaultValid;
    defaultValue defVal;
};

struct PrimaryKey
{
    std::string relName;
    std::vector<std::string> keys;
};

struct ForeignKey
{
    std::string relName;
    std::vector<std::string> attrs;
    std::string ref;
    std::vector<std::string> refAttrs;
};

struct RelCat
{
    char relName[RELNAME_MAX_BYTES];
    int tupleLength;
    int attrCount;
    int indexCount;
};

struct AttrCat
{
    char relName[RELNAME_MAX_BYTES];
    char attrName[ATTRNAME_MAX_BYTES];
    int offset;
    AttrType type;
    int typeLen;
    int indexNo;
    int nullable;
    int defaultValid;
    char defaultVal[VARCHAR_MAX_BYTES];
    friend std::ostream &operator<<(std::ostream &out, AttrCat value)
    {
        out << "|" << std::setw(15) << std::left << value.attrName
            << "|" << std::setw(14) << std::left << Type2String[value.type];
        if (value.nullable == 1)
            out << "|Yes   ";
        else
            out << "|No    ";
        if (value.defaultValid == 1)
        {
            switch (value.type)
            {
            case AttrType::INT:
            {
                int *val = reinterpret_cast<int *>(value.defaultVal);
                out << "|" << std::left << std::setw(9) << *val << "|";
                break;
            }
            case AttrType::FLOAT:
            {
                float *val = reinterpret_cast<float *>(value.defaultVal);
                out << "|" << std::left << std::setw(9) << *val << "|";
                break;
            }
            case AttrType::VARCHAR:
            {
                char val[10] = {"\0"};
                memcpy(val, value.defaultVal, 9);
                out << "|" << std::left << std::setw(9) << val << "|";
                break;
            }
            default:
                break;
            }
        }
        else
            out << "|NULL     |";
        return out;
    }
};
