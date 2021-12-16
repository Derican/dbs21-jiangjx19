#pragma once

#include <string>
#include <vector>
#include <iostream>
#include <iomanip>
#include <memory.h>
#include "../recmanager/constants.h"

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
    int nullable;
    int defaultValid;
    defaultValue defaultVal;
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
                out << "|" << std::left << std::setw(9) << value.defaultVal.Int << "|";
                break;
            }
            case AttrType::FLOAT:
            {
                out << "|" << std::left << std::setw(9) << value.defaultVal.Float << "|";
                break;
            }
            case AttrType::VARCHAR:
            {
                char val[10] = {"\0"};
                memcpy(val, value.defaultVal.String, 9);
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

inline void SplitString(const std::string &s, std::vector<std::string> &v, const std::string &c)
{
    std::string::size_type pos1, pos2;
    pos2 = s.find(c);
    pos1 = 0;
    while (std::string::npos != pos2)
    {
        v.push_back(s.substr(pos1, pos2 - pos1));

        pos1 = pos2 + c.size();
        pos2 = s.find(c, pos1);
    }
    if (pos1 != s.length())
        v.push_back(s.substr(pos1));
}
