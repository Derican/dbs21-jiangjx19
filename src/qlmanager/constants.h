#pragma once

#include <string>
#include "../recmanager/constants.h"
#include "../sysmanager/constants.h"

struct RelAttr
{
    std::string relName;
    std::string attrName;
    RelAttr() {}
    RelAttr(std::string rel, std::string attr)
    {
        relName = rel;
        attrName = attr;
    }
    bool operator==(const RelAttr &t)
    {
        return relName == t.relName && attrName == t.attrName;
    }
};

struct Value
{
    AttrType type;
    int len;
    defaultValue pData;
    Value()
    {
        type = AttrType::NONE;
        len = 0;
        memset(&pData, 0, VARCHAR_MAX_BYTES);
    }
    bool operator==(const Value &v) const
    {
        return type == v.type && len == v.len && memcmp(&pData, &v.pData, len) == 0;
    }
    bool operator<(const Value &v) const
    {
        if (type != v.type)
            return false;
        if (type == AttrType::INT)
            return pData.Int < v.pData.Int;
        if (type == AttrType::FLOAT)
            return pData.Float < v.pData.Float;
        if (type == AttrType::VARCHAR)
            return strcmp(pData.String, v.pData.String) < 0;
        return false;
    }
    bool operator<=(const Value &v) const
    {
        if (type != v.type)
            return false;
        if (type == AttrType::INT)
            return pData.Int <= v.pData.Int;
        if (type == AttrType::FLOAT)
            return pData.Float <= v.pData.Float;
        if (type == AttrType::VARCHAR)
            return strcmp(pData.String, v.pData.String) <= 0;
        return false;
    }
    bool operator>(const Value &v) const
    {
        if (type != v.type)
            return false;
        if (type == AttrType::INT)
            return pData.Int > v.pData.Int;
        if (type == AttrType::FLOAT)
            return pData.Float > v.pData.Float;
        if (type == AttrType::VARCHAR)
            return strcmp(pData.String, v.pData.String) > 0;
        return false;
    }
    bool operator>=(const Value &v) const
    {
        if (type != v.type)
            return false;
        if (type == AttrType::INT)
            return pData.Int >= v.pData.Int;
        if (type == AttrType::FLOAT)
            return pData.Float >= v.pData.Float;
        if (type == AttrType::VARCHAR)
            return strcmp(pData.String, v.pData.String) >= 0;
        return false;
    }
    bool operator!=(const Value &v) const
    {
        if (type != v.type)
            return false;
        if (type == AttrType::INT)
            return pData.Int != v.pData.Int;
        if (type == AttrType::FLOAT)
            return pData.Float != v.pData.Float;
        if (type == AttrType::VARCHAR)
            return strcmp(pData.String, v.pData.String) != 0;
        return false;
    }
};

struct Condition
{
    RelAttr lhs;
    CompOp op;
    int bRhsIsAttr;
    RelAttr rhs;
    Value rhsValue;
    std::vector<Value> rhsValues;
};

inline CompOp negOp(CompOp op)
{
    switch (op)
    {
    case CompOp::L:
        return CompOp::G;
        break;
    case CompOp::LE:
        return CompOp::GE;
        break;
    case CompOp::G:
        return CompOp::L;
        break;
    case CompOp::GE:
        return CompOp::LE;
        break;
    default:
        return op;
        break;
    }
    return op;
}