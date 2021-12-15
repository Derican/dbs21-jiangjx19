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