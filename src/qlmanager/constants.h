#pragma once

#include <string>
#include "../recmanager/constants.h"

struct RelAttr
{
    std::string relName;
    std::string attrName;
};

struct Value
{
    AttrType type;
    unsigned int *pData;
};

struct Condition
{
    RelAttr lhs;
    CompOp op;
    int bRhsIsAttr;
    RelAttr rhs;
    Value rhsValue;
};
