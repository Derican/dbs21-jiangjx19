#pragma once

#include "../recmanager/RID.hpp"
#include "../recmanager/constants.h"

struct IndexHeader
{
    int numPages;
    int pairSize;
    int rootPage;
    int height;
    AttrType type;
    int typeLen;
};

#define IX_M 100