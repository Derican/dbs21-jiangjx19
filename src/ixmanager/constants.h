#pragma once

#include "../recmanager/RID.hpp"
#include "../recmanager/constants.h"

struct IndexHeader
{
    int numPages;
    int rootPage;
    int height;
    AttrType type;
    int num_attrs;
};

#define IX_M 4

#define MAX_KEYS IX_M
#define MIN_KEYS IX_M / 2

enum NodeType
{
    INTERNAL,
    LEAF
};

struct NodeHeader
{
    NodeType type;
    int num_keys;
    int parent;
    int leftSibling;
    int rightSibling;
};