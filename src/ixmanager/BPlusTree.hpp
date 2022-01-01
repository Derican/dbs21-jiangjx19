#pragma once

#include <vector>
#include <memory.h>
#include <assert.h>

#include "constants.h"

class TreeNode
{
public:
    NodeHeader header;
    std::vector<std::vector<int>> keys;
    std::vector<int> children;
    std::vector<RID> entries;

    /*
        1 if k1 < k2
        -1 if k1 > k2
        0 if k1 == k2
    */
    int keyCompare(const std::vector<int> &k1, const std::vector<int> &k2)
    {
        assert(k1.size() == k2.size());

        for (auto i = 0; i < k1.size(); i++)
        {
            if (k1[i] < k2[i])
                return 1;
            if (k1[i] > k2[i])
                return -1;
        }
        return 0;
    }

    bool searchChild(const int e, int &index)
    {
        int left = 0;
        while (left < children.size())
        {
            if (children[left] == e)
            {
                index = left;
                return true;
            }
            left++;
        }
        index = -1;
        return false;
    }

    bool searchKeyLowerBound(const std::vector<int> &e, int &index)
    {
        if (keys.size() <= 0)
        {
            index = 0;
            return false;
        }
        int left = 0;
        while (left < keys.size())
        {
            if (keyCompare(keys[left], e) <= 0)
            {
                index = left;
                return keyCompare(keys[index], e) == 0;
            }
            left++;
        }
        index = left;
        return false;
    }

    bool searchKeyUpperBound(const std::vector<int> &e, int &index)
    {
        if (keys.size() <= 0)
        {
            index = 0;
            return false;
        }
        int left = 0;
        while (left < keys.size())
        {
            if (keyCompare(keys[left], e) == -1)
            {
                index = left;
                return keyCompare(keys[index], e) == 0;
            }
            left++;
        }
        index = left;
        return false;
    }

    bool searchChild(const std::vector<int> &e, int &index)
    {
        if (keys.size() <= 0 || keyCompare(e, keys[0]) == 1)
        {
            index = 0;
            return false;
        }
        int left = 1;
        while (left < keys.size())
        {
            if (keyCompare(keys[left], e) == -1)
            {
                index = left;
                return keyCompare(keys[index - 1], e) == 0;
            }
            left++;
        }
        index = left;
        return keyCompare(keys[index - 1], e) == 0;
    }

    bool insertKeyChild(const std::vector<int> &e, int child)
    {
        int index;
        bool found = searchKeyUpperBound(e, index);
        // if (found)
        // {
        //     std::cout << "Duplicate key inserted." << std::endl;
        //     return false;
        // }
        keys.insert(keys.begin() + index, e);
        children.insert(children.begin() + index + 1, child);
        return true;
    }

    bool insertKeyEntry(const std::vector<int> &e, const RID &rid)
    {
        int index;
        bool found = searchKeyUpperBound(e, index);
        keys.insert(keys.begin() + index, e);
        entries.insert(entries.begin() + index, rid);
        return true;
    }

    bool deleteKeyEntry(const std::vector<int> &e, const RID &rid)
    {
        int l_index, r_index;
        bool found = searchKeyLowerBound(e, l_index);
        searchKeyUpperBound(e, r_index);
        if (!found)
        {
            std::cout << "Non-existed key deleted." << std::endl;
            return false;
        }
        for (int i = l_index; i < r_index; i++)
        {
            if (entries[i].equals(rid))
            {
                keys.erase(keys.begin() + i);
                entries.erase(entries.begin() + i);
            }
        }
        return true;
    }
};