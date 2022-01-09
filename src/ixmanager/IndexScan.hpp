#pragma once

#include "IndexHandle.hpp"
#include "../recmanager/constants.h"

class IndexScan
{
private:
    IndexHandle handle;
    int fileID;
    IndexHeader ih;
    BufPageManager *bpm;
    CompOp op;
    std::vector<int> keys;
    int pageID, slotID;
    std::shared_ptr<TreeNode> curNode;

public:
    IndexScan()
    {
        pageID = -1;
        slotID = 0;
        curNode = nullptr;
    }
    ~IndexScan() {}

    bool openScan(const IndexHandle &indexHandle, CompOp op, std::vector<int> keys)
    {
        handle = indexHandle;
        indexHandle.getFileID(fileID);
        indexHandle.getIndexHeader(ih);
        indexHandle.getBufPageManager(bpm);
        this->op = op;
        this->keys = keys;
        slotID = 0;
        curNode = nullptr;

        switch (this->op)
        {
        case CompOp::L:
        case CompOp::LE:
            handle.getMostLeft(pageID);
            break;

        case CompOp::G:
        case CompOp::GE:
        case CompOp::E:
            handle.searchEntry(ih.rootPage, keys, pageID, slotID);
            break;
        case CompOp::BETWEEN:
        case CompOp::BETWEENL:
        case CompOp::BETWEENR:
        case CompOp::BETWEENLR:
            handle.searchEntry(ih.rootPage, std::vector<int>(keys.begin(), keys.begin() + ih.num_attrs), pageID, slotID);
            break;
        default:
            break;
        }

        if (pageID > 0 && this->op == CompOp::G)
        {
            std::shared_ptr<TreeNode> tmp;
            handle.loadTreeNode(pageID, tmp);
            while (tmp->keyCompare(tmp->keys[slotID], keys) != -1)
            {
                slotID++;
                if (slotID == tmp->keys.size())
                {
                    pageID = tmp->header.rightSibling;
                    slotID = 0;
                    if (pageID > 0)
                        handle.loadTreeNode(pageID, tmp);
                    else
                        break;
                }
            }
        }
        if (pageID > 0 && (this->op == CompOp::BETWEEN || this->op == CompOp::BETWEENR))
        {
            std::shared_ptr<TreeNode> tmp;
            handle.loadTreeNode(pageID, tmp);
            while (tmp->keyCompare(tmp->keys[slotID], std::vector<int>(keys.begin(), keys.begin() + ih.num_attrs)) != -1)
            {
                slotID++;
                if (slotID == tmp->keys.size())
                {
                    pageID = tmp->header.rightSibling;
                    slotID = 0;
                    if (pageID > 0)
                        handle.loadTreeNode(pageID, tmp);
                    else
                        break;
                }
            }
        }
    }

    bool getNextEntry(RID &rid)
    {
        if (pageID <= 0)
            return false;
        if (curNode == nullptr)
        {
            handle.loadTreeNode(pageID, curNode);
        }
        if (slotID == curNode->keys.size())
        {
            pageID = curNode->header.rightSibling;
            if (pageID <= 0)
                return false;
            handle.loadTreeNode(pageID, curNode);
            slotID = 0;
        }
        switch (this->op)
        {
        case CompOp::L:
            if (curNode->keyCompare(curNode->keys[slotID], keys) == 1)
            {
                rid = curNode->entries[slotID];
                slotID++;
                return true;
            }
            break;
        case CompOp::LE:
            if (curNode->keyCompare(curNode->keys[slotID], keys) >= 0)
            {
                rid = curNode->entries[slotID];
                slotID++;
                return true;
            }
            break;
        case CompOp::G:
            if (curNode->keyCompare(curNode->keys[slotID], keys) == -1)
            {
                rid = curNode->entries[slotID];
                slotID++;
                return true;
            }
            break;
        case CompOp::GE:
            if (curNode->keyCompare(curNode->keys[slotID], keys) <= 0)
            {
                rid = curNode->entries[slotID];
                slotID++;
                return true;
            }
            break;
        case CompOp::E:
            if (curNode->keyCompare(curNode->keys[slotID], keys) == 0)
            {
                rid = curNode->entries[slotID];
                slotID++;
                return true;
            }
            break;
        case CompOp::BETWEEN:
        case CompOp::BETWEENL:
            if (curNode->keyCompare(curNode->keys[slotID], std::vector<int>(keys.begin() + ih.num_attrs, keys.end())) == 1)
            {
                rid = curNode->entries[slotID];
                slotID++;
                return true;
            }
            break;
        case CompOp::BETWEENR:
        case CompOp::BETWEENLR:
            if (curNode->keyCompare(curNode->keys[slotID], std::vector<int>(keys.begin() + ih.num_attrs, keys.end())) >= 0)
            {
                rid = curNode->entries[slotID];
                slotID++;
                return true;
            }
            break;
        default:
            break;
        }
        return false;
    }

    bool closeScan()
    {
        if (curNode)
            curNode.reset();
        return true;
    }
};