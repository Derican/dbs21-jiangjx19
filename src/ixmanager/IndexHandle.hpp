#pragma once

#include <memory>
#include <map>
#include <memory.h>
#include "../recmanager/RID.hpp"
#include "constants.h"
#include "../fileio/FileManager.h"
#include "../bufmanager/BufPageManager.h"
#include "BPlusTree.hpp"

class IndexHandle
{
private:
    int fileID;
    BufPageManager *bpm;
    IndexHeader ih;
    std::map<int, std::shared_ptr<TreeNode>> nodesMap;

public:
    IndexHandle() {}
    IndexHandle(int _fileID, BufPageManager *_bpm)
    {
        fileID = _fileID;
        bpm = _bpm;
        int index;
        BufType b = bpm->getPage(fileID, 0, index);
        memcpy(&ih, b, sizeof(IndexHeader));
        bpm->access(index);
    }
    ~IndexHandle()
    {
        bpm = nullptr;
    }

    bool getFileID(int &_fileID) const
    {
        _fileID = fileID;
        return true;
    }

    bool getIndexHeader(IndexHeader &_ih) const
    {
        _ih = ih;
        return true;
    }

    bool getBufPageManager(BufPageManager *&_bpm) const
    {
        _bpm = bpm;
        return true;
    }

    bool insertEntry(const std::vector<int> &key, const RID &rid)
    {
        int pID = ih.rootPage;
        if (pID <= 0)
        {
            std::shared_ptr<TreeNode> node(new TreeNode());
            getNewPage(pID);
            ih.height = 1;
            ih.rootPage = pID;
            saveIndexHeader();
            node->header.pageID = pID;
            node->header.leftSibling = -1;
            node->header.num_keys = 0;
            node->header.parent = -1;
            node->header.rightSibling = -1;
            node->header.type = NodeType::LEAF;
            saveTreeNode(pID, node);
        }

        std::shared_ptr<TreeNode> node;
        loadTreeNode(pID, node);
        int index;
        while (node->header.type != NodeType::LEAF)
        {
            node->searchChild(key, index);
            pID = node->children[index];
            loadTreeNode(pID, node);
        }
        node->insertKeyEntry(key, rid);
        saveTreeNode(pID, node);

        if (node->keys.size() > MAX_KEYS)
            splitLeafNode(node);
        return true;
    }

    bool insertChildren(const std::vector<int> &child, int leftChildPID, int rightChildPID, std::shared_ptr<TreeNode> node, int pID)
    {
        node->insertKeyChild(child, rightChildPID);
        saveTreeNode(pID, node);

        if (node->keys.size() > MAX_KEYS)
            splitInternalNode(node);

        return true;
    }

    bool deleteEntry(const std::vector<int> &key, const RID &rid)
    {
        int pID = ih.rootPage;
        if (pID <= 0)
        {
            return false;
        }
        std::shared_ptr<TreeNode> node;
        loadTreeNode(pID, node);
        int index;
        while (node->header.type != NodeType::LEAF)
        {
            node->searchChild(key, index);
            pID = node->children[index];
            loadTreeNode(pID, node);
        }
        node->deleteKeyEntry(key, rid);
        saveTreeNode(pID, node);

        if (node->keys.size() >= MIN_KEYS || pID == ih.rootPage)
            return true;

        if (borrowKeyEntry(node, false))
        {
            saveTreeNode(pID, node);
            return true;
        }

        if (borrowKeyEntry(node, true))
        {
            saveTreeNode(pID, node);
            return true;
        }

        int deleted_child = node->header.leftSibling;
        if (mergeEntry(node, false))
        {
            saveTreeNode(pID, node);
            std::shared_ptr<TreeNode> p;
            loadTreeNode(node->header.parent, p);
            deleteKey(p, deleted_child);
            saveTreeNode(node->header.parent, p);

            if (ih.rootPage == pID)
                node->header.parent = -1;
            saveTreeNode(pID, node);
            return true;
        }

        if (mergeEntry(node, true))
        {
            std::shared_ptr<TreeNode> p;
            loadTreeNode(node->header.parent, p);
            deleteKey(p, pID);
            saveTreeNode(node->header.parent, p);

            if (ih.rootPage == node->header.rightSibling)
            {
                std::shared_ptr<TreeNode> r;
                loadTreeNode(node->header.rightSibling, r);
                r->header.parent = -1;
                saveTreeNode(node->header.rightSibling, r);
            }
            return true;
        }

        return false;
    }

    bool deleteKey(std::shared_ptr<TreeNode> node, int child)
    {
        int pID = node->header.pageID;
        int index;
        bool found = node->searchChild(child, index);
        assert(found);

        node->keys.erase(node->keys.begin() + index);
        node->children.erase(node->children.begin() + index);

        if (node->keys.size() <= 0 && pID == ih.rootPage)
        {
            ih.height--;
            ih.rootPage = node->children[0];
            saveIndexHeader();
            return true;
        }

        if (node->keys.size() >= MIN_KEYS)
            return true;

        if (borrowKeyChild(node, false))
        {
            saveTreeNode(pID, node);
            return true;
        }

        if (borrowKeyChild(node, true))
        {
            saveTreeNode(pID, node);
            return true;
        }

        int deleted_child = node->header.leftSibling;
        if (mergeChild(node, false))
        {
            std::shared_ptr<TreeNode> p;
            loadTreeNode(node->header.parent, p);
            deleteKey(p, deleted_child);
            saveTreeNode(node->header.parent, p);

            if (ih.rootPage == pID)
                node->header.parent = -1;
            return true;
        }

        if (mergeChild(node, true))
        {
            std::shared_ptr<TreeNode> p;
            loadTreeNode(node->header.parent, p);
            deleteKey(p, pID);
            saveTreeNode(node->header.parent, p);

            if (ih.rootPage == node->header.rightSibling)
            {
                std::shared_ptr<TreeNode> r;
                loadTreeNode(node->header.rightSibling, r);
                r->header.parent = -1;
                saveTreeNode(node->header.rightSibling, r);
            }
            return true;
        }

        return false;
    }

    bool forcePages() {}

    bool getNewPage(int &pageID)
    {
        pageID = ih.numPages;
        int index;
        bpm->allocPage(fileID, pageID, index, false);
        ih.numPages++;
        BufType b = bpm->getPage(fileID, 0, index);
        DataType d = reinterpret_cast<DataType>(b);
        memcpy(d, &ih, sizeof(ih));
        bpm->markDirty(index);
        bpm->writeBack(index);

        b = bpm->getPage(fileID, pageID, index);
        d = reinterpret_cast<DataType>(b);
        memset(d, 0, sizeof(NodeHeader));
        bpm->markDirty(index);
        bpm->writeBack(index);
        return true;
    }

    bool searchEntry(const std::vector<int> &key)
    {
        int pID = ih.rootPage;
        int pageID, slotID;
        return searchEntry(pID, key, pageID, slotID);
    }

    bool searchEntry(const int pID, const std::vector<int> &key, int &pageID, int &slotID)
    {
        if (pID <= 0)
            return false;
        else
        {
            std::shared_ptr<TreeNode> node;
            loadTreeNode(pID, node);
            int index;
            bool found = node->searchChild(key, index);
            if (node->header.type == NodeType::INTERNAL)
            {
                return searchEntry(node->children[index], key, pageID, slotID);
            }
            else
            {
                pageID = pID;
                node->searchKeyLowerBound(key, slotID);
                return found;
            }
        }
    }

    bool getMostLeft(int &pageID)
    {
        int pID = ih.rootPage;
        std::shared_ptr<TreeNode> t;
        loadTreeNode(pID, t);
        while (t->header.type == NodeType::INTERNAL)
        {
            pID = t->children[0];
            loadTreeNode(pID, t);
        }
        pageID = pID;
        return true;
    }

    bool loadTreeNode(const int pID, std::shared_ptr<TreeNode> &node)
    {
        auto it = nodesMap.find(pID);
        if (it != nodesMap.end() && it->second.use_count() != 0)
        {
            node = it->second;
            return true;
        }
        node = std::shared_ptr<TreeNode>(new TreeNode());
        nodesMap[pID] = node;

        int index;
        BufType b = bpm->getPage(fileID, pID, index);
        DataType d = reinterpret_cast<DataType>(b);

        memcpy(&node->header, d, sizeof(NodeHeader));
        if (node->header.type == NodeType::INTERNAL)
        {
            for (auto i = 0; i <= node->header.num_keys && node->header.num_keys > 0; i++)
            {
                int child;
                memcpy(&child, &d[sizeof(NodeHeader) + i * (4 + 4 * ih.num_attrs)], 4);
                node->children.push_back(child);

                if (i < node->header.num_keys)
                {
                    std::vector<int> key;
                    for (auto j = 0; j < ih.num_attrs; j++)
                    {
                        int tmp;
                        memcpy(&tmp, &d[sizeof(NodeHeader) + i * (4 + 4 * ih.num_attrs) + 4 + j * 4], 4);
                        key.push_back(tmp);
                    }

                    node->keys.push_back(key);
                }
            }
        }
        else
        {
            for (auto i = 0; i < node->header.num_keys && node->header.num_keys > 0; i++)
            {
                RID entry;
                memcpy(&entry, &d[sizeof(NodeHeader) + i * (sizeof(RID) + 4 * ih.num_attrs)], sizeof(RID));
                node->entries.push_back(entry);

                std::vector<int> key;
                for (auto j = 0; j < ih.num_attrs; j++)
                {
                    int tmp;
                    memcpy(&tmp, &d[sizeof(NodeHeader) + i * (sizeof(RID) + 4 * ih.num_attrs) + sizeof(RID) + j * 4], 4);
                    key.push_back(tmp);
                }

                node->keys.push_back(key);
            }
        }
        return true;
    }

    bool saveTreeNode(const int pID, std::shared_ptr<TreeNode> node)
    {
        int index;
        BufType b = bpm->getPage(fileID, pID, index);
        DataType d = reinterpret_cast<DataType>(b);

        node->header.num_keys = node->keys.size();
        memcpy(d, &node->header, sizeof(NodeHeader));
        if (node->header.type == NodeType::INTERNAL)
        {
            for (auto i = 0; i <= node->header.num_keys && node->header.num_keys > 0; i++)
            {
                memcpy(&d[sizeof(NodeHeader) + i * (4 + 4 * ih.num_attrs)], &node->children[i], 4);

                for (auto j = 0; j < ih.num_attrs && i < node->header.num_keys; j++)
                {
                    memcpy(&d[sizeof(NodeHeader) + i * (4 + 4 * ih.num_attrs) + 4 + j * 4], &node->keys[i][j], 4);
                }
            }
        }
        else
        {
            for (auto i = 0; i < node->header.num_keys && node->header.num_keys > 0; i++)
            {
                memcpy(&d[sizeof(NodeHeader) + i * (sizeof(RID) + 4 * ih.num_attrs)], &node->entries[i], sizeof(RID));

                for (auto j = 0; j < ih.num_attrs && i < node->header.num_keys; j++)
                {
                    memcpy(&d[sizeof(NodeHeader) + i * (sizeof(RID) + 4 * ih.num_attrs) + sizeof(RID) + j * 4], &node->keys[i][j], 4);
                }
            }
        }
        bpm->markDirty(index);
        bpm->writeBack(index);
        return true;
    }

    bool saveIndexHeader()
    {
        int index;
        BufType b = bpm->getPage(fileID, 0, index);
        DataType d = reinterpret_cast<DataType>(b);

        memcpy(d, &ih, sizeof(IndexHeader));
        bpm->markDirty(index);
        bpm->writeBack(index);
        return true;
    }

    bool splitLeafNode(std::shared_ptr<TreeNode> node)
    {
        int pID = node->header.pageID;
        int surrogatePID;
        getNewPage(surrogatePID);

        std::shared_ptr<TreeNode> surrogateNode(new TreeNode());
        saveTreeNode(surrogatePID, surrogateNode);
        surrogateNode.reset();
        loadTreeNode(surrogatePID, surrogateNode);

        surrogateNode->header.type = NodeType::LEAF;
        surrogateNode->header.pageID = surrogatePID;
        surrogateNode->header.leftSibling = pID;
        surrogateNode->header.rightSibling = node->header.rightSibling;
        surrogateNode->keys.insert(surrogateNode->keys.end(), node->keys.begin() + MIN_KEYS, node->keys.end());
        surrogateNode->entries.insert(surrogateNode->entries.end(), node->entries.begin() + MIN_KEYS, node->entries.end());
        node->keys.resize(MIN_KEYS);
        node->entries.resize(MIN_KEYS);

        if (node->header.rightSibling > 0)
        {
            std::shared_ptr<TreeNode> tmp;
            loadTreeNode(node->header.rightSibling, tmp);
            tmp->header.leftSibling = surrogatePID;
            saveTreeNode(node->header.rightSibling, tmp);
        }
        node->header.rightSibling = surrogatePID;

        if (node->header.parent > 0)
        {
            surrogateNode->header.parent = node->header.parent;
            saveTreeNode(surrogatePID, surrogateNode);
            saveTreeNode(pID, node);

            std::shared_ptr<TreeNode> parentNode;
            loadTreeNode(node->header.parent, parentNode);
            insertChildren(surrogateNode->keys.front(), pID, surrogatePID, parentNode, node->header.parent);
        }
        else
        {
            int parentPID;
            getNewPage(parentPID);
            std::shared_ptr<TreeNode> parent(new TreeNode());
            parent->header.pageID = parentPID;
            parent->header.num_keys = 1;
            parent->header.parent = -1;
            parent->header.leftSibling = -1;
            parent->header.rightSibling = -1;
            parent->header.type = NodeType::INTERNAL;

            surrogateNode->header.parent = parentPID;
            node->header.parent = parentPID;

            parent->keys.push_back(surrogateNode->keys.front());
            parent->children.push_back(pID);
            parent->children.push_back(surrogatePID);

            saveTreeNode(parentPID, parent);
            saveTreeNode(surrogatePID, surrogateNode);
            saveTreeNode(pID, node);

            ih.height++;
            ih.rootPage = parentPID;
            saveIndexHeader();
        }
        return true;
    }

    bool splitInternalNode(std::shared_ptr<TreeNode> node)
    {
        int pID = node->header.pageID;
        int surrogatePID;
        getNewPage(surrogatePID);

        std::shared_ptr<TreeNode> surrogateNode(new TreeNode());
        saveTreeNode(surrogatePID, surrogateNode);
        surrogateNode.reset();
        loadTreeNode(surrogatePID, surrogateNode);

        surrogateNode->header.type = NodeType::INTERNAL;
        surrogateNode->header.pageID = surrogatePID;
        surrogateNode->header.rightSibling = node->header.rightSibling;
        for (auto i = MIN_KEYS + 1; i <= MAX_KEYS; i++)
        {
            surrogateNode->keys.push_back(node->keys[i]);
        }
        for (auto i = MIN_KEYS + 1; i <= MAX_KEYS + 1; i++)
        {
            surrogateNode->children.push_back(node->children[i]);

            std::shared_ptr<TreeNode> childNode;
            loadTreeNode(node->children[i], childNode);
            childNode->header.parent = surrogatePID;
            saveTreeNode(node->children[i], childNode);
        }
        std::vector<int> carryKey = node->keys[MIN_KEYS];
        node->keys.resize(MIN_KEYS);
        node->children.resize(MIN_KEYS + 1);

        if (node->header.rightSibling > 0)
        {
            std::shared_ptr<TreeNode> tmp;
            loadTreeNode(node->header.rightSibling, tmp);
            tmp->header.leftSibling = surrogatePID;
            saveTreeNode(node->header.rightSibling, tmp);
        }
        node->header.rightSibling = surrogatePID;

        if (node->header.parent > 0)
        {
            surrogateNode->header.parent = node->header.parent;
            saveTreeNode(surrogatePID, surrogateNode);
            saveTreeNode(pID, node);

            std::shared_ptr<TreeNode> parentNode;
            loadTreeNode(node->header.parent, parentNode);
            insertChildren(carryKey, pID, surrogatePID, parentNode, node->header.parent);
        }
        else
        {
            int parentPID;
            getNewPage(parentPID);
            std::shared_ptr<TreeNode> parent(new TreeNode());
            parent->header.pageID = parentPID;
            parent->header.num_keys = 1;
            parent->header.parent = -1;
            parent->header.leftSibling = -1;
            parent->header.rightSibling = -1;
            parent->header.type = NodeType::INTERNAL;

            surrogateNode->header.parent = parentPID;
            node->header.parent = parentPID;

            parent->keys.push_back(carryKey);
            parent->children.push_back(pID);
            parent->children.push_back(surrogatePID);

            saveTreeNode(parentPID, parent);
            saveTreeNode(surrogatePID, surrogateNode);
            saveTreeNode(pID, node);

            ih.height++;
            ih.rootPage = parentPID;
            saveIndexHeader();
        }
        return true;
    }

    bool borrowKeyEntry(std::shared_ptr<TreeNode> node, bool fromRight)
    {
        int pID = node->header.pageID;
        int lenderPID = fromRight ? node->header.rightSibling : node->header.leftSibling;
        if (lenderPID <= 0)
            return false;
        std::shared_ptr<TreeNode> lenderNode;
        loadTreeNode(lenderPID, lenderNode);

        assert(lenderNode->keys.size() >= MIN_KEYS && lenderNode->keys.size() <= MAX_KEYS);

        if (lenderNode->keys.size() == MIN_KEYS)
            return false;
        if (fromRight)
        {
            node->keys.push_back(lenderNode->keys[0]);
            node->entries.push_back(lenderNode->entries[0]);

            lenderNode->keys.erase(lenderNode->keys.begin());
            lenderNode->entries.erase(lenderNode->entries.begin());

            std::shared_ptr<TreeNode> p;
            loadTreeNode(node->header.parent, p);
            changeParentChild(p, node->keys.back(), lenderNode->keys.front());
            saveTreeNode(node->header.parent, p);
        }
        else
        {
            node->keys.insert(node->keys.begin(), lenderNode->keys.back());
            node->entries.insert(node->entries.begin(), lenderNode->entries.back());

            lenderNode->keys.pop_back();
            lenderNode->entries.pop_back();

            std::shared_ptr<TreeNode> p;
            loadTreeNode(lenderNode->header.parent, p);
            changeParentChild(p, lenderNode->keys.back(), node->keys.front());
            saveTreeNode(lenderNode->header.parent, p);
        }

        saveTreeNode(pID, node);
        saveTreeNode(lenderPID, lenderNode);

        return true;
    }

    bool borrowKeyChild(std::shared_ptr<TreeNode> node, bool fromRight)
    {
        int pID = node->header.pageID;
        int lenderPID = fromRight ? node->header.rightSibling : node->header.leftSibling;
        if (lenderPID <= 0)
            return false;
        std::shared_ptr<TreeNode> lenderNode;
        loadTreeNode(lenderPID, lenderNode);

        assert(lenderNode->keys.size() >= MIN_KEYS && lenderNode->keys.size() <= MAX_KEYS);

        if (lenderNode->keys.size() == MIN_KEYS)
            return false;
        if (fromRight)
        {
            std::shared_ptr<TreeNode> p;
            loadTreeNode(node->header.parent, p);
            int index;
            p->searchChild(pID, index);

            node->keys.push_back(p->keys[index]);
            node->children.push_back(lenderNode->children[0]);

            std::shared_ptr<TreeNode> childNode;
            loadTreeNode(lenderNode->children[0], childNode);
            childNode->header.parent = pID;
            saveTreeNode(lenderNode->children[0], childNode);

            changeParentChild(p, p->keys[index], lenderNode->keys[0]);

            lenderNode->keys.erase(lenderNode->keys.begin());
            lenderNode->children.erase(lenderNode->children.begin());

            saveTreeNode(node->header.parent, p);
        }
        else
        {
            std::shared_ptr<TreeNode> p;
            loadTreeNode(node->header.parent, p);
            int index;
            p->searchChild(lenderPID, index);

            node->keys.insert(node->keys.begin(), p->keys[index]);
            node->children.insert(node->children.begin(), lenderNode->children.back());

            std::shared_ptr<TreeNode> childNode;
            loadTreeNode(lenderNode->children.back(), childNode);
            childNode->header.parent = pID;
            saveTreeNode(lenderNode->children.back(), childNode);

            changeParentChild(p, p->keys[index], lenderNode->keys.back());

            lenderNode->keys.pop_back();
            lenderNode->children.pop_back();

            saveTreeNode(node->header.parent, p);
        }

        saveTreeNode(pID, node);
        saveTreeNode(lenderPID, lenderNode);

        return true;
    }

    bool changeParentChild(std::shared_ptr<TreeNode> parent, const std::vector<int> &o, const std::vector<int> &n)
    {
        int index;
        bool found = parent->searchChild(o, index);
        // assert(found);
        parent->keys[index - 1] = n;
        if (index == parent->keys.size() && parent->header.parent > 0)
        {
            std::shared_ptr<TreeNode> p;
            loadTreeNode(parent->header.parent, p);
            changeParentChild(p, o, n);
            saveTreeNode(parent->header.parent, p);
        }
        return true;
    }

    bool mergeEntry(std::shared_ptr<TreeNode> node, bool withRight)
    {
        int pID = node->header.pageID;
        int siblingPID = withRight ? node->header.rightSibling : node->header.leftSibling;
        if (siblingPID <= 0)
            return false;
        std::shared_ptr<TreeNode> siblingNode;
        loadTreeNode(siblingPID, siblingNode);
        if (siblingNode->header.parent != node->header.parent)
            return false;

        if (withRight)
        {
            siblingNode->keys.insert(siblingNode->keys.begin(), node->keys.begin(), node->keys.end());
            siblingNode->entries.insert(siblingNode->entries.begin(), node->entries.begin(), node->entries.end());

            siblingNode->header.leftSibling = node->header.leftSibling;
            if (node->header.leftSibling > 0)
            {
                std::shared_ptr<TreeNode> l;
                loadTreeNode(node->header.leftSibling, l);
                l->header.rightSibling = siblingPID;
                saveTreeNode(node->header.leftSibling, l);
            }
        }
        else
        {
            node->keys.insert(node->keys.begin(), siblingNode->keys.begin(), siblingNode->keys.end());
            node->entries.insert(node->entries.begin(), siblingNode->entries.begin(), siblingNode->entries.end());

            node->header.leftSibling = siblingNode->header.leftSibling;
            if (siblingNode->header.leftSibling > 0)
            {
                std::shared_ptr<TreeNode> ll;
                loadTreeNode(siblingNode->header.leftSibling, ll);
                ll->header.rightSibling = pID;
                saveTreeNode(siblingNode->header.leftSibling, ll);
            }
        }

        saveTreeNode(siblingPID, siblingNode);
        return true;
    }

    bool mergeChild(std::shared_ptr<TreeNode> node, bool withRight)
    {
        int pID = node->header.pageID;
        int siblingPID = withRight ? node->header.rightSibling : node->header.leftSibling;
        if (siblingPID <= 0)
            return false;
        std::shared_ptr<TreeNode> siblingNode;
        loadTreeNode(siblingPID, siblingNode);
        if (siblingNode->header.parent != node->header.parent)
            return false;

        std::shared_ptr<TreeNode> p;
        loadTreeNode(node->header.parent, p);
        int index;
        bool found = p->searchChild(pID, index);
        assert(found);
        if (withRight)
        {
            siblingNode->keys.insert(siblingNode->keys.begin(), p->keys[index]);
            siblingNode->keys.insert(siblingNode->keys.begin(), node->keys.begin(), node->keys.end());
            siblingNode->children.insert(siblingNode->children.begin(), node->children.begin(), node->children.end());

            for (auto childPID : node->children)
            {
                std::shared_ptr<TreeNode> c;
                loadTreeNode(childPID, c);
                c->header.parent = siblingPID;
                saveTreeNode(childPID, c);
            }

            siblingNode->header.leftSibling = node->header.leftSibling;
            if (node->header.leftSibling > 0)
            {
                std::shared_ptr<TreeNode> l;
                loadTreeNode(node->header.leftSibling, l);
                l->header.rightSibling = siblingPID;
                saveTreeNode(node->header.leftSibling, l);
            }
        }
        else
        {
            node->keys.insert(node->keys.begin(), p->keys[index]);
            node->keys.insert(node->keys.begin(), siblingNode->keys.begin(), siblingNode->keys.end());
            node->children.insert(node->children.begin(), siblingNode->children.begin(), siblingNode->children.end());

            for (auto childPID : siblingNode->children)
            {
                std::shared_ptr<TreeNode> c;
                loadTreeNode(childPID, c);
                c->header.parent = pID;
                saveTreeNode(childPID, c);
            }

            node->header.leftSibling = siblingNode->header.leftSibling;
            if (siblingNode->header.leftSibling > 0)
            {
                std::shared_ptr<TreeNode> ll;
                loadTreeNode(siblingNode->header.leftSibling, ll);
                ll->header.rightSibling = pID;
                saveTreeNode(siblingNode->header.leftSibling, ll);
            }
        }

        saveTreeNode(siblingPID, siblingNode);
        return true;
    }
};