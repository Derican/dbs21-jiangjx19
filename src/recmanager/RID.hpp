#pragma once

class RID
{
private:
    int pageID, slotID;

public:
    RID() {}
    RID(int _pageID, int _slotID)
    {
        pageID = _pageID;
        slotID = _slotID;
    }
    ~RID() {}
    bool getPageID(int &_pageID) const
    {
        _pageID = pageID;
        return true;
    }
    bool getSlotID(int &_slotID) const
    {
        _slotID = slotID;
        return true;
    }
    bool setPageID(const int _pageID)
    {
        pageID = _pageID;
        return true;
    }
    bool setSlotID(const int _slotID)
    {
        slotID = _slotID;
        return true;
    }
    bool valid()
    {
        return pageID >= 0 && slotID >= 0;
    }
    bool equals(const RID &r)
    {
        return pageID == r.pageID && slotID == r.slotID;
    }
};
