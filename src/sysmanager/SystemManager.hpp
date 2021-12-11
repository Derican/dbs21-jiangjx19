#pragma once

#include "constants.h"
#include <string>
#include <vector>
#include <stdlib.h>
#include "../recmanager/RecordManager.hpp"
#include "../recmanager/FileScan.hpp"
#include "../ixmanager/IndexManager.hpp"

inline bool file_exists(const std::string &name)
{
    return (access(name.c_str(), F_OK) != -1);
}

class SystemManager
{
private:
    RecordManager *rm;
    IndexManager *im;
    FileHandle dbCatHandle, relCatHandle, attrCatHandle;

public:
    bool dbOpened = false;
    std::string openedDbName;
    SystemManager() {}
    SystemManager(RecordManager *_rm, IndexManager *_im)
    {
        rm = _rm;
        im = _im;
        // check dbcat
        if (!file_exists("dbcat"))
        {
            rm->CreateFile("dbcat", DBNAME_MAX_BYTES);
        }
    }
    ~SystemManager()
    {
        rm = nullptr;
        im = nullptr;
    }

    bool createDb(const std::string dbName)
    {
        // check validation
        char value[DBNAME_MAX_BYTES] = "\0";
        strcpy(value, dbName.c_str());
        rm->OpenFile("dbcat", dbCatHandle);
        FileScan fs;
        fs.openScan(dbCatHandle, AttrType::VARCHAR, DBNAME_MAX_BYTES, 0, CompOp::E, value);
        Record rec;
        while (fs.getNextRec(rec))
        {
            std::cout << "Database " << dbName << " already exists." << std::endl;
            return false;
        }
        fs.closeScan();
        RID rid;
        dbCatHandle.insertRec(value, rid);
        rm->CloseFile("dbcat");

        std::string prefix = "mkdir ";
        system((prefix + dbName).c_str());
        rm->CreateFile(dbName + "/relcat", sizeof(RelCat));
        rm->CreateFile(dbName + "/attrcat", sizeof(AttrCat));
        return true;
    }
    bool destroyDb(const std::string dbName)
    {
        char value[DBNAME_MAX_BYTES] = "\0";
        strcpy(value, dbName.c_str());
        rm->OpenFile("dbcat", dbCatHandle);
        FileScan fs;
        fs.openScan(dbCatHandle, AttrType::VARCHAR, DBNAME_MAX_BYTES, 0, CompOp::E, value);
        Record rec;
        RID rid(-1, -1);
        while (fs.getNextRec(rec))
        {
            rec.getRID(rid);
            break;
        }
        fs.closeScan();
        if (rid.valid())
            dbCatHandle.deleteRec(rid);
        rm->CloseFile("dbcat");

        if (rid.valid())
        {
            std::string prefix = "rm -r ";
            system((prefix + dbName).c_str());
            return true;
        }
        else
        {
            std::cout << "Database " << dbName << " not exists." << std::endl;
            return false;
        }
    }
    bool openDb(const std::string dbName)
    {
        // check validation
        char value[DBNAME_MAX_BYTES] = "\0";
        strcpy(value, dbName.c_str());
        rm->OpenFile("dbcat", dbCatHandle);
        FileScan fs;
        fs.openScan(dbCatHandle, AttrType::VARCHAR, DBNAME_MAX_BYTES, 0, CompOp::E, value);
        Record rec;
        RID rid(-1, -1);
        while (fs.getNextRec(rec))
        {
            rec.getRID(rid);
            break;
        }
        fs.closeScan();
        rm->CloseFile("dbcat");

        if (!rid.valid())
        {
            std::cout << "Database " << dbName << " not exists." << std::endl;
            return false;
        }

        dbOpened = true;
        openedDbName = dbName;
        return true;
    }
    bool closeDb()
    {
        if (!dbOpened)
            return false;
        dbOpened = false;
        return true;
    }
    bool createTable(const std::string tableName, const std::vector<AttrInfo> &attributes)
    {
        if (!dbOpened)
        {
            std::cout << "No database used." << std::endl;
            return false;
        }

        FileScan scan;
        Record rec;
        char value[RELNAME_MAX_BYTES] = "\0";
        strcpy(value, tableName.c_str());
        rm->OpenFile(openedDbName + "/relcat", relCatHandle);
        scan.openScan(relCatHandle, AttrType::VARCHAR, RELNAME_MAX_BYTES, offsetof(RelCat, RelCat::relName), CompOp::E, value);
        RID rid(-1, -1);
        while (scan.getNextRec(rec))
        {
            rec.getRID(rid);
        }
        scan.closeScan();
        rm->CloseFile(openedDbName + "/relcat");

        if (rid.valid())
        {
            std::cout << "Table " << tableName << " already exists." << std::endl;
            return false;
        }

        if (attributes.size() <= 0)
        {
            std::cout << "Table must contain at least one column." << std::endl;
            return false;
        }
        int offset = (attributes.size() - 1) / 8 + 1;
        AttrCat attrCat;
        strcpy(attrCat.relName, tableName.c_str());
        rm->OpenFile(openedDbName + "/attrcat", attrCatHandle);
        for (auto info : attributes)
        {
            attrCat.type = info.attrType;
            attrCat.typeLen = info.attrLength;
            attrCat.offset = offset;
            strcpy(attrCat.attrName, info.attrName.c_str());
            attrCat.nullable = info.nullable;
            attrCat.defaultValid = info.defaultValid;
            attrCat.defaultVal = info.defVal;
            offset += info.attrLength;
            attrCatHandle.insertRec(reinterpret_cast<DataType>(&attrCat), rid);
        }
        rm->CloseFile(openedDbName + "/attrcat");

        rm->CreateFile(openedDbName + "/" + tableName, offset);

        RelCat relCat;
        strcpy(relCat.relName, tableName.c_str());
        relCat.tupleLength = offset;
        relCat.attrCount = attributes.size();
        relCat.indexCount = 0;
        rm->OpenFile(openedDbName + "/relcat", relCatHandle);
        relCatHandle.insertRec(reinterpret_cast<DataType>(&relCat), rid);
        rm->CloseFile(openedDbName + "/relcat");

        rm->CreateFile(openedDbName + "/" + tableName + ".index", 4 + 4 * relCat.attrCount);
        return true;
    }
    bool dropTable(const std::string tableName)
    {
        if (!dbOpened)
        {
            std::cout << "No database used." << std::endl;
            return false;
        }

        // check relcat
        FileScan scan;
        Record rec;
        char value[RELNAME_MAX_BYTES] = "\0";
        strcpy(value, tableName.c_str());
        rm->OpenFile(openedDbName + "/relcat", relCatHandle);
        scan.openScan(relCatHandle, AttrType::VARCHAR, RELNAME_MAX_BYTES, offsetof(RelCat, RelCat::relName), CompOp::E, value);
        RID rid(-1, -1);
        RID relCatRID;
        std::vector<RID> attrCatRIDs;
        while (scan.getNextRec(rec))
        {
            rm->DestroyFile(openedDbName + "/" + tableName);
            rec.getRID(rid);
        }
        scan.closeScan();
        rm->CloseFile(openedDbName + "/relcat");

        if (!rid.valid())
        {
            std::cout << "Table " << tableName << " not exists." << std::endl;
            return false;
        }

        relCatRID = rid;

        // check attrcat
        std::vector<std::string> attributes;
        std::vector<int> offsets;
        rm->OpenFile(openedDbName + "/attrcat", attrCatHandle);
        scan.openScan(attrCatHandle, AttrType::VARCHAR, 100, 0, CompOp::E, value);
        while (scan.getNextRec(rec))
        {
            DataType tmp;
            rec.getData(tmp);
            AttrCat *cat = reinterpret_cast<AttrCat *>(tmp);
            attributes.push_back(cat->attrName);
            offsets.push_back(cat->offset);
            RID rid;
            rec.getRID(rid);
            attrCatRIDs.push_back(rid);
        }
        scan.closeScan();
        rm->CloseFile(openedDbName + "/attrcat");

        // check indexes
        FileHandle hd;
        std::vector<std::vector<std::string>> attrNames;
        rm->OpenFile(openedDbName + "/" + tableName + ".index", hd);
        scan.openScan(hd, AttrType::ANY, 4, 0, CompOp::NO, nullptr);
        while (scan.getNextRec(rec))
        {
            DataType tmp;
            rec.getData(tmp);
            int *vec = reinterpret_cast<int *>(tmp + 4);
            int i = 0;
            std::vector<string> attrName;
            while (vec[i] >= 0 && i < attributes.size())
            {
                auto it = std::find(offsets.begin(), offsets.end(), vec[i]);
                auto idx = std::distance(offsets.begin(), it);
                attrName.push_back(attributes[idx]);
                i++;
            }
            attrNames.push_back(attrName);
        }
        scan.closeScan();
        rm->CloseFile(openedDbName + "/" + tableName + ".index");

        // drop indexes
        for (auto attrName : attrNames)
            dropIndex(tableName, attrName);
        rm->DestroyFile(openedDbName + "/" + tableName + ".index");

        // delete from attrcat
        rm->OpenFile(openedDbName + "/attrcat", attrCatHandle);
        for (auto ri : attrCatRIDs)
            attrCatHandle.deleteRec(ri);
        rm->CloseFile(openedDbName + "/attrcat");

        // delete from relcat
        rm->OpenFile(openedDbName + "/relcat", relCatHandle);
        relCatHandle.deleteRec(relCatRID);
        rm->CloseFile(openedDbName + "/relcat");
        return true;
    }
    bool createIndex(const std::string tableName, const std::vector<std::string> &attrName, bool isPrimary)
    {
        if (!dbOpened)
        {
            std::cout << "No database used." << std::endl;
            return false;
        }

        FileScan scan;
        Record rec;
        char value[RELNAME_MAX_BYTES] = "\0";
        strcpy(value, tableName.c_str());

        std::vector<std::string> attributes;
        std::vector<int> offsets;
        std::vector<int> indexNo;
        rm->OpenFile(openedDbName + "/attrcat", attrCatHandle);
        scan.openScan(attrCatHandle, AttrType::VARCHAR, RELNAME_MAX_BYTES, offsetof(AttrCat, AttrCat::relName), CompOp::E, value);
        AttrCat *cat = nullptr;
        while (scan.getNextRec(rec))
        {
            DataType tmp;
            rec.getData(tmp);
            cat = reinterpret_cast<AttrCat *>(tmp);
            attributes.push_back(cat->attrName);
            offsets.push_back(cat->offset);
        }
        scan.closeScan();
        rm->CloseFile(openedDbName + "/attrcat");

        if (cat == nullptr)
        {
            std::cout << "Table " << tableName << " not exists." << std::endl;
            return false;
        }
        for (auto name : attrName)
        {
            auto it = std::find(attributes.begin(), attributes.end(), name);
            auto idx = std::distance(attributes.begin(), it);
            if (it == attributes.end())
            {
                std::cout << "Attribute " << name << " not exists." << std::endl;
                return false;
            }
            indexNo.push_back(offsets[idx]);
        }

        int *data = new int[attributes.size() + 1];
        memset(data, 0, 4 * (attributes.size() + 1));
        data[0] = isPrimary ? 1 : 0;
        for (auto i = 0; i < attributes.size(); i++)
        {
            if (i < indexNo.size())
                data[i + 1] = indexNo[i];
            else
                data[i + 1] = -1;
        }
        FileHandle hd;
        rm->OpenFile(openedDbName + "/" + tableName + ".index", hd);
        RID r(-1, -1);
        scan.openScan(hd, AttrType::ANY, 4, 0, CompOp::NO, nullptr);
        bool primaryExisted = false;
        int primary = 1;
        while (scan.getNextRec(rec))
        {
            DataType tmp;
            rec.getData(tmp);
            if (memcmp(data, tmp + 4, 4 * attributes.size()) == 0)
                rec.getRID(r);
            if (memcmp(&primary, tmp, 4) == 0)
                primaryExisted = true;
        }
        scan.closeScan();
        if (isPrimary && primaryExisted)
        {
            std::cout << "Primary Key already exists." << std::endl;
            rm->CloseFile(openedDbName + "/" + tableName + ".index");
            return false;
        }
        if (r.valid())
        {
            std::cout << "Index already exists." << std::endl;
            rm->CloseFile(openedDbName + "/" + tableName + ".index");
            return false;
        }
        hd.insertRec(reinterpret_cast<DataType>(data), r);
        rm->CloseFile(openedDbName + "/" + tableName + ".index");
        delete[] data;

        // here we open two files at one time
        im->createIndex(openedDbName + "/" + tableName, indexNo, AttrType::INT, 4 * indexNo.size());
        IndexHandle ih;
        im->openIndex(openedDbName + "/" + tableName, indexNo, ih);
        FileHandle fh;
        rm->OpenFile(openedDbName + "/" + tableName, fh);
        scan.openScan(fh, cat->type, cat->typeLen, cat->offset, CompOp::NO, nullptr);
        while (scan.getNextRec(rec))
        {
            DataType tmp;
            rec.getData(tmp);
            RID rid;
            rec.getRID(rid);
            std::vector<int> key;
            for (auto offset : offsets)
            {
                int *i = reinterpret_cast<int *>(tmp + offset);
                key.push_back(*i);
            }
            ih.insertEntry(key, rid);
        }
        scan.closeScan();
        rm->CloseFile(openedDbName + "/" + tableName);
        im->closeIndex(openedDbName + "/" + tableName, indexNo);
        return true;
    }
    bool dropIndex(const std::string tableName, const std::vector<std::string> &attrName)
    {
        if (!dbOpened)
        {
            std::cout << "No database used." << std::endl;
            return false;
        }

        FileScan scan;
        Record rec;
        char value[RELNAME_MAX_BYTES] = "\0";
        strcpy(value, tableName.c_str());

        std::vector<std::string> attributes;
        std::vector<int> offsets;
        std::vector<int> indexNo;
        rm->OpenFile(openedDbName + "/attrcat", attrCatHandle);
        scan.openScan(attrCatHandle, AttrType::VARCHAR, RELNAME_MAX_BYTES, offsetof(AttrCat, AttrCat::relName), CompOp::E, value);
        AttrCat *cat = nullptr;
        while (scan.getNextRec(rec))
        {
            DataType tmp;
            rec.getData(tmp);
            cat = reinterpret_cast<AttrCat *>(tmp);
            attributes.push_back(cat->attrName);
            offsets.push_back(cat->offset);
        }
        scan.closeScan();
        rm->CloseFile(openedDbName + "/attrcat");

        if (cat == nullptr)
        {
            std::cout << "Table " << tableName << " not exists." << std::endl;
            return false;
        }
        for (auto name : attrName)
        {
            auto it = std::find(attributes.begin(), attributes.end(), name);
            auto idx = std::distance(attributes.begin(), it);
            if (it == attributes.end())
            {
                std::cout << "Attribute " << name << " not exists." << std::endl;
                return false;
            }
            indexNo.push_back(offsets[idx]);
        }

        int *data = new int[attributes.size()];
        memset(data, 0, 4 * attributes.size());
        for (auto i = 0; i < attributes.size(); i++)
        {
            if (i < indexNo.size())
                data[i] = indexNo[i];
            else
                data[i] = -1;
        }
        FileHandle hd;
        rm->OpenFile(openedDbName + "/" + tableName + ".index", hd);
        RID r(-1, -1);
        scan.openScan(hd, AttrType::ANY, 4, 0, CompOp::NO, nullptr);
        while (scan.getNextRec(rec))
        {
            DataType tmp;
            rec.getData(tmp);
            if (memcmp(data, tmp + 4, 4 * attributes.size()) == 0)
                rec.getRID(r);
        }
        scan.closeScan();
        if (!r.valid())
        {
            std::cout << "Index not exists." << std::endl;
            rm->CloseFile(openedDbName + "/" + tableName + ".index");
            return false;
        }
        hd.deleteRec(r);
        rm->CloseFile(openedDbName + "/" + tableName + ".index");

        delete[] data;
        im->destroyIndex(openedDbName + "/" + tableName, indexNo);
        return true;
    }
    bool getAllAttr(const std::string &tableName, std::vector<std::string> &attrName, std::vector<int> &offsets, std::vector<AttrType> &types, std::vector<int> &typeLens)
    {
        if (!dbOpened)
            return false;
        FileScan scan;
        Record rec;
        char value[ATTRNAME_MAX_BYTES] = "\0";
        strcpy(value, tableName.c_str());

        rm->OpenFile(openedDbName + "/attrcat", attrCatHandle);
        scan.openScan(attrCatHandle, AttrType::VARCHAR, ATTRNAME_MAX_BYTES, offsetof(AttrCat, AttrCat::relName), CompOp::E, value);
        AttrCat *cat = nullptr;
        attrName.clear();
        offsets.clear();
        types.clear();
        typeLens.clear();
        while (scan.getNextRec(rec))
        {
            DataType tmp;
            rec.getData(tmp);
            cat = reinterpret_cast<AttrCat *>(tmp);
            attrName.push_back(std::string(cat->attrName));
            offsets.push_back(cat->offset);
            types.push_back(cat->type);
            typeLens.push_back(cat->typeLen);
        }
        scan.closeScan();
        rm->CloseFile(openedDbName + "/attrcat");
    }
    bool getAllAttr(const std::string &tableName, std::vector<std::string> &attrName, std::vector<int> &offsets, std::vector<AttrType> &types, std::vector<int> &typeLens, std::vector<bool> &nulls, std::vector<bool> &defaultValids, std::vector<defaultValue> &defaults)
    {
        if (!dbOpened)
            return false;
        FileScan scan;
        Record rec;
        char value[ATTRNAME_MAX_BYTES] = "\0";
        strcpy(value, tableName.c_str());

        rm->OpenFile(openedDbName + "/attrcat", attrCatHandle);
        scan.openScan(attrCatHandle, AttrType::VARCHAR, ATTRNAME_MAX_BYTES, offsetof(AttrCat, AttrCat::relName), CompOp::E, value);
        AttrCat *cat = nullptr;
        attrName.clear();
        offsets.clear();
        types.clear();
        typeLens.clear();
        nulls.clear();
        defaultValids.clear();
        defaults.clear();
        while (scan.getNextRec(rec))
        {
            DataType tmp;
            rec.getData(tmp);
            cat = reinterpret_cast<AttrCat *>(tmp);
            attrName.push_back(std::string(cat->attrName));
            offsets.push_back(cat->offset);
            types.push_back(cat->type);
            typeLens.push_back(cat->typeLen);
            nulls.push_back(cat->nullable);
            defaultValids.push_back(cat->defaultValid);
            defaults.push_back(cat->defaultVal);
        }
        scan.closeScan();
        rm->CloseFile(openedDbName + "/attrcat");
    }
    bool getAttrInfo(const std::string &attrName, int &offset, AttrType &type, int &typeLen, std::string &tableName)
    {
        if (!dbOpened)
            return false;
        FileScan scan;
        Record rec;
        char value[100] = "\0";
        strcpy(value, attrName.c_str());

        rm->OpenFile(openedDbName + "/attrcat", attrCatHandle);
        scan.openScan(attrCatHandle, AttrType::VARCHAR, ATTRNAME_MAX_BYTES, offsetof(AttrCat, AttrCat::attrName), CompOp::E, value);
        AttrCat *cat = nullptr;
        bool flag = false;
        while (scan.getNextRec(rec))
        {
            DataType tmp;
            rec.getData(tmp);
            cat = reinterpret_cast<AttrCat *>(tmp);
            if (flag)
                std::cout << "WARNING: duplicate column name in table " << cat->relName << "." << std::endl;
            tableName = cat->relName;
            offset = cat->offset;
            type = cat->type;
            typeLen = cat->typeLen;
            flag = true;
        }
        scan.closeScan();
        rm->CloseFile(openedDbName + "/attrcat");
    }
    bool getAttrInfo(const std::string &tableName, const std::string &attrName, int &offset, AttrType &type, int &typeLen)
    {
        if (!dbOpened)
            return false;
        FileScan scan;
        Record rec;
        char value[RELNAME_MAX_BYTES] = "\0";
        strcpy(value, tableName.c_str());

        rm->OpenFile(openedDbName + "/attrcat", attrCatHandle);
        scan.openScan(attrCatHandle, AttrType::VARCHAR, RELNAME_MAX_BYTES, offsetof(AttrCat, AttrCat::relName), CompOp::E, value);
        AttrCat *cat = nullptr;
        while (scan.getNextRec(rec))
        {
            DataType tmp;
            rec.getData(tmp);
            cat = reinterpret_cast<AttrCat *>(tmp);
            if (strcmp(cat->attrName, attrName.c_str()) == 0)
            {
                offset = cat->offset;
                type = cat->type;
                typeLen = cat->typeLen;
                break;
            }
        }
        scan.closeScan();
        rm->CloseFile(openedDbName + "/attrcat");

        if (cat)
            return true;
        else
        {
            std::cout << "Attribute " << tableName << "." << attrName << " not exists." << std::endl;
            return false;
        }
    }
    bool showAllDb()
    {
        std::cout << "+----------------+" << std::endl
                  << "|   databases    |" << std::endl
                  << "+----------------+" << std::endl;
        rm->OpenFile("dbcat", dbCatHandle);
        FileScan fs;
        fs.openScan(dbCatHandle, AttrType::VARCHAR, DBNAME_MAX_BYTES, 0, CompOp::NO, nullptr);
        Record rec;
        int count = 0;
        while (fs.getNextRec(rec))
        {
            DataType tmp;
            rec.getData(tmp);
            std::cout << "|" << std::left << setw(16) << tmp << "|" << std::endl;
            count++;
        }
        std::cout << "+----------------+" << std::endl;
        std::cout << count << " database(s) stored." << std::endl;
        fs.closeScan();
        rm->CloseFile("dbcat");
        return true;
    }
    bool showAllTables()
    {
        if (!dbOpened)
        {
            std::cout << "No database used." << std::endl;
            return false;
        }
        std::cout << "+----------------+" << std::endl
                  << "|     tables     |" << std::endl
                  << "+----------------+" << std::endl;
        rm->OpenFile(openedDbName + "/relcat", relCatHandle);
        FileScan fs;
        fs.openScan(relCatHandle, AttrType::VARCHAR, RELNAME_MAX_BYTES, 0, CompOp::NO, nullptr);
        Record rec;
        int count = 0;
        while (fs.getNextRec(rec))
        {
            DataType tmp;
            rec.getData(tmp);
            std::cout << "|" << std::left << setw(16) << tmp << "|" << std::endl;
            count++;
        }
        std::cout << "+----------------+" << std::endl;
        std::cout << count << " table(s) stored." << std::endl;
        fs.closeScan();
        rm->CloseFile(openedDbName + "/relcat");
        return true;
    }
    bool descTable(std::string tableName)
    {
        if (!dbOpened)
        {
            std::cout << "No database used." << std::endl;
            return false;
        }

        FileScan fs;
        Record rec;
        char value[RELNAME_MAX_BYTES] = "\0";
        strcpy(value, tableName.c_str());
        rm->OpenFile(openedDbName + "/relcat", relCatHandle);
        fs.openScan(relCatHandle, AttrType::VARCHAR, RELNAME_MAX_BYTES, offsetof(RelCat, RelCat::relName), CompOp::E, value);
        RID rid(-1, -1);
        while (fs.getNextRec(rec))
        {
            rec.getRID(rid);
        }
        fs.closeScan();
        rm->CloseFile(openedDbName + "/relcat");

        if (!rid.valid())
        {
            std::cout << "Table " << tableName << " not exists." << std::endl;
            return false;
        }

        std::vector<std::string> attributes;
        std::vector<int> offsets;
        std::cout << "+---------------+--------------+------+---------+" << std::endl
                  << "|     Field     |     Type     | Null | Default |" << std::endl
                  << "+---------------+--------------+------+---------+" << std::endl;
        rm->OpenFile(openedDbName + "/attrcat", attrCatHandle);
        fs.openScan(attrCatHandle, AttrType::VARCHAR, RELNAME_MAX_BYTES, offsetof(AttrCat, AttrCat::relName), CompOp::E, value);
        AttrCat attrcat;
        while (fs.getNextRec(rec))
        {
            DataType tmp;
            rec.getData(tmp);
            memcpy(&attrcat, tmp, sizeof(AttrCat));
            std::cout << attrcat << std::endl;
            attributes.push_back(attrcat.attrName);
            offsets.push_back(attrcat.offset);
        }
        std::cout << "+---------------+--------------+------+---------+" << std::endl;
        fs.closeScan();
        rm->CloseFile(openedDbName + "/attrcat");

        int *indexNo = new int[attributes.size()];
        FileHandle hd;
        rm->OpenFile(openedDbName + "/" + tableName + ".index", hd);
        int primary = 1;
        fs.openScan(hd, AttrType::INT, 4, 0, CompOp::E, &primary);
        while (fs.getNextRec(rec))
        {
            std::cout << "PRIMARY KEY (";
            DataType tmp;
            rec.getData(tmp);
            memcpy(indexNo, tmp + 4, 4 * attributes.size());
            int i = 0;
            while (indexNo[i] >= 0 && i < attributes.size())
            {
                if (i > 0)
                    std::cout << ", ";
                auto it = std::find(offsets.begin(), offsets.end(), indexNo[i]);
                auto idx = std::distance(offsets.begin(), it);
                std::cout << attributes[idx];
                i++;
            }
            std::cout << ")" << std::endl;
        }
        fs.closeScan();

        primary = 0;
        fs.openScan(hd, AttrType::INT, 4, 0, CompOp::E, &primary);
        while (fs.getNextRec(rec))
        {
            std::cout << "INDEX (";
            DataType tmp;
            rec.getData(tmp);
            memcpy(indexNo, tmp + 4, 4 * attributes.size());
            int i = 0;
            while (indexNo[i] >= 0 && i < attributes.size())
            {
                if (i > 0)
                    std::cout << ", ";
                auto it = std::find(offsets.begin(), offsets.end(), indexNo[i]);
                auto idx = std::distance(offsets.begin(), it);
                std::cout << attributes[i];
                i++;
            }
            std::cout << ")" << std::endl;
        }
        fs.closeScan();
        rm->CloseFile(openedDbName + "/" + tableName + ".index");
        return true;
    }
    bool checkTableExists(const std::string &tableName)
    {
        if (!dbOpened)
            return false;
        FileScan scan;
        Record rec;
        char value[RELNAME_MAX_BYTES] = "\0";
        strcpy(value, tableName.c_str());
        rm->OpenFile(openedDbName + "/relcat", relCatHandle);
        scan.openScan(relCatHandle, AttrType::VARCHAR, RELNAME_MAX_BYTES, offsetof(RelCat, RelCat::relName), CompOp::E, value);
        RID rid(-1, -1);
        while (scan.getNextRec(rec))
        {
            scan.closeScan();
            rm->CloseFile(openedDbName + "/relcat");
            return true;
        }
        scan.closeScan();
        rm->CloseFile(openedDbName + "/relcat");
        return false;
    }
    bool getPrimaryKey(const std::string &tableName, std::vector<int> &indexNo)
    {
        indexNo.clear();
        int attrCount;

        if (!dbOpened)
            return false;
        FileScan scan;
        Record rec;
        char value[RELNAME_MAX_BYTES] = "\0";
        strcpy(value, tableName.c_str());
        rm->OpenFile(openedDbName + "/relcat", relCatHandle);
        scan.openScan(relCatHandle, AttrType::VARCHAR, RELNAME_MAX_BYTES, offsetof(RelCat, RelCat::relName), CompOp::E, value);
        RID rid(-1, -1);
        while (scan.getNextRec(rec))
        {
            rec.getRID(rid);
            DataType tmp;
            rec.getData(tmp);
            RelCat cat;
            memcpy(&cat, tmp, sizeof(RelCat));
            attrCount = cat.attrCount;
        }
        scan.closeScan();
        rm->CloseFile(openedDbName + "/relcat");

        if (!rid.valid())
            return false;

        FileHandle hd;
        rm->OpenFile(openedDbName + "/" + tableName + ".index", hd);
        int primary = 1;
        scan.openScan(hd, AttrType::INT, 4, 0, CompOp::E, &primary);
        while (scan.getNextRec(rec))
        {
            DataType tmp;
            rec.getData(tmp);
            int *ind = new int[attrCount];
            memcpy(ind, tmp + 4, 4 * attrCount);
            for (auto i = 0; i < attrCount; i++)
            {
                if (ind[i] >= 0)
                    indexNo.push_back(ind[i]);
                else
                    break;
            }
            delete[] ind;
        }
        scan.closeScan();
        rm->CloseFile(openedDbName + "/" + tableName + ".index");
        return true;
    }
    bool getAllIndex(const std::string &tableName, std::vector<std::vector<int>> &indexNo)
    {
        indexNo.clear();
        int attrCount;

        if (!dbOpened)
            return false;
        FileScan scan;
        Record rec;
        char value[RELNAME_MAX_BYTES] = "\0";
        strcpy(value, tableName.c_str());
        rm->OpenFile(openedDbName + "/relcat", relCatHandle);
        scan.openScan(relCatHandle, AttrType::VARCHAR, RELNAME_MAX_BYTES, offsetof(RelCat, RelCat::relName), CompOp::E, value);
        RID rid(-1, -1);
        while (scan.getNextRec(rec))
        {
            rec.getRID(rid);
            DataType tmp;
            rec.getData(tmp);
            RelCat cat;
            memcpy(&cat, tmp, sizeof(RelCat));
            attrCount = cat.attrCount;
        }
        scan.closeScan();
        rm->CloseFile(openedDbName + "/relcat");

        if (!rid.valid())
            return false;

        FileHandle hd;
        rm->OpenFile(openedDbName + "/" + tableName + ".index", hd);
        int primary = 0;
        scan.openScan(hd, AttrType::INT, 4, 0, CompOp::E, &primary);
        while (scan.getNextRec(rec))
        {
            DataType tmp;
            rec.getData(tmp);
            int *ind = new int[attrCount];
            memcpy(ind, tmp + 4, 4 * attrCount);
            std::vector<int> inde;
            for (auto i = 0; i < attrCount; i++)
            {
                if (ind[i] >= 0)
                    inde.push_back(ind[i]);
                else
                    break;
            }
            indexNo.push_back(inde);
            delete[] ind;
        }
        scan.closeScan();
        rm->CloseFile(openedDbName + "/" + tableName + ".index");
        return true;
    }
    bool getPrimaryKeyAndIndex(const std::string &tableName, std::vector<std::vector<int>> &indexNo)
    {
        indexNo.clear();
        int attrCount;

        if (!dbOpened)
            return false;
        FileScan scan;
        Record rec;
        char value[RELNAME_MAX_BYTES] = "\0";
        strcpy(value, tableName.c_str());
        rm->OpenFile(openedDbName + "/relcat", relCatHandle);
        scan.openScan(relCatHandle, AttrType::VARCHAR, RELNAME_MAX_BYTES, offsetof(RelCat, RelCat::relName), CompOp::E, value);
        RID rid(-1, -1);
        while (scan.getNextRec(rec))
        {
            rec.getRID(rid);
            DataType tmp;
            rec.getData(tmp);
            RelCat cat;
            memcpy(&cat, tmp, sizeof(RelCat));
            attrCount = cat.attrCount;
        }
        scan.closeScan();
        rm->CloseFile(openedDbName + "/relcat");

        if (!rid.valid())
            return false;

        FileHandle hd;
        rm->OpenFile(openedDbName + "/" + tableName + ".index", hd);
        scan.openScan(hd, AttrType::INT, 4, 0, CompOp::NO, nullptr);
        while (scan.getNextRec(rec))
        {
            DataType tmp;
            rec.getData(tmp);
            int *ind = new int[attrCount];
            memcpy(ind, tmp + 4, 4 * attrCount);
            std::vector<int> inde;
            for (auto i = 0; i < attrCount; i++)
            {
                if (ind[i] >= 0)
                    inde.push_back(ind[i]);
                else
                    break;
            }
            indexNo.push_back(inde);
            delete[] ind;
        }
        scan.closeScan();
        rm->CloseFile(openedDbName + "/" + tableName + ".index");
        return true;
    }
    bool dropPrimaryKey(const std::string tableName)
    {
        if (!dbOpened)
        {
            std::cout << "No database used." << std::endl;
            return false;
        }

        FileScan scan;
        Record rec;
        char value[RELNAME_MAX_BYTES] = "\0";
        strcpy(value, tableName.c_str());

        std::vector<std::string> attributes;
        std::vector<int> offsets;
        std::vector<int> indexNo;
        rm->OpenFile(openedDbName + "/attrcat", attrCatHandle);
        scan.openScan(attrCatHandle, AttrType::VARCHAR, RELNAME_MAX_BYTES, offsetof(AttrCat, AttrCat::relName), CompOp::E, value);
        AttrCat *cat = nullptr;
        while (scan.getNextRec(rec))
        {
            DataType tmp;
            rec.getData(tmp);
            cat = reinterpret_cast<AttrCat *>(tmp);
            attributes.push_back(cat->attrName);
            offsets.push_back(cat->offset);
        }
        scan.closeScan();
        rm->CloseFile(openedDbName + "/attrcat");

        if (cat == nullptr)
        {
            std::cout << "Table " << tableName << " not exists." << std::endl;
            return false;
        }

        int *data = new int[attributes.size()];
        memset(data, 0, 4 * attributes.size());
        FileHandle hd;
        rm->OpenFile(openedDbName + "/" + tableName + ".index", hd);
        RID r(-1, -1);
        int primary = 1;
        scan.openScan(hd, AttrType::INT, 4, 0, CompOp::E, &primary);
        while (scan.getNextRec(rec))
        {
            DataType tmp;
            rec.getData(tmp);
            memcpy(data, tmp + 4, 4 * attributes.size());
            rec.getRID(r);
        }
        scan.closeScan();
        if (!r.valid())
        {
            std::cout << "Primary Key not exists." << std::endl;
            rm->CloseFile(openedDbName + "/" + tableName + ".index");
            return false;
        }
        hd.deleteRec(r);
        rm->CloseFile(openedDbName + "/" + tableName + ".index");

        for (int i = 0; i < attributes.size() && data[i] >= 0; i++)
            indexNo.push_back(data[i]);
        delete[] data;
        im->destroyIndex(openedDbName + "/" + tableName, indexNo);
        return true;
    }
};