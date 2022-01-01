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
            rm->createFile("dbcat", DBNAME_MAX_BYTES);
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
        rm->openFile("dbcat", dbCatHandle);
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
        rm->closeFile("dbcat");

        std::string prefix = "mkdir ";
        system((prefix + dbName).c_str());
        rm->createFile(dbName + "/relcat", sizeof(RelCat));
        rm->createFile(dbName + "/attrcat", sizeof(AttrCat));
        return true;
    }
    bool destroyDb(const std::string dbName)
    {
        char value[DBNAME_MAX_BYTES] = "\0";
        strcpy(value, dbName.c_str());
        rm->openFile("dbcat", dbCatHandle);
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
        rm->closeFile("dbcat");

        if (rid.valid())
        {
            std::string prefix = "rm -r ";
            system((prefix + dbName).c_str());

            if (dbOpened && dbName == openedDbName)
                closeDb();
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
        rm->openFile("dbcat", dbCatHandle);
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
        rm->closeFile("dbcat");

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
        rm->openFile(openedDbName + "/relcat", relCatHandle);
        scan.openScan(relCatHandle, AttrType::VARCHAR, RELNAME_MAX_BYTES, offsetof(RelCat, RelCat::relName), CompOp::E, value);
        RID rid(-1, -1);
        while (scan.getNextRec(rec))
        {
            rec.getRID(rid);
        }
        scan.closeScan();
        rm->closeFile(openedDbName + "/relcat");

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
        rm->openFile(openedDbName + "/attrcat", attrCatHandle);
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
        rm->closeFile(openedDbName + "/attrcat");

        rm->createFile(openedDbName + "/" + tableName, offset);

        RelCat relCat;
        strcpy(relCat.relName, tableName.c_str());
        relCat.tupleLength = offset;
        relCat.attrCount = attributes.size();
        relCat.indexCount = 0;
        rm->openFile(openedDbName + "/relcat", relCatHandle);
        relCatHandle.insertRec(reinterpret_cast<DataType>(&relCat), rid);
        rm->closeFile(openedDbName + "/relcat");

        rm->createFile(openedDbName + "/" + tableName + ".index", 4 + 4 * relCat.attrCount + IXNAMECHAR_MAX_BYTES);
        rm->createFile(openedDbName + "/" + tableName + ".unique", 4 * relCat.attrCount);
        rm->createFile(openedDbName + "/" + tableName + ".foreign", RELNAME_MAX_BYTES);
        rm->createFile(openedDbName + "/" + tableName + ".ref", RELNAME_MAX_BYTES);
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
        rm->openFile(openedDbName + "/relcat", relCatHandle);
        scan.openScan(relCatHandle, AttrType::VARCHAR, RELNAME_MAX_BYTES, offsetof(RelCat, RelCat::relName), CompOp::E, value);
        RID rid(-1, -1);
        RID relCatRID;
        std::vector<RID> attrCatRIDs;
        while (scan.getNextRec(rec))
        {
            rm->destroyFile(openedDbName + "/" + tableName);
            rec.getRID(rid);
        }
        scan.closeScan();
        rm->closeFile(openedDbName + "/relcat");

        if (!rid.valid())
        {
            std::cout << "Table " << tableName << " not exists." << std::endl;
            return false;
        }

        relCatRID = rid;

        // check attrcat
        std::vector<std::string> attributes;
        std::vector<int> offsets;
        rm->openFile(openedDbName + "/attrcat", attrCatHandle);
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
        rm->closeFile(openedDbName + "/attrcat");

        // check indexes
        FileHandle hd;
        std::vector<std::vector<std::string>> attrNames;
        rm->openFile(openedDbName + "/" + tableName + ".index", hd);
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
        rm->closeFile(openedDbName + "/" + tableName + ".index");

        // drop unique
        rm->destroyFile(openedDbName + "/" + tableName + ".unique");

        // drop foreign
        rm->openFile(openedDbName + "/" + tableName + ".foreign", hd);
        scan.openScan(hd, AttrType::VARCHAR, -1, -1, CompOp::NO, nullptr);
        std::vector<std::string> refTables;
        while (scan.getNextRec(rec))
        {
            DataType tmp;
            rec.getData(tmp);
            refTables.push_back(tmp);
        }
        scan.closeScan();
        rm->closeFile(openedDbName + "/" + tableName + ".foreign");
        for (auto reft : refTables)
        {
            rm->openFile(openedDbName + "/" + reft + ".ref", hd);
            scan.openScan(hd, AttrType::VARCHAR, RELNAME_MAX_BYTES, 0, CompOp::E, value);
            while (scan.getNextRec(rec))
            {
                rec.getRID(rid);
                hd.deleteRec(rid);
            }
            scan.closeScan();
            rm->closeFile(openedDbName + "/" + reft + ".ref");
            rm->destroyFile(openedDbName + "/" + tableName + "." + reft);
        }
        rm->destroyFile(openedDbName + "/" + tableName + ".foreign");

        // drop ref
        rm->openFile(openedDbName + "/" + tableName + ".ref", hd);
        scan.openScan(hd, AttrType::VARCHAR, -1, -1, CompOp::NO, nullptr);
        std::vector<std::string> reffedTables;
        while (scan.getNextRec(rec))
        {
            DataType tmp;
            rec.getData(tmp);
            reffedTables.push_back(tmp);
        }
        scan.closeScan();
        rm->closeFile(openedDbName + "/" + tableName + ".ref");
        for (auto reft : reffedTables)
        {
            dropTable(reft);
        }
        rm->destroyFile(openedDbName + "/" + tableName + ".ref");

        // drop indexes
        for (auto attrName : attrNames)
            dropIndex(tableName, attrName);
        rm->destroyFile(openedDbName + "/" + tableName + ".index");

        // delete from attrcat
        rm->openFile(openedDbName + "/attrcat", attrCatHandle);
        for (auto ri : attrCatRIDs)
            attrCatHandle.deleteRec(ri);
        rm->closeFile(openedDbName + "/attrcat");

        // delete from relcat
        rm->openFile(openedDbName + "/relcat", relCatHandle);
        relCatHandle.deleteRec(relCatRID);
        rm->closeFile(openedDbName + "/relcat");
        return true;
    }
    bool createIndex(const std::string tableName, const std::vector<std::string> &attrName, bool isPrimary, std::string indexName = "")
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
        rm->openFile(openedDbName + "/attrcat", attrCatHandle);
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
        rm->closeFile(openedDbName + "/attrcat");

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

        // check if already exists
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
        rm->openFile(openedDbName + "/" + tableName + ".index", hd);
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
            rm->closeFile(openedDbName + "/" + tableName + ".index");
            return false;
        }
        if (r.valid())
        {
            std::cout << "Index already exists." << std::endl;
            rm->closeFile(openedDbName + "/" + tableName + ".index");
            return false;
        }
        rm->closeFile(openedDbName + "/" + tableName + ".index");

        // check unique for primary
        if (isPrimary && !checkUnique(tableName, attrName))
        {
            std::cout << "Primary key not created due to duplicated value." << std::endl;
            return false;
        }

        // insert to .index
        rm->openFile(openedDbName + "/" + tableName + ".index", hd);
        char *data_ext = new char[4 + 4 * attributes.size() + IXNAMECHAR_MAX_BYTES];
        memcpy(data_ext, data, 4 + 4 * attributes.size());
        strcpy(data_ext + 4 + 4 * attributes.size(), indexName.c_str());
        hd.insertRec(data_ext, r);
        rm->closeFile(openedDbName + "/" + tableName + ".index");
        delete[] data;

        im->createIndex(openedDbName + "/" + tableName, indexNo, AttrType::INT, 4 * indexNo.size());
        IndexHandle ih;
        im->openIndex(openedDbName + "/" + tableName, indexNo, ih);
        FileHandle fh;
        rm->openFile(openedDbName + "/" + tableName, fh);
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
        rm->closeFile(openedDbName + "/" + tableName);
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
        rm->openFile(openedDbName + "/attrcat", attrCatHandle);
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
        rm->closeFile(openedDbName + "/attrcat");

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
        rm->openFile(openedDbName + "/" + tableName + ".index", hd);
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
            rm->closeFile(openedDbName + "/" + tableName + ".index");
            return false;
        }
        hd.deleteRec(r);
        rm->closeFile(openedDbName + "/" + tableName + ".index");

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

        rm->openFile(openedDbName + "/attrcat", attrCatHandle);
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
        rm->closeFile(openedDbName + "/attrcat");
        return true;
    }
    bool getAllAttr(const std::string &tableName, std::vector<std::string> &attrName, std::vector<int> &offsets, std::vector<AttrType> &types, std::vector<int> &typeLens, std::vector<bool> &nulls, std::vector<bool> &defaultValids, std::vector<defaultValue> &defaults)
    {
        if (!dbOpened)
            return false;
        FileScan scan;
        Record rec;
        char value[ATTRNAME_MAX_BYTES] = "\0";
        strcpy(value, tableName.c_str());

        // check attrcat
        rm->openFile(openedDbName + "/attrcat", attrCatHandle);
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
        rm->closeFile(openedDbName + "/attrcat");

        // check primary key
        std::vector<int> primaryKey;
        getPrimaryKey(tableName, primaryKey);
        for (auto key : primaryKey)
        {
            auto it = std::find(offsets.begin(), offsets.end(), key);
            auto idx = std::distance(offsets.begin(), it);
            nulls[idx] = false;
        }

        return true;
    }
    bool getAttrInfo(const std::string &attrName, int &offset, AttrType &type, int &typeLen, std::string &tableName)
    {
        if (!dbOpened)
            return false;
        FileScan scan;
        Record rec;
        char value[100] = "\0";
        strcpy(value, attrName.c_str());

        rm->openFile(openedDbName + "/attrcat", attrCatHandle);
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
        rm->closeFile(openedDbName + "/attrcat");
    }
    bool getAttrInfo(const std::string &tableName, const std::string &attrName, int &offset, AttrType &type, int &typeLen)
    {
        if (!dbOpened)
            return false;
        FileScan scan;
        Record rec;
        char value[RELNAME_MAX_BYTES] = "\0";
        strcpy(value, tableName.c_str());

        rm->openFile(openedDbName + "/attrcat", attrCatHandle);
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
        rm->closeFile(openedDbName + "/attrcat");

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
        rm->openFile("dbcat", dbCatHandle);
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
        rm->closeFile("dbcat");
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
        rm->openFile(openedDbName + "/relcat", relCatHandle);
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
        rm->closeFile(openedDbName + "/relcat");
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
        rm->openFile(openedDbName + "/relcat", relCatHandle);
        fs.openScan(relCatHandle, AttrType::VARCHAR, RELNAME_MAX_BYTES, offsetof(RelCat, RelCat::relName), CompOp::E, value);
        RID rid(-1, -1);
        while (fs.getNextRec(rec))
        {
            rec.getRID(rid);
        }
        fs.closeScan();
        rm->closeFile(openedDbName + "/relcat");

        if (!rid.valid())
        {
            std::cout << "Table " << tableName << " not exists." << std::endl;
            return false;
        }

        // attributes
        std::vector<std::string> attributes;
        std::vector<int> offsets;
        std::cout << "+---------------+--------------+------+---------+" << std::endl
                  << "|     Field     |     Type     | Null | Default |" << std::endl
                  << "+---------------+--------------+------+---------+" << std::endl;
        rm->openFile(openedDbName + "/attrcat", attrCatHandle);
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
        rm->closeFile(openedDbName + "/attrcat");

        // primary key
        int *indexNo = new int[attributes.size()];
        FileHandle hd;
        rm->openFile(openedDbName + "/" + tableName + ".index", hd);
        int primary = 1;
        fs.openScan(hd, AttrType::INT, 4, 0, CompOp::E, &primary);
        while (fs.getNextRec(rec))
        {
            DataType tmp;
            rec.getData(tmp);
            std::string indexName(tmp + 4 + 4 * attributes.size());
            std::cout << "PRIMARY KEY " << indexName << " (";
            memcpy(indexNo, tmp + 4, 4 * attributes.size());
            size_t i = 0;
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

        // index
        primary = 0;
        fs.openScan(hd, AttrType::INT, 4, 0, CompOp::E, &primary);
        while (fs.getNextRec(rec))
        {
            DataType tmp;
            rec.getData(tmp);
            std::string indexName(tmp + 4 + 4 * attributes.size());
            std::cout << "INDEX " << indexName << " (";
            memcpy(indexNo, tmp + 4, 4 * attributes.size());
            size_t i = 0;
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
        rm->closeFile(openedDbName + "/" + tableName + ".index");

        // foreign key
        std::vector<std::string> refTableName, foreignNames;
        std::vector<std::vector<int>> indexNos, refIndexNo;
        getForeign(tableName, refTableName, indexNos, refIndexNo, foreignNames);
        for (size_t i = 0; i < refTableName.size(); i++)
        {
            std::vector<std::string> allAttrName, allRefAttrName;
            std::vector<int> allOffsets, allRefOffsets;
            std::vector<AttrType> allTypes, allRefTypes;
            std::vector<int> allTypeLens, allRefTypeLens;
            getAllAttr(tableName, allAttrName, allOffsets, allTypes, allTypeLens);
            getAllAttr(refTableName[i], allRefAttrName, allRefOffsets, allRefTypes, allRefTypeLens);

            std::cout << "FOREIGN KEY " << foreignNames[i] << " (";
            std::string p = "";
            for (auto off : indexNos[i])
            {
                auto it = std::find(allOffsets.begin(), allOffsets.end(), off);
                auto idx = std::distance(allOffsets.begin(), it);
                p += allAttrName[idx] + ", ";
            }
            p.pop_back();
            p.pop_back();
            std::cout << p << ") REFERENCES " << refTableName[i] << " (";

            p = "";
            for (auto off : refIndexNo[i])
            {
                auto it = std::find(allRefOffsets.begin(), allRefOffsets.end(), off);
                auto idx = std::distance(allRefOffsets.begin(), it);
                p += allRefAttrName[idx] + ", ";
            }
            p.pop_back();
            p.pop_back();
            std::cout << p << ")" << std::endl;
        }

        // unique
        std::vector<std::vector<int>> uniques;
        getUnique(tableName, uniques);
        for (size_t i = 0; i < uniques.size(); i++)
        {
            std::cout << "UNIQUE (";
            std::string p;
            for (auto off : uniques[i])
            {
                auto it = std::find(offsets.begin(), offsets.end(), off);
                auto idx = std::distance(offsets.begin(), it);
                p += attributes[idx] + ", ";
            }
            p.pop_back();
            p.pop_back();
            std::cout << p << ")" << std::endl;
        }
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
        rm->openFile(openedDbName + "/relcat", relCatHandle);
        scan.openScan(relCatHandle, AttrType::VARCHAR, RELNAME_MAX_BYTES, offsetof(RelCat, RelCat::relName), CompOp::E, value);
        RID rid(-1, -1);
        while (scan.getNextRec(rec))
        {
            scan.closeScan();
            rm->closeFile(openedDbName + "/relcat");
            return true;
        }
        scan.closeScan();
        rm->closeFile(openedDbName + "/relcat");
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
        rm->openFile(openedDbName + "/relcat", relCatHandle);
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
        rm->closeFile(openedDbName + "/relcat");

        if (!rid.valid())
            return false;

        FileHandle hd;
        rm->openFile(openedDbName + "/" + tableName + ".index", hd);
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
        rm->closeFile(openedDbName + "/" + tableName + ".index");
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
        rm->openFile(openedDbName + "/relcat", relCatHandle);
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
        rm->closeFile(openedDbName + "/relcat");

        if (!rid.valid())
            return false;

        FileHandle hd;
        rm->openFile(openedDbName + "/" + tableName + ".index", hd);
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
        rm->closeFile(openedDbName + "/" + tableName + ".index");
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
        rm->openFile(openedDbName + "/relcat", relCatHandle);
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
        rm->closeFile(openedDbName + "/relcat");

        if (!rid.valid())
            return false;

        FileHandle hd;
        rm->openFile(openedDbName + "/" + tableName + ".index", hd);
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
        rm->closeFile(openedDbName + "/" + tableName + ".index");
        return true;
    }
    bool dropPrimaryKey(const std::string tableName, std::string indexName = "")
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
        rm->openFile(openedDbName + "/attrcat", attrCatHandle);
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
        rm->closeFile(openedDbName + "/attrcat");

        if (cat == nullptr)
        {
            std::cout << "Table " << tableName << " not exists." << std::endl;
            return false;
        }

        int *data = new int[attributes.size()];
        memset(data, 0, 4 * attributes.size());
        FileHandle hd;
        rm->openFile(openedDbName + "/" + tableName + ".index", hd);
        RID r(-1, -1);
        int primary = 1;
        scan.openScan(hd, AttrType::INT, 4, 0, CompOp::E, &primary);
        while (scan.getNextRec(rec))
        {
            DataType tmp;
            rec.getData(tmp);
            if (indexName.size() > 0 && strcmp(tmp + 4 + 4 * attributes.size(), indexName.c_str()) != 0)
                continue;
            memcpy(data, tmp + 4, 4 * attributes.size());
            rec.getRID(r);
        }
        scan.closeScan();
        if (!r.valid())
        {
            std::cout << "Primary Key " << indexName << " not exists." << std::endl;
            rm->closeFile(openedDbName + "/" + tableName + ".index");
            return false;
        }
        hd.deleteRec(r);
        rm->closeFile(openedDbName + "/" + tableName + ".index");

        for (int i = 0; i < attributes.size() && data[i] >= 0; i++)
            indexNo.push_back(data[i]);
        delete[] data;
        im->destroyIndex(openedDbName + "/" + tableName, indexNo);
        return true;
    }
    bool showAllIndexes()
    {
        if (!dbOpened)
        {
            std::cout << "No database used." << std::endl;
            return false;
        }

        // get all tables
        std::vector<std::string> tableNames;
        rm->openFile(openedDbName + "/relcat", relCatHandle);
        FileScan fs;
        fs.openScan(relCatHandle, AttrType::VARCHAR, RELNAME_MAX_BYTES, 0, CompOp::NO, nullptr);
        Record rec;
        while (fs.getNextRec(rec))
        {
            DataType tmp;
            rec.getData(tmp);
            tableNames.push_back(tmp);
        }
        fs.closeScan();
        rm->closeFile(openedDbName + "/relcat");

        // get and print every index on each table
        std::cout << "+-------+---------+---------+" << std::endl
                  << "| Table | PRIMARY | columns |" << std::endl
                  << "+-------+---------+---------+" << std::endl;
        for (auto tableName : tableNames)
        {
            std::vector<std::string> allAttrName;
            std::vector<int> allOffsets;
            std::vector<AttrType> allTypes;
            std::vector<int> allTypeLens;
            getAllAttr(tableName, allAttrName, allOffsets, allTypes, allTypeLens);

            // print primary key
            std::vector<int> primaryKey;
            getPrimaryKey(tableName, primaryKey);
            if (primaryKey.size() > 0)
            {
                std::cout << "|" << std::left << setw(7) << tableName << "|"
                          << "YES      "
                          << "|";
                string col;
                for (auto key : primaryKey)
                {
                    auto it = std::find(allOffsets.begin(), allOffsets.end(), key);
                    auto idx = std::distance(allOffsets.begin(), it);
                    col += allAttrName[idx] + ",";
                }
                col.pop_back();
                std::cout << std::left << setw(9) << col << "|" << std::endl;
            }

            // print indexes
            std::vector<std::vector<int>> indexNo;
            getAllIndex(tableName, indexNo);
            if (indexNo.size() > 0)
            {
                for (auto index : indexNo)
                {
                    std::cout << "|" << std::left << setw(7) << tableName << "|"
                              << "NO       "
                              << "|";
                    string col;
                    for (auto key : index)
                    {
                        auto it = std::find(allOffsets.begin(), allOffsets.end(), key);
                        auto idx = std::distance(allOffsets.begin(), it);
                        col += allAttrName[idx] + ",";
                    }
                    col.pop_back();
                    std::cout << std::left << setw(9) << col << "|" << std::endl;
                }
            }
        }
        std::cout << "+-------+---------+---------+" << std::endl;
        return true;
    }
    bool dumpToFile(const std::string &filename, const std::string &tableName)
    {
        if (!dbOpened)
        {
            std::cout << "No database used." << std::endl;
            return false;
        }

        if (!checkTableExists(tableName))
        {
            std::cout << "Table " << tableName << " not exists." << std::endl;
            return false;
        }

        // get all attr
        std::vector<std::string> allAttrName;
        std::vector<int> allOffsets;
        std::vector<AttrType> allTypes;
        std::vector<int> allTypeLens;
        getAllAttr(tableName, allAttrName, allOffsets, allTypes, allTypeLens);

        // set file ostream
        std::streambuf *coutBuf = std::cout.rdbuf();
        std::ofstream of(filename);
        std::streambuf *fileBuf = of.rdbuf();
        std::cout.rdbuf(fileBuf);

        // print column name
        std::string attrStr;
        for (auto name : allAttrName)
            attrStr += name + "'";
        attrStr.pop_back();
        attrStr += '\n';
        std::cout << attrStr;

        FileHandle fh;
        rm->openFile(openedDbName + "/" + tableName, fh);
        FileScan fs;
        fs.openScan(fh, AttrType::ANY, -1, -1, CompOp::NO, nullptr);
        Record rec;
        while (fs.getNextRec(rec))
        {
            DataType data;
            rec.getData(data);

            std::string recStr;
            SlotMap nullMap(data, allAttrName.size());
            for (int i = 0; i < allAttrName.size(); i++)
            {
                if (nullMap.test(i))
                    recStr += "NULL'";
                else
                {
                    switch (allTypes[i])
                    {
                    case AttrType::INT:
                    {
                        int *val = new int;
                        memcpy(val, data + allOffsets[i], sizeof(int));
                        recStr += std::to_string(*val) + "'";
                        break;
                    }
                    case AttrType::FLOAT:
                    {
                        float *val = new float;
                        memcpy(val, data + allOffsets[i], sizeof(float));
                        recStr += std::to_string(*val) + "'";
                        break;
                    }
                    case AttrType::VARCHAR:
                    {
                        char *val = new char[allTypeLens[i]];
                        memcpy(val, data + allOffsets[i], allTypeLens[i]);
                        recStr += std::string(val) + "'";
                        break;
                    }
                    default:
                        recStr += "NULL'";
                        break;
                    }
                }
            }
            recStr.pop_back();
            recStr += "\n";
            std::cout << recStr;
        }
        fs.closeScan();
        rm->closeFile(openedDbName + "/" + tableName);

        // reset file ostream
        of.flush();
        of.close();
        std::cout.rdbuf(coutBuf);

        return true;
    }
    bool checkIndex(const std::string &tableName, const std::vector<std::string> &attrName, std::vector<int> &indexNo, int &attrCount)
    {
        FileScan scan;
        Record rec;
        char value[RELNAME_MAX_BYTES] = "\0";
        strcpy(value, tableName.c_str());

        std::vector<std::string> attributes;
        std::vector<int> offsets;
        rm->openFile(openedDbName + "/attrcat", attrCatHandle);
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
        rm->closeFile(openedDbName + "/attrcat");

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

        attrCount = attributes.size();
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
        rm->openFile(openedDbName + "/" + tableName + ".index", hd);
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
            rm->closeFile(openedDbName + "/" + tableName + ".index");
            return false;
        }
        rm->closeFile(openedDbName + "/" + tableName + ".index");
        delete[] data;
        return true;
    }
    bool createForeign(const std::string &tableName, const std::vector<std::string> &forns, const std::string &refTableName, const std::vector<std::string> &refs, std::string foreignName = "")
    {
        if (!dbOpened)
        {
            std::cout << "No database used." << std::endl;
            return false;
        }

        if (!checkTableExists(tableName))
        {
            std::cout << "Table " << tableName << " not exists." << std::endl;
            return false;
        }
        if (!checkTableExists(refTableName))
        {
            std::cout << "Table " << refTableName << " not exists." << std::endl;
            return false;
        }

        // check foerign columns count
        if (forns.size() != refs.size())
        {
            std::cout << "ERROR: Columns do not have the same count." << std::endl;
            return false;
        }

        // check type
        std::vector<std::string> allAttrName, allRefAttrName;
        std::vector<int> allOffsets, allRefOffsets;
        std::vector<AttrType> allTypes, allRefTypes;
        std::vector<int> allTypeLens, allRefTypeLens;
        getAllAttr(tableName, allAttrName, allOffsets, allTypes, allTypeLens);
        getAllAttr(refTableName, allRefAttrName, allRefOffsets, allRefTypes, allRefTypeLens);
        for (int i = 0; i < forns.size(); i++)
        {
            auto it = std::find(allAttrName.begin(), allAttrName.end(), forns[i]);
            auto idx = std::distance(allAttrName.begin(), it);
            auto it_ = std::find(allRefAttrName.begin(), allRefAttrName.end(), refs[i]);
            auto idx_ = std::distance(allRefAttrName.begin(), it_);
            if (allTypes[idx] != allRefTypes[idx_])
            {
                std::cout << "ERROR: Type mismatched with column " << tableName << "." << forns[i] << " and " << refTableName << "." << refs[i] << std::endl;
                return false;
            }
            if (allTypes[idx] != AttrType::INT)
            {
                std::cout << "ERROR: Foreign column " << forns[i] << " must be INT." << std::endl;
                return false;
            }
        }

        // check if reference table has an index on it
        std::vector<int> refIndexNo;
        int refAttrCount;
        if (!checkIndex(refTableName, refs, refIndexNo, refAttrCount))
        {
            std::cout << "ERROR: Reference table must have an unique index on the given columns." << std::endl;
            return false;
        }

        // check if reference table has duplicate key on it
        if (!checkUnique(refTableName, refs))
        {
            std::cout << "ERROR: Reference table " << refTableName << " has duplicate key." << std::endl;
            return false;
        }

        // insert tableName to refTable's .ref
        FileHandle hd;
        rm->openFile(openedDbName + "/" + refTableName + ".ref", hd);
        Record rec;
        RID rid(-1, -1);
        FileScan fs;
        char *val = new char[RELNAME_MAX_BYTES];
        strcpy(val, tableName.c_str());
        fs.openScan(hd, AttrType::VARCHAR, RELNAME_MAX_BYTES, 0, CompOp::E, val);
        while (fs.getNextRec(rec))
        {
            rec.getRID(rid);
            break;
        }
        fs.closeScan();
        if (!rid.valid())
            hd.insertRec(val, rid);
        rm->closeFile(openedDbName + "/" + refTableName + ".ref");

        // check if table has an index on it
        std::vector<int> indexNo;
        int attrCount;
        if (!checkIndex(tableName, forns, indexNo, attrCount))
            createIndex(tableName, forns, false);

        // insert refTableName to table's .foreign
        rm->openFile(openedDbName + "/" + tableName + ".foreign", hd);
        rid = RID(-1, -1);
        memset(val, 0, RELNAME_MAX_BYTES);
        strcpy(val, refTableName.c_str());
        fs.openScan(hd, AttrType::VARCHAR, RELNAME_MAX_BYTES, 0, CompOp::E, val);
        while (fs.getNextRec(rec))
        {
            rec.getRID(rid);
            break;
        }
        fs.closeScan();
        if (!rid.valid())
        {
            hd.insertRec(val, rid);
            rm->createFile(openedDbName + "/" + tableName + "." + refTableName, 4 * (attrCount + refAttrCount) + IXNAMECHAR_MAX_BYTES);
        }
        rm->closeFile(openedDbName + "/" + tableName + ".foreign");

        // insert offsets to foreign
        int *data = new int[attrCount + refAttrCount];
        int i = 0;
        while (i < attrCount)
        {
            if (i < indexNo.size())
                data[i] = indexNo[i];
            else
                data[i] = -1;
            i++;
        }
        while (i < attrCount + refAttrCount)
        {
            if (i < attrCount + refIndexNo.size())
                data[i] = refIndexNo[i - attrCount];
            else
                data[i] = -1;
            i++;
        }
        rm->openFile(openedDbName + "/" + tableName + "." + refTableName, hd);
        char *data_ext = new char[4 * (attrCount + refAttrCount) + IXNAMECHAR_MAX_BYTES];
        memcpy(data_ext, data, 4 * (attrCount + refAttrCount));
        strcpy(data_ext + 4 * (attrCount + refAttrCount), foreignName.c_str());
        hd.insertRec(data_ext, rid);
        rm->closeFile(openedDbName + "/" + tableName + "." + refTableName);

        delete[] val;
        delete[] data;
        return true;
    }
    bool dropForeign(const std::string &tableName, const std::string &foreignName)
    {
        std::vector<std::string> refTableName, foreignNames;
        std::vector<std::vector<int>> indexNo, refIndexNo;
        getForeign(tableName, refTableName, indexNo, refIndexNo, foreignNames);

        auto it = std::find(foreignNames.begin(), foreignNames.end(), foreignName);
        if (it == foreignNames.end())
        {
            std::cout << "Foreign key " << foreignName << " not exists." << std::endl;
            return false;
        }

        int attrCount, refAttrCount;
        getAttrCount(tableName, attrCount);
        char val[IXNAMECHAR_MAX_BYTES] = "\0";
        strcpy(val, foreignName.c_str());
        std::string chosenRef;
        for (size_t i = 0; i < refTableName.size(); i++)
        {
            getAttrCount(refTableName[i], refAttrCount);
            FileHandle fh;
            FileScan fs;
            Record rec;
            RID r(-1, -1);
            rm->openFile(openedDbName + "/" + tableName + "." + refTableName[i], fh);
            fs.openScan(fh, AttrType::VARCHAR, IXNAMECHAR_MAX_BYTES, 4 * (attrCount + refAttrCount), CompOp::E, val);
            while (fs.getNextRec(rec))
            {
                rec.getRID(r);
                break;
            }
            fs.closeScan();
            if (r.valid())
                fh.deleteRec(r);
            rm->closeFile(openedDbName + "/" + tableName + "." + refTableName[i]);
        }
        return true;
    }
    bool createUnique(const std::string &tableName, const std::vector<std::string> &attrName)
    {
        FileScan scan;
        Record rec;
        char value[RELNAME_MAX_BYTES] = "\0";
        strcpy(value, tableName.c_str());

        std::vector<std::string> attributes;
        std::vector<int> offsets;
        std::vector<int> indexNo;
        rm->openFile(openedDbName + "/attrcat", attrCatHandle);
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
        rm->closeFile(openedDbName + "/attrcat");

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

        // check if already exists
        std::vector<std::vector<int>> uniques;
        getUnique(tableName, uniques);
        auto it = std::find(uniques.begin(), uniques.end(), indexNo);
        if (it != uniques.end())
        {
            std::cout << "Unique constraint already exists." << std::endl;
            return false;
        }

        // check duplicate
        if (!checkUnique(tableName, attrName))
        {
            std::cout << "Unique not created due to duplicate value." << std::endl;
            return false;
        }

        // finally insert
        FileHandle fh;
        rm->openFile(openedDbName + "/" + tableName + ".unique", fh);
        RID rid;
        fh.insertRec(reinterpret_cast<DataType>(data), rid);
        rm->closeFile(openedDbName + "/" + tableName + ".unique");

        return true;
    }
    bool checkUnique(const std::string &tableName, const std::vector<std::string> &attrName)
    {
        FileScan scan;
        Record rec;
        char value[RELNAME_MAX_BYTES] = "\0";
        strcpy(value, tableName.c_str());

        std::vector<std::string> attributes;
        std::vector<int> offsets;
        std::vector<int> lens;
        std::vector<int> indexNo;
        std::vector<int> lenNo;
        rm->openFile(openedDbName + "/attrcat", attrCatHandle);
        scan.openScan(attrCatHandle, AttrType::VARCHAR, RELNAME_MAX_BYTES, offsetof(AttrCat, AttrCat::relName), CompOp::E, value);
        AttrCat *cat = nullptr;
        while (scan.getNextRec(rec))
        {
            DataType tmp;
            rec.getData(tmp);
            cat = reinterpret_cast<AttrCat *>(tmp);
            attributes.push_back(cat->attrName);
            offsets.push_back(cat->offset);
            lens.push_back(cat->typeLen);
        }
        scan.closeScan();
        rm->closeFile(openedDbName + "/attrcat");

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
            lenNo.push_back(lens[idx]);
        }
        std::sort(indexNo.begin(), indexNo.end());

        int *data = new int[attributes.size()];
        memset(data, 0, 4 * attributes.size());
        for (auto i = 0; i < attributes.size(); i++)
        {
            if (i < indexNo.size())
                data[i] = indexNo[i];
            else
                data[i] = -1;
        }
        // first check if there's a unique constraint
        FileHandle hd;
        rm->openFile(openedDbName + "/" + tableName + ".unique", hd);
        RID r(-1, -1);
        scan.openScan(hd, AttrType::ANY, 4, 0, CompOp::NO, nullptr);
        while (scan.getNextRec(rec))
        {
            DataType tmp;
            rec.getData(tmp);
            if (memcmp(data, tmp, 4 * attributes.size()) == 0)
                rec.getRID(r);
        }
        scan.closeScan();
        if (r.valid())
        {
            rm->closeFile(openedDbName + "/" + tableName + ".index");
            return true;
        }
        rm->closeFile(openedDbName + "/" + tableName + ".unique");
        delete[] data;

        // second check if there's a primary key
        std::vector<int> primaryKey;
        getPrimaryKey(tableName, primaryKey);
        if (primaryKey == indexNo)
            return true;

        // last check every records in the table
        FileScan fs2;
        Record rec2;
        rm->openFile(openedDbName + "/" + tableName, hd);
        RID p, q;
        DataType pd, qd;
        bool flag = false;
        scan.openScan(hd, AttrType::ANY, 4, 0, CompOp::NO, nullptr);
        while (scan.getNextRec(rec))
        {
            rec.getRID(p);
            rec.getData(pd);
            fs2.openScan(hd, AttrType::ANY, 4, 0, CompOp::NO, nullptr);
            while (fs2.getNextRec(rec2))
            {
                rec2.getRID(q);
                rec2.getData(qd);
                if (p.equals(q))
                    continue;
                flag = true;
                for (auto i = 0; i < lenNo.size() && flag; i++)
                    if (memcmp(pd + indexNo[i], qd + indexNo[i], lenNo[i]) != 0)
                        flag = false;
                if (flag)
                    break;
            }
            fs2.closeScan();
            if (flag)
                break;
        }
        scan.closeScan();
        rm->closeFile(openedDbName + "/" + tableName);
        return !flag;
    }
    bool getUnique(const std::string &tableName, std::vector<std::vector<int>> &indexNo)
    {
        // get attrCount
        int attrCount;
        rm->openFile(openedDbName + "/relcat", relCatHandle);
        FileScan fs;
        Record rec;
        RID rid(-1, -1);
        RelCat cat;
        char val[RELNAME_MAX_BYTES] = "\0";
        strcpy(val, tableName.c_str());
        fs.openScan(relCatHandle, AttrType::VARCHAR, RELNAME_MAX_BYTES, offsetof(RelCat, RelCat::relName), CompOp::E, val);
        while (fs.getNextRec(rec))
        {
            rec.getRID(rid);
            DataType tmp;
            rec.getData(tmp);
            memcpy(&cat, tmp, sizeof(RelCat));
            attrCount = cat.attrCount;
            break;
        }
        if (!rid.valid())
            return false;

        // deserialize indexNo
        FileHandle fh;
        int *data = new int[attrCount];
        memset(data, 0, 4 * attrCount);
        rm->openFile(openedDbName + "/" + tableName + ".unique", fh);
        fs.openScan(fh, AttrType::ANY, -1, -1, CompOp::NO, nullptr);
        while (fs.getNextRec(rec))
        {
            std::vector<int> uniq;
            DataType tmp;
            rec.getData(tmp);
            memcpy(data, tmp, 4 * attrCount);
            for (int i = 0; i < attrCount && data[i] >= 0; i++)
                uniq.push_back(data[i]);
            indexNo.push_back(uniq);
        }
        fs.closeScan();
        rm->closeFile(openedDbName + "/" + tableName + ".unique");
        return true;
    }
    bool getForeign(const std::string &tableName, std::vector<std::string> &refTableName, std::vector<std::vector<int>> &indexNo, std::vector<std::vector<int>> &refIndexNo, std::vector<std::string> &foreignName)
    {
        if (!dbOpened)
        {
            std::cout << "No database used." << std::endl;
            return false;
        }

        if (!checkTableExists(tableName))
            return false;

        // get reference tables
        std::vector<std::string> refTables;
        FileHandle fh;
        rm->openFile(openedDbName + "/" + tableName + ".foreign", fh);
        FileScan fs;
        fs.openScan(fh, AttrType::ANY, -1, -1, CompOp::NO, nullptr);
        Record rec;
        while (fs.getNextRec(rec))
        {
            DataType tmp;
            rec.getData(tmp);
            refTables.push_back(tmp);
        }
        fs.closeScan();
        rm->closeFile(openedDbName + "/" + tableName + ".foreign");

        // get foreign offsets for each ref table
        for (auto reft : refTables)
        {
            int attrCount, refAttrCount;
            getAttrCount(tableName, attrCount);
            getAttrCount(reft, refAttrCount);

            int *data = new int[attrCount + refAttrCount];
            memset(data, 0, 4 * (attrCount + refAttrCount));
            rm->openFile(openedDbName + "/" + tableName + "." + reft, fh);
            fs.openScan(fh, AttrType::ANY, -1, -1, CompOp::NO, nullptr);
            while (fs.getNextRec(rec))
            {
                DataType tmp;
                rec.getData(tmp);
                memcpy(data, tmp, 4 * (attrCount + refAttrCount));
                std::vector<int> p, q;
                for (int i = 0; i < attrCount && data[i] >= 0; i++)
                    p.push_back(data[i]);
                for (int i = 0; i < refAttrCount && data[attrCount + i] >= 0; i++)
                    q.push_back(data[attrCount + i]);
                refTableName.push_back(reft);
                indexNo.push_back(p);
                refIndexNo.push_back(q);
                std::string name(tmp + 4 * (attrCount + refAttrCount));
                foreignName.push_back(name);
            }
            fs.closeScan();
            rm->closeFile(openedDbName + "/" + tableName + "." + reft);
        }

        return true;
    }
    bool getReference(const std::string &refTableName, std::vector<std::string> &tableName, std::vector<std::vector<int>> &refIndexNo, std::vector<std::vector<int>> &indexNo)
    {
        if (!dbOpened)
        {
            std::cout << "No database used." << std::endl;
            return false;
        }

        if (!checkTableExists(refTableName))
            return false;

        // get reference tables
        std::vector<std::string> tables;
        FileHandle fh;
        rm->openFile(openedDbName + "/" + refTableName + ".ref", fh);
        FileScan fs;
        fs.openScan(fh, AttrType::ANY, -1, -1, CompOp::NO, nullptr);
        Record rec;
        while (fs.getNextRec(rec))
        {
            DataType tmp;
            rec.getData(tmp);
            tables.push_back(tmp);
        }
        fs.closeScan();
        rm->closeFile(openedDbName + "/" + refTableName + ".ref");

        // get foreign offsets for each ref table
        for (auto t : tables)
        {
            int attrCount, refAttrCount;
            getAttrCount(t, attrCount);
            getAttrCount(refTableName, refAttrCount);

            int *data = new int[attrCount + refAttrCount];
            memset(data, 0, 4 * (attrCount + refAttrCount));
            rm->openFile(openedDbName + "/" + t + "." + refTableName, fh);
            fs.openScan(fh, AttrType::ANY, -1, -1, CompOp::NO, nullptr);
            while (fs.getNextRec(rec))
            {
                DataType tmp;
                rec.getData(tmp);
                memcpy(data, tmp, 4 * (attrCount + refAttrCount));
                std::vector<int> p, q;
                for (int i = 0; i < attrCount && data[i] >= 0; i++)
                    p.push_back(data[i]);
                for (int i = 0; i < refAttrCount && data[attrCount + i] >= 0; i++)
                    q.push_back(data[attrCount + i]);
                tableName.push_back(t);
                indexNo.push_back(p);
                refIndexNo.push_back(q);
            }
            fs.closeScan();
            rm->closeFile(openedDbName + "/" + t + "." + refTableName);
        }

        return true;
    }
    bool getAttrCount(const std::string &tableName, int &attrCount)
    {
        FileHandle hd;
        if (!dbOpened)
            return false;
        FileScan scan;
        Record rec;
        char value[RELNAME_MAX_BYTES] = "\0";
        strcpy(value, tableName.c_str());

        rm->openFile(openedDbName + "/relcat", hd);
        scan.openScan(hd, AttrType::VARCHAR, RELNAME_MAX_BYTES, offsetof(RelCat, RelCat::relName), CompOp::E, value);
        RelCat *cat = nullptr;
        while (scan.getNextRec(rec))
        {
            DataType tmp;
            rec.getData(tmp);
            cat = reinterpret_cast<RelCat *>(tmp);
        }
        scan.closeScan();
        rm->closeFile(openedDbName + "/relcat");
        if (cat)
        {
            attrCount = cat->attrCount;
            return true;
        }
        else
            return false;
    }
};