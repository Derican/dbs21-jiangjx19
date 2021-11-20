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
    bool dbOpened = false;

public:
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
        std::string prefix = "rm -r ";
        system((prefix + dbName).c_str());
        return true;
    }
    bool openDb(const std::string dbName)
    {
        if (dbOpened)
            return false;
        std::string prefix = "cd ";
        system((prefix + dbName).c_str());
        dbOpened = true;
        return true;
    }
    bool closeDb()
    {
        if (!dbOpened)
            return false;
        std::string prefix = "cd ..";
        system(prefix.c_str());
        dbOpened = false;
        return true;
    }
    bool createTable(const std::string tableName, const std::vector<AttrInfo> &attributes)
    {
        int offset = 0;
        RID rid;
        AttrCat attrCat;
        strcpy(attrCat.attrName, tableName.c_str());
        rm->OpenFile("attrcat", attrCatHandle);
        for (auto info : attributes)
        {
            attrCat.type = info.attrType;
            attrCat.typeLen = info.attrLength;
            attrCat.indexNo = -1;
            attrCat.offset = offset;
            strcpy(attrCat.attrName, info.attrName.c_str());
            offset += info.attrLength;
            attrCatHandle.insertRec(reinterpret_cast<DataType>(&attrCat), rid);
        }
        rm->CloseFile("attrcat");

        rm->CreateFile(tableName, offset);
        RelCat relCat;
        strcpy(relCat.relName, tableName.c_str());
        relCat.tupleLength = offset;
        relCat.attrCount = attributes.size();
        relCat.indexCount = 0;
        rm->OpenFile("relcat", relCatHandle);
        relCatHandle.insertRec(reinterpret_cast<DataType>(&relCat), rid);
        rm->CloseFile("relcat");
        return true;
    }
    bool dropTable(const std::string tableName)
    {
        FileScan scan;
        Record rec;
        char value[100] = "\0";
        strcpy(value, tableName.c_str());
        rm->OpenFile("relcat", relCatHandle);
        scan.openScan(relCatHandle, AttrType::VARCHAR, 100, 0, CompOp::E, value);
        while (scan.getNextRec(rec))
        {
            rm->DestroyFile(tableName);
            RID rid;
            rec.getRID(rid);
            relCatHandle.deleteRec(rid);
        }
        scan.closeScan();
        rm->CloseFile("relcat");

        rm->OpenFile("attrcat", attrCatHandle);
        scan.openScan(attrCatHandle, AttrType::VARCHAR, 100, 0, CompOp::E, value);
        while (scan.getNextRec(rec))
        {
            DataType tmp;
            rec.getData(tmp);
            AttrCat *cat = reinterpret_cast<AttrCat *>(tmp);
            if (cat->indexNo >= 0)
                dropIndex(tableName, cat->attrName);
            RID rid;
            rec.getRID(rid);
            attrCatHandle.deleteRec(rid);
        }
        scan.closeScan();
        rm->CloseFile("attrcat");

        return true;
    }
    bool createIndex(const std::string tableName, const std::string attrName)
    {
        FileScan scan;
        Record rec;
        char value[100] = "\0";
        strcpy(value, tableName.c_str());

        rm->OpenFile("attrcat", attrCatHandle);
        scan.openScan(attrCatHandle, AttrType::VARCHAR, 100, 0, CompOp::E, value);
        AttrCat *cat = nullptr;
        while (scan.getNextRec(rec))
        {
            DataType tmp;
            rec.getData(tmp);
            cat = reinterpret_cast<AttrCat *>(tmp);
            if (strcmp(cat->attrName, attrName.c_str()) == 0)
            {
                if (cat->indexNo >= 0)
                    return false;
                cat->indexNo = cat->offset;
                im->createIndex(tableName, cat->indexNo, cat->type, cat->typeLen);
                break;
            }
        }
        scan.closeScan();
        rm->CloseFile("attrcat");

        if (cat == nullptr)
            return false;
        // here we open two files at one time
        IndexHandle ih;
        im->openIndex(tableName, cat->indexNo, ih);
        FileHandle fh;
        rm->OpenFile(tableName, fh);
        scan.openScan(fh, cat->type, cat->typeLen, cat->offset, CompOp::NO, nullptr);
        while (scan.getNextRec(rec))
        {
            DataType tmp;
            rec.getData(tmp);
            RID rid;
            rec.getRID(rid);
            ih.insertEntry(tmp + cat->offset, rid);
        }
        scan.closeScan();
        rm->CloseFile(tableName);
        im->closeIndex(ih);
        return true;
    }
    bool dropIndex(const std::string tableName, const std::string attrName)
    {
        FileScan scan;
        Record rec;
        char value[100] = "\0";
        strcpy(value, tableName.c_str());

        rm->OpenFile("attrcat", attrCatHandle);
        scan.openScan(attrCatHandle, AttrType::VARCHAR, 100, 0, CompOp::E, value);
        AttrCat *cat = nullptr;
        while (scan.getNextRec(rec))
        {
            DataType tmp;
            rec.getData(tmp);
            cat = reinterpret_cast<AttrCat *>(tmp);
            if (strcmp(cat->attrName, attrName.c_str()) == 0)
            {
                if (cat->indexNo >= 0)
                    return false;
                cat->indexNo = cat->offset;
                im->createIndex(tableName, cat->indexNo, cat->type, cat->typeLen);
                break;
            }
        }
        scan.closeScan();
        rm->CloseFile("attrcat");

        im->destroyIndex(tableName, cat->indexNo);
        cat->indexNo = -1;
        RID rid;
        rec.getRID(rid);
        Record newRec(rid, reinterpret_cast<DataType>(cat));
        attrCatHandle.updateRec(newRec);
        return true;
    }
    bool getAllAttr(const std::string &tableName, std::vector<std::string> &attrName, std::vector<int> &offsets, std::vector<AttrType> &types, std::vector<int> &typeLens)
    {
        if (!dbOpened)
            return false;
        FileScan scan;
        Record rec;
        char value[100] = "\0";
        strcpy(value, tableName.c_str());

        rm->OpenFile("attrcat", attrCatHandle);
        scan.openScan(attrCatHandle, AttrType::VARCHAR, 100, 0, CompOp::E, value);
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
        rm->CloseFile("attrcat");
    }
    bool getAttrInfo(const std::string &tableName, const std::string &attrName, int &offset, AttrType &type, int &typeLen)
    {
        if (!dbOpened)
            return false;
        FileScan scan;
        Record rec;
        char value[100] = "\0";
        strcpy(value, tableName.c_str());

        rm->OpenFile("attrcat", attrCatHandle);
        scan.openScan(attrCatHandle, AttrType::VARCHAR, 100, 0, CompOp::E, value);
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
        rm->CloseFile("attrcat");
    }
    bool showAllDb()
    {
        std::cout << "|----------------" << std::endl
                  << "|   databases   " << std::endl
                  << "|----------------" << std::endl;
        rm->OpenFile("dbcat", dbCatHandle);
        FileScan fs;
        fs.openScan(dbCatHandle, AttrType::VARCHAR, DBNAME_MAX_BYTES, 0, CompOp::NO, nullptr);
        Record rec;
        while (fs.getNextRec(rec))
        {
            DataType tmp;
            rec.getData(tmp);
            std::cout << "| " << tmp << std::endl;
        }
        std::cout << "|----------------" << std::endl;
        fs.closeScan();
        rm->CloseFile("dbcat");
        return true;
    }
};