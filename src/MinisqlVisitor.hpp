#include <iostream>
#include "antlr4-runtime.h"
#include "SQLLexer.h"
#include "SQLParser.h"
#include "SQLBaseVisitor.h"
#include "SystemManager.hpp"
#include "QueryManager.hpp"

using namespace antlr4;

class MinisqlVisitor : public SQLBaseVisitor
{
private:
    SystemManager *sm;
    QueryManager *qm;

public:
    AttrType getType(SQLParser::Type_Context *ctx)
    {
        std::string type = ctx->getText();
        if (type == "INT")
            return AttrType::INT;
        else if (type == "FLOAT")
            return AttrType::FLOAT;
        else if (type.length() >= 7 && type.substr(0, 7) == "VARCHAR")
            return AttrType::VARCHAR;
        else
            return AttrType::ANY;
    }
    MinisqlVisitor() {}
    MinisqlVisitor(SystemManager *_sm, QueryManager *_qm)
    {
        sm = _sm;
        qm = _qm;
    }
    ~MinisqlVisitor()
    {
        sm = nullptr;
        qm = nullptr;
    }
    antlrcpp::Any visit(SQLParser::ProgramContext *iTree)
    {
        return iTree->accept(this);
    }

    antlrcpp::Any visitCreate_db(SQLParser::Create_dbContext *ctx) override
    {
        std::string dbName = ctx->Identifier()->getText();
        sm->createDb(dbName);
        antlrcpp::Any res;
        return res;
    }

    antlrcpp::Any visitDrop_db(SQLParser::Drop_dbContext *ctx) override
    {
        std::string dbName = ctx->Identifier()->getText();
        sm->destroyDb(dbName);
        antlrcpp::Any res;
        return res;
    }

    antlrcpp::Any visitShow_dbs(SQLParser::Show_dbsContext *ctx) override
    {
        sm->showAllDb();
        antlrcpp::Any res;
        return res;
    }

    antlrcpp::Any visitUse_db(SQLParser::Use_dbContext *ctx) override
    {
        std::string dbName = ctx->Identifier()->getText();
        sm->openDb(dbName);
        antlrcpp::Any res;
        return res;
    }

    antlrcpp::Any visitShow_tables(SQLParser::Show_tablesContext *ctx) override
    {
        sm->showAllTables();
        antlrcpp::Any res;
        return res;
    }

    antlrcpp::Any visitCreate_table(SQLParser::Create_tableContext *ctx) override
    {
        std::string tableName = ctx->Identifier()->getText();
        std::vector<AttrInfo> attrs;
        PrimaryKey prims;
        ForeignKey forns;
        for (auto field : ctx->field_list()->field())
        {
            if (auto normal = dynamic_cast<SQLParser::Normal_fieldContext *>(field))
            {
                AttrInfo info;
                info.attrName = normal->Identifier()->getText();
                info.attrType = getType(normal->type_());
                if (info.attrType == AttrType::VARCHAR)
                    info.attrLength = std::stoi(normal->type_()->Integer()->getText(), nullptr);
                else
                    info.attrLength = 4;
                if (normal->Null())
                    info.nullable = 0;
                else
                    info.nullable = 1;
                memset(info.defVal.String, 0, VARCHAR_MAX_BYTES);
                if (normal->value())
                {
                    info.defaultValid = 1;
                    if (info.attrType == AttrType::INT)
                        info.defVal.Int = std::stoi(normal->value()->Integer()->getText(), nullptr);
                    else if (info.attrType == AttrType::FLOAT)
                        info.defVal.Float = std::stof(normal->value()->Float()->getText(), nullptr);
                    else if (info.attrType == AttrType::VARCHAR)
                    {
                        std::string val = normal->value()->String()->getText();
                        memcpy(info.defVal.String, val.c_str() + 1, val.size() - 2);
                    }
                    else
                        info.defaultValid = 0;
                }
                else
                    info.defaultValid = 0;
                attrs.push_back(info);
            }
            else if (auto primary = dynamic_cast<SQLParser::Primary_key_fieldContext *>(field))
            {
                if (primary->Identifier())
                    prims.relName = primary->Identifier()->getText();
                for (auto ident : primary->identifiers()->Identifier())
                {
                    std::string p_name = ident->getText();
                    prims.keys.push_back(p_name);
                }
            }
            else if (auto foreign = dynamic_cast<SQLParser::Foreign_key_fieldContext *>(field))
            {
                if (foreign->Identifier().size() == 1)
                {
                    forns.ref = foreign->Identifier(0)->getText();
                }
                else
                {
                    forns.relName = foreign->Identifier(0)->getText();
                    forns.ref = foreign->Identifier(1)->getText();
                }
                for (auto ident : foreign->identifiers(0)->Identifier())
                {
                    std::string a_name = ident->getText();
                    forns.attrs.push_back(a_name);
                }
                for (auto ident : foreign->identifiers(1)->Identifier())
                {
                    std::string a_name = ident->getText();
                    forns.refAttrs.push_back(a_name);
                }
            }
        }
        sm->createTable(tableName, attrs);
        antlrcpp::Any res;
        return res;
    }

    antlrcpp::Any visitDrop_table(SQLParser::Drop_tableContext *ctx) override
    {
        std::string tableName = ctx->Identifier()->getText();
        sm->dropTable(tableName);
        antlrcpp::Any res;
        return res;
    }

    antlrcpp::Any visitDescribe_table(SQLParser::Describe_tableContext *ctx) override
    {
        std::string tableName = ctx->Identifier()->getText();
        sm->descTable(tableName);
        antlrcpp::Any res;
        return res;
    }
};