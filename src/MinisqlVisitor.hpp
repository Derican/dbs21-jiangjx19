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

    antlrcpp::Any visitShow_dbs(SQLParser::Show_dbsContext *ctx) override
    {
        sm->showAllDb();
        antlrcpp::Any res;
        return res;
    }

    antlrcpp::Any visitUse_db(SQLParser::Use_dbContext *ctx) override
    {
        return visitChildren(ctx);
    }
};