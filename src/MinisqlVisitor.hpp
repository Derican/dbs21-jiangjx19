#include <iostream>
#include "antlr4-runtime.h"
#include "SQLLexer.h"
#include "SQLParser.h"
#include "SQLBaseVisitor.h"

using namespace antlr4;

class MinisqlVisitor : public SQLBaseVisitor
{
public:
    antlrcpp::Any visit(SQLParser::ProgramContext *iTree)
    {
        return iTree->accept(this);
    }

    antlrcpp::Any visitShow_dbs(SQLParser::Show_dbsContext *ctx) override
    {
        std::cout << "databases" << std::endl;
        antlrcpp::Any res;
        return res;
    }
};