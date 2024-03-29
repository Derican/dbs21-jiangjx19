#include <iostream>

#include "antlr4-runtime.h"
#include "MinisqlVisitor.hpp"
#include "recmanager/RecordManager.hpp"
#include "ixmanager/IndexManager.hpp"
#include "sysmanager/SystemManager.hpp"
#include "qlmanager/QueryManager.hpp"

using namespace antlr4;

class MyParserErrorListener : public antlr4::BaseErrorListener
{
    virtual void syntaxError(
        antlr4::Recognizer *recognizer,
        antlr4::Token *offendingSymbol,
        size_t line,
        size_t charPositionInLine,
        const std::string &msg,
        std::exception_ptr e) override
    {
        std::string s1;
        s1 = "Line(" + std::to_string(line) + ":" + std::to_string(charPositionInLine) + ") Error(" + msg + ")";
        throw std::invalid_argument(s1);
    }
};

antlrcpp::Any parse(std::string sSQL, SystemManager *sm, QueryManager *qm)
{
    MyParserErrorListener errorListner;
    // 解析SQL语句sSQL的过程
    // 转化为输入流
    ANTLRInputStream sInputStream(sSQL);
    // 设置Lexer
    SQLLexer iLexer(&sInputStream);
    CommonTokenStream sTokenStream(&iLexer);
    // 设置Parser
    SQLParser iParser(&sTokenStream);
    iParser.removeErrorListeners();
    iParser.addErrorListener(&errorListner);
    try
    {
        auto iTree = iParser.program();
        // 构造你的visitor
        MinisqlVisitor iVisitor(sm, qm);
        // visitor模式下执行SQL解析过程
        // --如果采用解释器方式可以在解析过程中完成执行过程（相对简单，但是很难进行进一步优化，功能上已经达到实验要求）
        // --如果采用编译器方式则需要生成自行设计的物理执行执行计划（相对复杂，易于进行进一步优化，希望有能力的同学自行调研尝试）
        auto iRes = iVisitor.visit(iTree);
        return iRes;
    }
    catch (std::invalid_argument &e)
    {
        std::cout << e.what() << std::endl;
        return 0;
    }
}

std::string getSql()
{
    char c = ' ';
    std::string sql;
    while (c != ';')
    {
        c = getchar();
        if (c != '\n')
            sql.push_back(c);
    }
    return sql;
}

int main(int, const char **)
{
    std::string sSQL;

    MyBitMap::initConst();
    FileManager *fm = new FileManager();
    BufPageManager *bpm = new BufPageManager(fm);
    RecordManager *rm = new RecordManager(fm, bpm);
    IndexManager *im = new IndexManager(fm, bpm);
    SystemManager *sm = new SystemManager(rm, im);
    QueryManager *qm = new QueryManager(sm, im, rm);

    while (true)
    {
        if (sm->dbOpened)
            std::cout << "minisql(" << sm->openedDbName << ")> ";
        else
            std::cout << "minisql> ";
        sSQL = getSql();
        if (sSQL == "EXIT;" || sSQL == "QUIT;")
            break;
        try
        {
            parse(sSQL, sm, qm);
        }
        catch (const std::exception &e)
        {
            std::cerr << e.what() << '\n';
            std::cout << "Error! Some data may be deprecated." << std::endl;
        }
    }

    delete qm;
    delete sm;
    delete im;
    delete rm;
    return 0;
}