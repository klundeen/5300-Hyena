// Authors: Dhruv Patel
// Course: CPSC5300, Seattle University, WQ'24

#include "db_cxx.h"
#include "SQLParser.h"
#include "sqlhelper.h"
#include "heap_storage.h"
#include <stdio.h>
#include <stdlib.h>
#include <cstring>
#include <iostream>
#include <string>

using namespace std;
using namespace hsql;

// Prototypes
string expressionToString(const Expr * expression);
string operatorToString(const Expr *opExpr);
string columnToString(const ColumnDefinition *col);
string tableRefToString(const TableRef *table);
string executeSelect(const SelectStatement *statement);
string executeCreate(const CreateStatement *statement);
string executeInsert(const InsertStatement *statement);
string execute(const SQLStatement *statement);

// Function to convert an expression to a string
string expressionToString(const Expr * expression) {
  string result;

  switch (expression->type) {
    case kExprStar:
      result += "*";
      break;
    case kExprColumnRef:
      if (expression->table != NULL) {
        result += string(expression->table) + ".";
      }
    case kExprLiteralString:
      result += expression->name;
      break;
    case kExprLiteralFloat:
      result += to_string(expression->fval);
      break;
    case kExprLiteralInt:
      result += to_string(expression->ival);
      break;
    case kExprFunctionRef:
      result += string(expression->name) + " " + expression->expr->name;
      break;
    case kExprOperator:
      result += operatorToString(expression);
      break;
    default:
      result += "Unknown expression.";
      break;
  }

  if (expression->alias != NULL) {
    result += string(" AS ") + expression->alias;
  }

  return result;
}

// Function to convert an operator expression to a string
string operatorToString(const Expr *opExpr){
  string result;

  if (opExpr == NULL) {
    return "NULL";
  }

  result += expressionToString(opExpr->expr) + " ";

  switch (opExpr->opType) {
    case Expr::SIMPLE_OP:
      result += opExpr->opChar;
      break;
    case Expr::AND:
      result += "AND";
      break;
    case Expr::OR:
      result += "OR";
      break;
    case Expr::NOT:
      result += "NOT";
      break;
    default:
      break;
  }

  if (opExpr->expr2 != NULL) {
    result += " " + expressionToString(opExpr->expr2);
  }

  return result;
}

// Function to convert a column definition to a string
string columnToString(const ColumnDefinition *col){

  string result = string(col->name) + " ";

  switch(col->type){
    case ColumnDefinition::DOUBLE:
      result += "DOUBLE ";
      break;
    case ColumnDefinition::INT:
      result += "INT ";
      break;
    case ColumnDefinition::TEXT:
      result += "TEXT ";
      break;
    default:
      result += "...";
      break;
  }

  return result;
}

// Function to convert a table reference to a string
string tableRefToString(const TableRef *table) {
  string result;
  switch (table->type) {
    case kTableSelect:
      result += "kTableSelect not implemented"; 
      break;
    case kTableName:
      result += table->name;
      if (table->alias != NULL){
        result += string(" AS ") + table->alias;
      }
      break;
    case kTableJoin:
      result += tableRefToString(table->join->left);
      switch (table->join->type) {
        case kJoinCross:
          result += " JOIN CROSS NOT IMPLEMENTED ";
          break;
        case kJoinInner:
          result += " JOIN ";
          break;
        case kJoinOuter:
          result += " JOIN OUTER NOT IMPLEMENTED ";
          break;
        case kJoinLeftOuter:
          result += " JOIN LEFT OUTER NOT IMPLEMENTED ";
          break;
        case kJoinLeft:
          result += " LEFT JOIN ";
          break;
        case kJoinRightOuter:
          result += " JOIN RIGHT OUTER NOT IMPLEMENTED ";
          break;
        case kJoinRight:
          result += " RIGHT JOIN ";
          break;
        case kJoinNatural:
          result += " NATURAL JOIN ";
          break;
      }
      result += tableRefToString(table->join->right);
      if (table->join->condition != NULL){
        result += " ON " + expressionToString(table->join->condition);
      }
      break;
    case kTableCrossProduct:
      bool comma = false;
      for (TableRef *tbl : *table->list) {
        if (comma)
          result += ", ";
        result += tableRefToString(tbl);
        comma = true;
      }
      break;
  }
  return result;
}

// Function to execute a SELECT statement
string executeSelect(const SelectStatement *statement) {
  string result = "SELECT ";
  bool comma = false;
  for (Expr *expr : *statement->selectList) {
    if (comma) {
      result += ", ";
    }
    result += expressionToString(expr);
    comma = true;
  }
  result += " FROM " + tableRefToString(statement->fromTable);
  if (statement->whereClause != NULL) {
    result += " WHERE " + expressionToString(statement->whereClause);
  }
  return result;
}

// Function to execute a CREATE statement
string executeCreate(const CreateStatement *statement) {
  string result = "CREATE TABLE ";
  bool ifComma = false;

  if(statement->ifNotExists){
    return result + "DOES NOT EXIST ";
  }

  result += string(statement->tableName) + " (";

  for(ColumnDefinition *column: *statement->columns){
    if(ifComma){
      result += ", ";
    }

    result += columnToString(column);
    ifComma = true;
  }

  result += ")";
  return result;
}

// Function to execute an INSERT statement
string executeInsert(const InsertStatement *statement) {
  string result = " INSERT NOT IMPLEMENTED";
  return result;
}

// Function to execute a SQL statement
string execute(const SQLStatement *statement) {
  switch (statement->type()) {
    case kStmtSelect:
      return executeSelect((const SelectStatement *) statement);
    case kStmtInsert:
      return executeInsert((const InsertStatement *) statement);
    case kStmtCreate:
      return executeCreate((const CreateStatement *) statement);
    default:
      return "Statement not implemented.";
  }
}

DbEnv *_DB_ENV;

int main(int argc, char* argv[]){
  const string QUIT = "quit";
  const string TEST = "test";
  string userInput = "";
  char *location;
  SQLParserResult* parsedResult;

  if (argc != 2) {
    cerr << "Usage: ./sql5300 dbenvpath" << endl;
    return 1;
  }

  location = argv[1];

  char *envHome = argv[1];
  cout << "(sql5300: running with database environment at " << envHome << ")" << endl;
  DbEnv myEnv(0U);
  myEnv.set_message_stream(&cout);
  myEnv.set_error_stream(&cerr);
  myEnv.open(location, DB_CREATE | DB_INIT_MPOOL, 0);

  _DB_ENV = &myEnv;

  while (true) {
    cout << "SQL> ";
    getline(cin, userInput);

    if (userInput == QUIT) {
      break;
    }

    if (userInput == TEST) {
      cout << "testing_heap_storage: " << (test_heap_storage() ? "ok" : "failed") << endl;
      continue;
    }
    
    parsedResult = SQLParser::parseSQLString(userInput);

    if (parsedResult->isValid()) {
      for (uint i = 0; i < parsedResult->size(); i++) {
        cout << execute(parsedResult->getStatement(i)) << endl;
      }
    }
    else {
      cout << "ERROR: Invalid SQL" << endl;
    }

    delete parsedResult;
  }
}