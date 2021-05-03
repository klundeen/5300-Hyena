/**
 * @file SQLExec.cpp - implementation of SQLExec class
 * @author Kevin Lundeen
 * @see "Seattle University, CPSC5300, Spring 2021"
 */
#include "SQLExec.h"

using namespace std;
using namespace hsql;

// define static data
Tables *SQLExec::tables = nullptr;

// make query result be printable
ostream &operator<<(ostream &out, const QueryResult &qres) {
    if (qres.column_names != nullptr) {
        for (auto const &column_name: *qres.column_names)
            out << column_name << " ";
        out << endl << "+";
        for (unsigned int i = 0; i < qres.column_names->size(); i++)
            out << "----------+";
        out << endl;
        for (auto const &row: *qres.rows) {
            for (auto const &column_name: *qres.column_names) {
                Value value = row->at(column_name);
                switch (value.data_type) {
                    case ColumnAttribute::INT:
                        out << value.n;
                        break;
                    case ColumnAttribute::TEXT:
                        out << "\"" << value.s << "\"";
                        break;
                    default:
                        out << "???";
                }
                out << " ";
            }
            out << endl;
        }
    }
    out << qres.message;
    return out;
}

QueryResult::~QueryResult() {
    // FIXME
}


QueryResult *SQLExec::execute(const SQLStatement *statement) {
    if (SQLExec::tables == nullptr)
        SQLExec::tables = new Tables();
    try {
        switch (statement->type()) {
            case kStmtCreate:
                return create((const CreateStatement *) statement);
            case kStmtDrop:
                return drop((const DropStatement *) statement);
            case kStmtShow:
                return show((const ShowStatement *) statement);
            default:
                return new QueryResult("not implemented");
        }
    } catch (DbRelationError &e) {
        throw SQLExecError(string("DbRelationError: ") + e.what());
    }
}

void
SQLExec::column_definition(const ColumnDefinition *col, Identifier &column_name, ColumnAttribute &column_attribute) {
    throw SQLExecError("not implemented");  // FIXME
}

// create table
// .py line 100
QueryResult *SQLExec::create(const CreateStatement *statement) {

    Identifier table_name = statement->tableName;
    ColumnNames column_names;

    ColumnAttributes column_attributes;
    Identifier column_name;
    ColumnAttribute column_attribute;
    for (ColumnDefinition *col : *statement->columns)
    {
        column_definition(col, column_name, ColumnAttribute);
        column_names.push_back(column_name);
        column_attributes.push_back(column_attribute);
    }

    ValueDict row;
    row["table_name"] = table_name;
    Handle t_handle = SQLExec::tables->insert(&row); //insert a row into table

    try
    {
        for(uint i = 0; i < column_name.size(); i++)
        {
            row["column_name"] = column_names[i];
            row["data_type"] = Value(column_attributes[i].get_data_type() == ColumnAttribute::INT ? "INT" : "TEXT");
            c_handles.push_back(columns.insert(&row));
        }

        // create relation
        DbRelation
    }
    catch(const std::exception& e)
    {
        std::cerr << e.what() << '\n';
    }
    

    return new QueryResult("not implemented"); // FIXME
}

// drop table
// DROP ...
QueryResult *SQLExec::drop(const DropStatement *statement) {
    if(statement->type != hsql::DropStatement::kTable)
        throw SQLExecError("unrecognized DROP type");
    
    string table_name = statement->; 
    if (table_name. TABLE_NAME)//in schema tables.h
    {
        throw SQLExecError("Cannot drop a schema table!");
    }


    delete handles;

    table.drop();
    SQLExec::tables->del(*SQLExec::tables->select(&where)->begin());

    return new QueryResult("not implemented"); // FIXME
}

/**
 *  
 */
QueryResult *SQLExec::show(const ShowStatement *statement) {
    switch(statement->type) {
        case ShowStatement::kColumns:
            return show_columns(statement);
        case ShowStatement::kTables:
            return show_tables();
        default:
            throw new SQLExecError("Unsupported show type");
    }
}

/**
 * This method displays all the tables metadata info
 * 
 * @returns      querry result of show table 
 */
QueryResult *SQLExec::show_tables() {
    ColumnNames *column_names = new ColumnNames();
    column_names->push_back("table_name");

    ColumnAttributes *column_attributes = new ColumnAttributes();
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

    Handles *handles = SQLExec::tables->select();
    ValueDict *row;
    ValueDicts *rows;

    for (const auto & handle : *handles) {
       row = SQLExec::tables->project(handle, column_names); 
       Identifier table_name = (*row)["table_name"].s;
       if (table_name != Tables::TABLE_NAME && table_name != Columns::TABLE_NAME) {
           rows->push_back(row);
       }
    }

    return new QueryResult()
}

/**
 * This method returns columns metadata info of a table
 * 
 * @param   statement       ShowStatement to display columns metadata info of a table
 * @returns                 columns metadata info of a table
 */ 
QueryResult *SQLExec::show_columns(const ShowStatement *statement) {
    // obtain columns metadata table
    DbRelation &columns = SQLExec::tables->get_table(Columns::TABLE_NAME);
    ColumnNames *column_names;
    ColumnAttributes *column_attributes;
    ValueDict condition;
    ValueDicts *rows;

    // construct ColumnNames
    vector<string> columnList {"table_name", "column_name", "data_type"};
    for(const auto& col : columnList) {
        column_names->push_back(col);
    }

    // construct ColumnAttributes
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

    // retrive info for the specifc table
    condition["table_name"] = Value(statement->tableName);
    Handles *handles = columns.select(&condition);
    u_long n = handles->size();

    for (const auto &handle: *handles) {
        ValueDict *row = columns.project(handle, column_names);
        rows->push_back(row);
    }
    delete handles;
    return new QueryResult(column_names, column_attributes, rows, "returned " + to_string(n) + " rows");
}

