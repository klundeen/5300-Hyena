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
    if (column_names != nullptr) {
        delete column_names;
    }

    if (column_attributes != nullptr) {
        delete column_attributes;
    }

    if (rows != nullptr) {
        for (auto row : *rows) {
            delete row;
        }
        delete rows;
    }
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
   column_name = col->name;
   switch (col->type) {
        case ColumnDefinition::INT:
            column_attribute.set_data_type(ColumnAttribute::INT);
            break;
        case ColumnDefinition::TEXT:
            column_attribute.set_data_type(ColumnAttribute::TEXT);
            break;
        case ColumnDefinition::DOUBLE:
        default:
            throw SQLExecError("unrecognized data type");
   }
}

/**
 * This method creates a table using the values specified in CreateStatement
 * 
 * @param       statement       a CreateStatement to specify what table to create
 * @returns                     a message to indicate the table is created successfully
 */ 
QueryResult *SQLExec::create(const CreateStatement *statement) {

    Identifier table_name = statement->tableName;
    ColumnNames column_names;

    ColumnAttributes column_attributes;
    Identifier column_name;
    ColumnAttribute column_attribute;
    for (ColumnDefinition *col : *statement->columns) {
        column_definition(col, column_name, column_attribute);
        column_names.push_back(column_name);
        column_attributes.push_back(column_attribute);
    }

    ValueDict row;
    row["table_name"] = table_name;
    Handle t_handle = SQLExec::tables->insert(&row); //insert a row into table

    try {
        Handles c_handles;
        DbRelation &columns = SQLExec::tables->get_table(Columns::TABLE_NAME);
        try {
            for(uint i = 0; i < column_names.size(); i++) {
                row["column_name"] = column_names[i];
                row["data_type"] = Value(column_attributes[i].get_data_type() == ColumnAttribute::INT ? "INT" : "TEXT");
                c_handles.push_back(columns.insert(&row));
            }

            // create relation
            DbRelation &table = SQLExec::tables->get_table(table_name);
            if(statement->ifNotExists)
                table.create_if_not_exists();
            else
                table.create();
        } catch(exception &e) {
            try {
                for (auto const &handle:c_handles)
                    columns.del(handle);
            }
            catch(...) {}
            throw;
        }
    } catch(exception& e) {
       try {
           SQLExec::tables->del(t_handle);
       } catch(...) {}
       throw;
    }
    return new QueryResult("created " + table_name);
}

/**
 * This method drops a table specified by the DropStatement
 * 
 * @param       statement     a statement to specify which table to be dropped
 * @returns                   a message to indicate the table is dropped successfully
 */ 
QueryResult *SQLExec::drop(const DropStatement *statement) {
    Identifier table_name = statement->name;
    if (table_name == Tables::TABLE_NAME || table_name == Columns::TABLE_NAME) {
        throw SQLExecError("can't drop a schema table");
    }

    ValueDict where;
    where["table_name"] = Value(table_name);

    // get the table to drop
    DbRelation &table = SQLExec::tables->get_table(table_name);

    // remove from _columns schema
    DbRelation &columns = SQLExec::tables->get_table(Columns::TABLE_NAME);
    Handles *handles = columns.select(&where);
    for(auto const &handle : *handles)
        columns.del(handle);
    delete handles;

    // drop the table
    table.drop();

    // remove from _tables schema
    handles = SQLExec::tables->select(&where);
    SQLExec::tables->del(*handles->begin());
    delete handles;

    return new QueryResult("dropped " + table_name); 
}

/**
 *  This method displays table or columns metdata info based on the type of ShowStatement
 * 
 * @param statement         ShowStatement which to display metadata info of columns or tables etc.
 * @returns                 QueryResult of metadata info for columns or tables etc based on table name
 */
QueryResult *SQLExec::show(const ShowStatement *statement) {
    switch(statement->type) {
        case ShowStatement::kColumns:
            return show_columns(statement);
        case ShowStatement::kTables:
            return show_tables();
        default:
            throw SQLExecError("Unsupported show type");
    }
}

/**
 * This method displays all tables metadata info excluding schema table
 * 
 * @returns         metadta info of different tables excluding schema table
 */
QueryResult *SQLExec::show_tables() {
    ColumnNames *column_names = new ColumnNames;
    ColumnAttributes *column_attributes = new ColumnAttributes;
    ValueDicts *rows = new ValueDicts;

    column_names->push_back("table_name");
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));
    
    Handles *handles = SQLExec::tables->select();
    u_long n = handles->size() - 3;

    for (const auto & handle : *handles) {
       ValueDict *row = SQLExec::tables->project(handle, column_names);
       Identifier table_name = (*row)["table_name"].s;
       if (table_name != Tables::TABLE_NAME && table_name != Columns::TABLE_NAME && table_name != Indices::TABLE_NAME) {
           rows->push_back(row);
       } else {
           delete row;
       }
    }
    delete handles;
    return new QueryResult(column_names, column_attributes, rows, "returned " + to_string(n) + " rows");
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
    ColumnNames *column_names = new ColumnNames;
    ColumnAttributes *column_attributes = new ColumnAttributes;
    ValueDict condition;
    ValueDicts *rows = new ValueDicts;

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

QueryResult *SQLExec::show_index(const ShowStatement *statement) {
    return new QueryResult("not implemented"); // FIXME
}

QueryResult *SQLExec::drop_index(const DropStatement *statement) {
    return new QueryResult("drop index not implemented");  // FIXME
}

QueryResult *SQLExec::create_index(const CreateStatement *statement) {
    return new QueryResult("create index not implemented");  // FIXME
}

//create table?


