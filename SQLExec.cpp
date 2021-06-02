/**
 * Hyena: Sprint Invierno
 * @file SQLExec.cpp - implementation of SQLExec class
 * @author Kevin Lundeen
 * @author Ben Gruher
 * @author Jara Lindsay
 * @see "Seattle University, CPSC5300, Spring 2021"
 */
#include "SQLExec.h"
#include "EvalPlan.h"

using namespace std;
using namespace hsql;

// define static data
Tables *SQLExec::tables = nullptr;
Indices *SQLExec::indices = nullptr;

/**
 * Make query results printable
 * @param out   Stream
 * @param qres  Const QueryResult
 * @return out  Stream
 */
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
                    case ColumnAttribute::BOOLEAN:
                        out << (value.n == 0 ? "false" : "true");
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

/**
 * Recursive function to pull apart parse tree and pull out equality predicates and AND operators
 * @param expression    Statement expr
 * @return ValueDict    Return the where conjunction
 */
ValueDict *get_where_conjunction(const hsql::Expr *expr, ColumnNames *columnNames) {

    if (expr->type != kExprOperator) {
        throw DbRelationError("unknown operator");
    }
    if (expr->opType != Expr::AND && expr->opType != Expr::SIMPLE_OP) {
        throw DbRelationError("only support AND conjunctions");
    }

    ValueDict *rows = new ValueDict;

    if (expr->opType == Expr::AND) {
        // call get where conjunction recursively for AND
        ValueDict *recursive_row = get_where_conjunction(expr->expr, columnNames);

        if (!recursive_row->empty()) {
            rows->insert(recursive_row->begin(), recursive_row->end());
        }
        recursive_row = get_where_conjunction(expr->expr2, columnNames);
        rows->insert(recursive_row->begin(), recursive_row->end());
    }

    if (expr->opType == Expr::SIMPLE_OP) {
        if (expr->opChar != '=') {
            throw DbRelationError("only equality predicates currently supported");
        }
        Identifier column_name = expr->expr->name;
        if (find(columnNames->begin(), columnNames->end(), column_name) == columnNames->end()) {
            throw DbRelationError("unknown column '" + column_name + "'");
        }
        if (expr->expr2->type != kExprLiteralInt && expr->expr2->type != kExprLiteralString) {
            throw DbRelationError("don't know how to handle given data type in WHERE clause");
        }
        if (expr->expr2->type == kExprLiteralInt) {
            pair <Identifier, Value> value(column_name, Value(expr->expr2->ival));
            rows->insert(value);
        } else {
            pair <Identifier, Value> value(column_name, Value(expr->expr2->name));
            rows->insert(value);
        }
    }
    return rows;
}

/**
 * Destructor
 */
QueryResult::~QueryResult() {
    if (column_names != nullptr)
        delete column_names;
    if (column_attributes != nullptr)
        delete column_attributes;
    if (rows != nullptr) {
        for (auto row: *rows)
            delete row;
        delete rows;
    }
}

/**
 * Executes the provded SQL statement
 * @param statement     The SQL statement to execute
 * @return QueryResult  The result of the executed statement
 */
QueryResult *SQLExec::execute(const SQLStatement *statement) {
    // initialize _tables table, if not yet present
    if (SQLExec::tables == nullptr) {
        SQLExec::tables = new Tables();
        SQLExec::indices = new Indices();
    }

    try {
        switch (statement->type()) {
            case kStmtCreate:
                return create((const CreateStatement *) statement);
            case kStmtDrop:
                return drop((const DropStatement *) statement);
            case kStmtShow:
                return show((const ShowStatement *) statement);
            case kStmtInsert:
                return insert((const InsertStatement *) statement);
            case kStmtDelete:
                return del((const DeleteStatement *) statement);
            case kStmtSelect:
                return select((const SelectStatement *) statement);
            default:
                return new QueryResult("not implemented");
        }
    } catch (DbRelationError &e) {
        throw SQLExecError(string("DbRelationError: ") + e.what());
    }
}

/**
 * Insert the given statement into the table
 * @param statement     The statement to insert
 * @return QueryResult  The result of the Insert query
 */
QueryResult *SQLExec::insert(const InsertStatement *statement) {
    // construct ValueDict
    char *tableName = statement->tableName;
    DbRelation &table = SQLExec::tables->get_table(tableName);
    ValueDict row;

    // check that values/columns provided match the columns in the table
    const ColumnNames &const_table_columns = table.get_column_names();
    ColumnNames table_columns = const_table_columns;
    const ColumnAttributes &const_table_attr = table.get_column_attributes();
    ColumnAttributes table_attr = const_table_attr;
    if (table_columns.size() != statement->values->size()) {
        throw SQLExecError("DbRelationError: don't know how to handle NULLs, defaults, etc. yet");
    }

    // if column names are provided, map column names to values in order
    if (statement->columns != NULL) {
        for (size_t i = 0; i < statement->columns->size(); i++) {
            char *columnName = statement->columns->at(i);

            // find location in column names vector in order to lookup datatype in column attributes
            int index;
            auto it = find(table_columns.begin(), table_columns.end(), columnName);
            if (it != table_columns.end())
                index = it - table_columns.begin();
            else
                throw SQLExecError("Invalid column name");

            // use index to lookup datatype in column attributes, if INT, then look at statement->ival, if TEXT, then look at statement->name
            if (table_attr[index].get_data_type() == ColumnAttribute::INT)
                row[columnName] = Value(statement->values->at(i)->ival);
            else
                row[columnName] = Value(statement->values->at(i)->name);
        }
    }
        // columns not provided - add to columns in table order
    else {
        for (size_t i = 0; i < table_columns.size(); i++) {
            if (table_attr[i].get_data_type() == ColumnAttribute::INT)
                row[table_columns.at(i)] = Value(statement->values->at(i)->ival);
            else
                row[table_columns.at(i)] = Value(statement->values->at(i)->name);
        }
    }

    // insert ValueDict into table
    Handle insertHandle = table.insert(&row);

    // add to any indices
    IndexNames indices = SQLExec::indices->get_index_names(tableName);
    int numIndices = 0;
    for (Identifier indexName: indices) {
        numIndices++;
        DbIndex &index = SQLExec::indices->get_index(tableName, indexName);
        index.insert(insertHandle);
    }

    string output =
            string("successfully inserted 1 row into ") + string(tableName) + string(" and ") + to_string(numIndices) +
            string(" indices");
    return new QueryResult(output);
}

/**
 * Delete the given value from the table
 * @param statement     The value/s to delete
 * @return QueryResult  The result of the Delete query
 */
QueryResult *SQLExec::del(const DeleteStatement *statement) {
    char *table_name = statement->tableName;
    DbRelation &table = SQLExec::tables->get_table(table_name);
    EvalPlan *plan = new EvalPlan(table);

    ColumnNames column_names;
    for (auto const columns: table.get_column_names()) {
        column_names.push_back(columns);
    }

    if (statement->expr != nullptr) {
        plan = new EvalPlan(get_where_conjunction(statement->expr, &column_names), plan);
    }

    EvalPlan *optimized = plan->optimize();
    EvalPipeline pipeline = optimized->pipeline();

    //delete handles
    auto index_names = SQLExec::indices->get_index_names(table_name);
    Handles *handles = pipeline.second;
    u_long n = handles->size();
    u_long i = index_names.size();

    //iterate through and delete from indices
    for (auto const &handle: *handles) {
        for (u_long j = 0; j < i; j++) {
            DbIndex &indices = SQLExec::indices->get_index(table_name, index_names.at(j));
            indices.del(handle);
        }
    }

    //delete from table
    for (auto const &handle: *handles) {
        table.del(handle);
    }
    delete handles;

    string output =
            "successfully deleted " + to_string(n) + " rows from " + string(table_name) + " and " + to_string(i) +
            " indices";
    return new QueryResult(output);
}

/**
 * Select teh given value/s from the table
 * @param statement     The value/s to select
 * @return QueryResult  The result of the Select query
 */
QueryResult *SQLExec::select(const SelectStatement *statement) {

    Identifier tableName = statement->fromTable->name;
    DbRelation &table = SQLExec::tables->get_table(tableName);
    EvalPlan *plan = new EvalPlan(table);

    ColumnNames column_names;
    for (auto const columns: table.get_column_names()) {
        column_names.push_back(columns);
    }

    if (statement->whereClause != nullptr) {
        plan = new EvalPlan(get_where_conjunction(statement->whereClause, &column_names), plan);
    }

    // Wrap the whole thing in a ProjectAll or a Project
    ColumnAttributes *columnAttributes = new ColumnAttributes;
    ColumnNames *columnNames = new ColumnNames;

    if (statement->selectList->front()->type == kExprStar) {
        *columnNames = table.get_column_names();
        *columnAttributes = table.get_column_attributes();
        plan = new EvalPlan(EvalPlan::ProjectAll, plan);
    } else {

        for (auto const &columns : *statement->selectList) {
            columnNames->push_back(columns->name);
        }
        *columnAttributes = table.get_column_attributes();
        plan = new EvalPlan(columnNames, plan);
    }

    EvalPlan *optimized = plan->optimize();
    ValueDicts *rows = optimized->evaluate();
    u_long n = rows->size();
    string output = "successfully returned " + to_string(n) + " rows";
    return new QueryResult(columnNames, columnAttributes, rows, output);
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
 * Create the given table or index
 * @param statement     The table or index to create
 * @return QueryResult  The result of the Create query
 */
QueryResult *SQLExec::create(const CreateStatement *statement) {
    switch (statement->type) {
        case CreateStatement::kTable:
            return create_table(statement);
        case CreateStatement::kIndex:
            return create_index(statement);
        default:
            return new QueryResult("Only CREATE TABLE and CREATE INDEX are implemented");
    }
}

/**
 * Create the given table
 * @param statement     The table to create
 * @return QueryResult  The result of the Create Table query
 */
QueryResult *SQLExec::create_table(const CreateStatement *statement) {
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

    // Add to schema: _tables and _columns
    ValueDict row;
    row["table_name"] = table_name;
    Handle t_handle = SQLExec::tables->insert(&row);  // Insert into _tables
    try {
        Handles c_handles;
        DbRelation &columns = SQLExec::tables->get_table(Columns::TABLE_NAME);
        try {
            for (uint i = 0; i < column_names.size(); i++) {
                row["column_name"] = column_names[i];
                row["data_type"] = Value(column_attributes[i].get_data_type() == ColumnAttribute::INT ? "INT" : "TEXT");
                c_handles.push_back(columns.insert(&row));  // Insert into _columns
            }

            // Finally, actually create the relation
            DbRelation &table = SQLExec::tables->get_table(table_name);
            if (statement->ifNotExists)
                table.create_if_not_exists();
            else
                table.create();

        } catch (...) {
            // attempt to remove from _columns
            try {
                for (auto const &handle: c_handles)
                    columns.del(handle);
            } catch (...) {}
            throw;
        }

    } catch (exception &e) {
        try {
            // attempt to remove from _tables
            SQLExec::tables->del(t_handle);
        } catch (...) {}
        throw;
    }
    return new QueryResult("created " + table_name);
}

/**
 * Create an index on the given table
 * @param statement     The index to create
 * @return QueryResult  The result of the Create Index query
 */
QueryResult *SQLExec::create_index(const CreateStatement *statement) {
    Identifier index_name = statement->indexName;
    Identifier table_name = statement->tableName;

    // get underlying relation
    DbRelation &table = SQLExec::tables->get_table(table_name);

    // check that given columns exist in table
    const ColumnNames &table_columns = table.get_column_names();
    for (auto const &col_name: *statement->indexColumns)
        if (find(table_columns.begin(), table_columns.end(), col_name) == table_columns.end())
            throw SQLExecError(string("Column '") + col_name + "' does not exist in " + table_name);

    // insert a row for every column in index into _indices
    ValueDict row;
    row["table_name"] = Value(table_name);
    row["index_name"] = Value(index_name);
    row["index_type"] = Value(statement->indexType);
    row["is_unique"] = Value(string(statement->indexType) == "BTREE"); // assume HASH is non-unique --
    int seq = 0;
    Handles i_handles;
    try {
        for (auto const &col_name: *statement->indexColumns) {
            row["seq_in_index"] = Value(++seq);
            row["column_name"] = Value(col_name);
            i_handles.push_back(SQLExec::indices->insert(&row));
        }

        DbIndex &index = SQLExec::indices->get_index(table_name, index_name);
        index.create();

    } catch (...) {
        // attempt to remove from _indices
        try {  // if any exception happens in the reversal below, we still want to re-throw the original ex
            for (auto const &handle: i_handles)
                SQLExec::indices->del(handle);
        } catch (...) {}
        throw;  // re-throw the original exception (which should give the client some clue as to why it did
    }
    return new QueryResult("created index " + index_name);
}

/**
 * Drop the given table or index
 * @param statement     The table or index to drop
 * @return QueryResult  The result of the Drop query
 */
QueryResult *SQLExec::drop(const DropStatement *statement) {
    switch (statement->type) {
        case DropStatement::kTable:
            return drop_table(statement);
        case DropStatement::kIndex:
            return drop_index(statement);
        default:
            return new QueryResult("Only DROP TABLE and CREATE INDEX are implemented");
    }
}

/**
 * Drop the given table
 * @param statement     The table to drop
 * @return QueryResult  The result of the Drop Table query
 */
QueryResult *SQLExec::drop_table(const DropStatement *statement) {
    Identifier table_name = statement->name;
    if (table_name == Tables::TABLE_NAME || table_name == Columns::TABLE_NAME)
        throw SQLExecError("cannot drop a schema table");

    ValueDict where;
    where["table_name"] = Value(table_name);

    // get the table
    DbRelation &table = SQLExec::tables->get_table(table_name);

    // remove any indices
    for (auto const &index_name: SQLExec::indices->get_index_names(table_name)) {
        DbIndex &index = SQLExec::indices->get_index(table_name, index_name);
        index.drop();  // drop the index
    }
    Handles *handles = SQLExec::indices->select(&where);
    for (auto const &handle: *handles)
        SQLExec::indices->del(handle);  // remove all rows from _indices for each index on this table
    delete handles;

    // remove from _columns schema
    DbRelation &columns = SQLExec::tables->get_table(Columns::TABLE_NAME);
    handles = columns.select(&where);
    for (auto const &handle: *handles)
        columns.del(handle);
    delete handles;

    // remove table
    table.drop();

    // finally, remove from _tables schema
    handles = SQLExec::tables->select(&where);
    SQLExec::tables->del(*handles->begin()); // expect only one row from select
    delete handles;

    return new QueryResult(string("dropped ") + table_name);
}

/**
 * Drop the given index
 * @param statement     The index to drop
 * @return QueryResult  The result of the Drop Index query
 */
QueryResult *SQLExec::drop_index(const DropStatement *statement) {
    Identifier table_name = statement->name;
    Identifier index_name = statement->indexName;

    // drop index
    DbIndex &index = SQLExec::indices->get_index(table_name, index_name);
    index.drop();

    // remove rows from _indices for this index
    ValueDict where;
    where["table_name"] = Value(table_name);
    where["index_name"] = Value(index_name);
    Handles *handles = SQLExec::indices->select(&where);
    for (auto const &handle: *handles)
        SQLExec::indices->del(handle);
    delete handles;

    return new QueryResult("dropped index " + index_name);
}

/**
 * Show the given Table, Column, or Index
 * @param statement     The Table, Column, or Index to show
 * @return QueryResult  The result of the Show query
 */
QueryResult *SQLExec::show(const ShowStatement *statement) {
    switch (statement->type) {
        case ShowStatement::kTables:
            return show_tables();
        case ShowStatement::kColumns:
            return show_columns(statement);
        case ShowStatement::kIndex:
            return show_index(statement);
        default:
            throw SQLExecError("unrecognized SHOW type");
    }
}

/**
 * Show the given index
 * @param statement     The index to show
 * @return QueryResult  The result of the Show Index query
 */
QueryResult *SQLExec::show_index(const ShowStatement *statement) {
    ColumnNames *column_names = new ColumnNames;
    ColumnAttributes *column_attributes = new ColumnAttributes;
    column_names->push_back("table_name");
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

    column_names->push_back("index_name");
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

    column_names->push_back("column_name");
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

    column_names->push_back("seq_in_index");
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::INT));

    column_names->push_back("index_type");
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

    column_names->push_back("is_unique");
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::BOOLEAN));

    ValueDict where;
    where["table_name"] = Value(string(statement->tableName));
    Handles *handles = SQLExec::indices->select(&where);
    u_long n = handles->size();

    ValueDicts *rows = new ValueDicts;
    for (auto const &handle: *handles) {
        ValueDict *row = SQLExec::indices->project(handle, column_names);
        rows->push_back(row);
    }
    delete handles;
    return new QueryResult(column_names, column_attributes, rows,
                           "successfully returned " + to_string(n) + " rows");
}

/**
 * Show the given table
 * @return QueryResult  The result of the Show Table query
 */
QueryResult *SQLExec::show_tables() {
    ColumnNames *column_names = new ColumnNames;
    column_names->push_back("table_name");

    ColumnAttributes *column_attributes = new ColumnAttributes;
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

    Handles *handles = SQLExec::tables->select();
    u_long n = handles->size() - 3;

    ValueDicts *rows = new ValueDicts;
    for (auto const &handle: *handles) {
        ValueDict *row = SQLExec::tables->project(handle, column_names);
        Identifier table_name = row->at("table_name").s;
        if (table_name != Tables::TABLE_NAME && table_name != Columns::TABLE_NAME && table_name != Indices::TABLE_NAME)
            rows->push_back(row);
        else
            delete row;
    }
    delete handles;
    return new QueryResult(column_names, column_attributes, rows, "successfully returned " + to_string(n) + " rows");
}

/**
 * Show columns from the given table
 * @param statement     The table columns to show
 * @return QueryResult  The result of the Show Columns query
 */
QueryResult *SQLExec::show_columns(const ShowStatement *statement) {
    DbRelation &columns = SQLExec::tables->get_table(Columns::TABLE_NAME);

    ColumnNames *column_names = new ColumnNames;
    column_names->push_back("table_name");
    column_names->push_back("column_name");
    column_names->push_back("data_type");

    ColumnAttributes *column_attributes = new ColumnAttributes;
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

    ValueDict where;
    where["table_name"] = Value(statement->tableName);
    Handles *handles = columns.select(&where);
    u_long n = handles->size();

    ValueDicts *rows = new ValueDicts;
    for (auto const &handle: *handles) {
        ValueDict *row = columns.project(handle, column_names);
        rows->push_back(row);
    }
    delete handles;
    return new QueryResult(column_names, column_attributes, rows, "successfully returned " + to_string(n) + " rows");
}

