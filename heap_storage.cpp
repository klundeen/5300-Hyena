// Authors: Dhruv Patel
// Course: CPSC5300, Seattle University, WQ'24

#include "heap_storage.h"
#include <cstring>
#include <map>
#include <vector>

// test function -- returns true if all tests pass
bool test_heap_storage() {
	ColumnNames column_names;
	column_names.push_back("a");
	column_names.push_back("b");
	ColumnAttributes column_attributes;
	ColumnAttribute ca(ColumnAttribute::INT);
	column_attributes.push_back(ca);
	ca.set_data_type(ColumnAttribute::TEXT);
	column_attributes.push_back(ca);
    //    std::cout<< "Before HeapTable" << std::endl;
    HeapTable table1("_test_create_drop_cpp", column_names, column_attributes);
    table1.create();
    std::cout << "create ok" << std::endl;
    table1.drop();  // drop makes the object unusable because of BerkeleyDB restriction -- maybe want to fix this some day
    std::cout << "drop ok" << std::endl;

    HeapTable table("_test_data_cpp", column_names, column_attributes);
    table.create_if_not_exists();
    std::cout << "create_if_not_exsts ok" << std::endl;

    ValueDict row;
    row["a"] = Value(12);
    row["b"] = Value("Hello!");
    std::cout << "try insert" << std::endl;
    table.insert(&row);
    std::cout << "insert ok" << std::endl;
    Handles* handles = table.select();
    std::cout << "select ok " << handles->size() << std::endl;
    ValueDict *result = table.project((*handles)[0]);
    std::cout << "project ok" << std::endl;
    Value value = (*result)["a"];
    if (value.n != 12)
    	return false;
    value = (*result)["b"];
    if (value.s != "Hello!")
		return false;
    table.drop();

    return true;
}

SlottedPage::SlottedPage(Dbt &block, BlockID block_id, bool is_new): DbBlock(block, block_id, is_new)
{
  std::cout << "a";
  if (is_new) {
    this->num_records = 0;
    this->end_free = DbBlock::BLOCK_SZ - 1;
    put_header();
  }
  else {
    get_header(this->num_records, this->end_free);
  }

}

RecordID SlottedPage::add(const Dbt *data) {
  if (!has_room(data->get_size())){
    throw DbBlockNoRoomError("Not enough room in block");
  }
  this->num_records += 1;
  u_int16_t record_id = this->num_records;
  u_int16_t size = data->get_size();
  this->end_free -= size;
  u_int16_t loc = this->end_free + 1;
  put_header();
  put_header(record_id, size, loc);
  memcpy(this->address(loc), data->get_data(), size);
  return record_id;
}


Dbt* SlottedPage::get(RecordID record_id) {
  u_int16_t size;
  u_int16_t loc;

  get_header(size, loc, record_id);

  if (loc == 0){
    return NULL;
  }
  
  return new Dbt(this->address(loc), size);

}

void SlottedPage::put(RecordID record_id, const Dbt &data){
  
  u_int16_t size;
  u_int16_t loc;
  u_int16_t new_size = data.get_size();
  u_int16_t extra;
  
  get_header(size, loc, record_id);

  if(new_size > size){
    extra = new_size - size;
    if(!has_room(extra)){
      throw DbBlockNoRoomError("Not enough room in block");
    }
    
    slide(loc, loc - extra);
    memcpy(this->address(loc - extra), data.get_data(), size);
  }
  else{
    memcpy(this->address(loc), data.get_data(), new_size);
    slide(loc + new_size, loc + size);
  }

  get_header(size, loc, record_id);
  put_header(record_id, new_size, loc);
}

void SlottedPage::del(RecordID record_id){
  u_int16_t size;
  u_int16_t loc;

  get_header(size, loc, record_id);
  put_header(record_id, 0, 0);
  slide(loc, loc + size);

}

RecordIDs* SlottedPage::ids(void){
  RecordIDs *id(0);
  u_int16_t size;
  u_int16_t loc;
  
  for(u_int16_t i = 1; i < this->num_records + 1; i++){
    get_header(size, loc, i);
    
    if(loc != 0){
      id->push_back(i);
    }
    
  }

  return id;
}

void SlottedPage::get_header(u_int16_t &size, u_int16_t &loc, RecordID record_id){
  size = get_n(4 * record_id);
  loc = get_n(4 * record_id + 2);
}

void SlottedPage::put_header(RecordID id, u_int16_t size, u_int16_t loc) {
  if (id == 0) {
    size = this->num_records;
    loc = this->end_free;
  }
  put_n(4*id, size);
  put_n(4*id + 2, loc);

}

bool SlottedPage::has_room(u_int16_t size){

  u_int16_t avalaible = (this->end_free - (this->num_records + 2)*4);

  return size <= avalaible;
}

void SlottedPage::slide(u_int16_t start, u_int16_t end){
  u_int16_t shifted = end - start;

  if(shifted == 0){
    return;
  }



  memcpy(this->address(this->end_free + 1 + shifted), this->address(end_free + 1), start - (end_free + 1));
  
  for(u_int16_t i = 0; i < this->ids()->size(); i++){
    u_int16_t size;
    u_int16_t loc;

    get_header(size, loc, i);

    if(loc <= start){
      loc += start;
      put_header(i, size, loc);
    }
  }
    this->end_free += shifted;
    put_header();
}



u_int16_t SlottedPage::get_n(u_int16_t offset) {
    return *(u_int16_t*)this->address(offset);
}


void SlottedPage::put_n(u_int16_t offset, u_int16_t n) {
    *(u_int16_t*)this->address(offset) = n;
}


void* SlottedPage::address(u_int16_t offset) {
    return (void*)((char*)this->block.get_data() + offset);
}

// HEAP FILE code

void HeapFile::create(void) {
  this-> db_open(DB_CREATE);
  SlottedPage *block = this->get_new();
  this->put(block);
}

void HeapFile::drop(void){
  open();
  close();
  remove(this->dbfilename.c_str());
}

void HeapFile::open(void) {
  db_open(DB_CREATE);
}

void HeapFile::close(void) {
  if (!closed) {
    db.close(0);
    closed = true;
  }
}

SlottedPage* HeapFile::get_new(void) {
  char block[DbBlock::BLOCK_SZ];
  std::memset(block, 0, sizeof(block));
  Dbt data(block, sizeof(block));

  int block_id = ++this->last;
  Dbt key(&block_id, sizeof(block_id));

  SlottedPage* page = new SlottedPage(data, this->last, true);
  std::cout<<"after slot";
  this->db.put(nullptr, &key, &data, 0);
  std::cout<<"between";
  this->db.get(nullptr, &key, &data, 0);
  return page;
}

SlottedPage* HeapFile::get(BlockID block_id) {
  Dbt key(&block_id, sizeof(block_id));
  Dbt data;
  this->db.get(nullptr, &key, &data, 0);
  return new SlottedPage(data, block_id, false);
}

void HeapFile::put(DbBlock *block) {
  std::cout<<"put success" << std::endl;
  BlockID block_id  = block->get_block_id();
  Dbt key(&block_id, sizeof(block_id));
  std::cout<<"put success" << std::endl;
  this->db.put(nullptr, &key, block->get_block(), 0);
}

BlockIDs* HeapFile::block_ids() {
  BlockIDs* block_id(0);
  for (BlockID i = 1; i < (BlockID)this->last+1; i++) {
    block_id->push_back(i);
  }
  return block_id;
}

void HeapFile::db_open(uint flags) {
  if (this->closed == false) {
    return;
  }

    this->db.set_re_len(DbBlock::BLOCK_SZ);
    this->dbfilename = this->name + ".db";
    this->db.open(nullptr, this->dbfilename.c_str(), nullptr, DB_RECNO, flags, 0644);
    DB_BTREE_STAT *stat_type;
    this->db.stat(nullptr, &stat_type, DB_FAST_STAT);
    this->last = stat_type->bt_ndata;
    this->closed = false;
}



// HEAP TABLE code


HeapTable::HeapTable(Identifier table_name, ColumnNames column_names, ColumnAttributes column_attributes): DbRelation(table_name, column_names, column_attributes), file(table_name)
{}

void HeapTable::create(){
  
  this->file.create();
  
}

void HeapTable::create_if_not_exists(){
  
  try
    {
      this->open();
    }
  catch(DbRelationError const&)
    {
      this->create();
    }
}


void HeapTable::drop(){
  
  this->file.drop();
}


void HeapTable::open(){
  
  this->file.open();
}

void HeapTable::close(){
  
  this->file.close();
}

Handle HeapTable::insert(const ValueDict *row){
  std::cout<<"insert open" << std::endl;
  this->open();
  return this->append(this->validate(row));
  std::cout<<"insert after" << std::endl;
}

void HeapTable::update(const Handle handle, const ValueDict *new_values){
}

void HeapTable::del(const Handle handle){
}

Handles* HeapTable::select(){

  Handles* handles = new Handles();

  return handles;
}

Handles* HeapTable::select(const ValueDict *where){
  
  Handles* handles = new Handles();
  BlockIDs* block_ids = file.block_ids();
  for (auto const& block_id: *block_ids) {
    SlottedPage* block = file.get(block_id);
    RecordIDs* record_ids = block->ids();
    for (auto const& record_id: *record_ids)
      handles->push_back(Handle(block_id, record_id));
    delete record_ids;
    delete block;
  }
  delete block_ids;
  return handles;  
  
}

ValueDict* HeapTable::project(Handle handle){

  ValueDict *v_Dict = new ValueDict();

  return v_Dict ;
}

ValueDict* HeapTable::project(Handle handle, const ColumnNames *column_names){

  ValueDict *v_Dict = new ValueDict();

  return v_Dict ;
}


ValueDict* HeapTable::validate(const ValueDict *row){
  ValueDict *full_row = new ValueDict();

  uint col_num = 0;
  for (auto const& column_name: this->column_names) {
    ColumnAttribute column_attributes = this->column_attributes[col_num++];
    ValueDict::const_iterator column = row->find(column_name);
    Value value = column->second;
    if((column_attributes.get_data_type() != ColumnAttribute::DataType::INT) &&
       (column_attributes.get_data_type() != ColumnAttribute::DataType::TEXT)){
      throw DbRelationError ("Dont know how to handle Nulls, defaults, etc. yet");
    }
    else{
      value = row->at(column_name);
    }
    
     full_row->insert(std::pair<Identifier, Value>(column_name, value));
  }
  
  return full_row;

}

Handle HeapTable::append(const ValueDict *row){
  Dbt *data = this->marshal(row);

  SlottedPage *block = this->file.get(this->file.get_last_block_id());


  u_int16_t  record_id;;
  try
    {
      record_id = block->add(data);
    }
  catch(DbRelationError const&)
    {
      block = this->file.get_new();
      record_id = block->add(data);
    }

  this->file.put(block);

  return Handle(this->file.get_last_block_id(), record_id);
}


Dbt* HeapTable::marshal(const ValueDict *row){
  
  char *bytes = new char[DbBlock::BLOCK_SZ];
  uint offset = 0;
  uint col_num = 0;
  for (auto const& column_name: this->column_names) {
    ColumnAttribute ca = this->column_attributes[col_num++];
    ValueDict::const_iterator column = row->find(column_name);
    Value value = column->second;
    if (ca.get_data_type() == ColumnAttribute::DataType::INT) {
      *(u_int32_t*) (bytes + offset) = value.n;
      offset += sizeof(int32_t);
    } else if (ca.get_data_type() == ColumnAttribute::DataType::TEXT) {
      u_int16_t size = value.s.length();
      *(u_int16_t*) (bytes + offset) = size;
      offset += sizeof(u_int16_t);
      memcpy(bytes+offset, value.s.c_str(), size); // assume ascii for now
      offset += size;
    } else {
      throw DbRelationError("Only know how to marshal INT and TEXT");
    }
  }
  char *right_size_bytes = new char[offset];
  memcpy(right_size_bytes, bytes, offset);
  delete[] bytes;
  Dbt *data = new Dbt(right_size_bytes, offset);
  return data;
}


ValueDict* HeapTable::unmarshal(Dbt *data){

  ValueDict *v_Dict = new ValueDict();

  return v_Dict ;
}