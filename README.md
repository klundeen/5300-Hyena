# 5300-Hyena

Adama Sanoh & Bryn Lasher
Handoff Vid: https://youtu.be/BRvzuZNm_SI (updated because the first link did not work.)

On CS1 make your directories, then clone our repository.
```
$ mkdir cpsc5300
$ cd cpsc5300
$ git clone https://github.com/klundeen/5300-Hyena.git
```

Milestone 1:
```
$ cd cpsc5300
$ cd 5300-Hyena
$ make
$ ./sql5300 ~/cpsc5300/5300-Hyena
```

This program will promt you with 'SQL>' for sql statements the parse them and prin thtem out.
To exit type 'quit'.


Milestone 2:
```
$ cd cpsc5300
$ cd 5300-Hyena
$ make
$ ./sql5300 ~/cpsc5300/5300-Hyena
```

This program will promt you with 'SQL>' for sql statements the parse them and prin thtem out.
To test heap_storage.cpp type 'test'.
To exit type 'quit'.

BERKELEY DB API Reference (db_cxx.h): https://docs.oracle.com/cd/E17076_05/html/api_reference/C/frame_main.html

Please note that in heap_storage.cpp the HeapTable class has not been implemeted for the following functions:
virtual void update(const Handle handle, const ValueDict *new_values);
virtual void del(const Handle handle);
virtual Handles *select();
virtual ValueDict *project(Handle handle);
virtual ValueDict *project(Handle handle, const ColumnNames *column_names);
Also, insert only handles two data types for now, INTEGER (or INT) and TEXT. 

While milestone 1 should run fine, milestone 2 has an error.

---

## Milestone3: Schema Storage -- Team: Tong Xu, Yinhui Li
The following statements are implemented in milestone 3
* CREATE table
* DROP table
* SHOW tables
* SHOW columns

---
## Milestone4: Indexing Setup -- Team: Tong Xu, Yinhui Li
The following statements are implemented in milestone 4
* CREATE index
* SHOW index
* DROP index

---