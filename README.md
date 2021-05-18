# 5300-Hyena

- Team 1: Adama Sanoh, Bryn Lasher
- Team 2: Tong Xu, Yinhui Li
- Team 3: Jara Lindsay, Ben Gruher

---

##Milestone 1: Skeleton -- Team 1

On CS1 make your directories, then clone our repository.
```
$ mkdir cpsc5300
$ cd cpsc5300
$ git clone https://github.com/klundeen/5300-Hyena.git
```

```
$ cd cpsc5300
$ cd 5300-Hyena
$ make
$ ./sql5300 ~/cpsc5300/5300-Hyena
```

This program will promt you with 'SQL>' for sql statements the parse them and prin thtem out.
To exit type 'quit'.


##Milestone 2: Rudimentary Storage Engine -- Team 1
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

- Team 1's Handoff Video: https://youtu.be/BRvzuZNm_SI (updated because the first link did not work.)

---

## Milestone 3: Schema Storage -- Team 2
The following statements are implemented in milestone 3
* CREATE table
* DROP table
* SHOW tables
* SHOW columns  


On CS1, 
```
$ cd cpsc5300
$ git clone https://github.com/klundeen/5300-Hyena.git
$ git checkout tags/Milestone3
$ make
$ ./sql5300 ~/cpsc5300/5300-Hyena
```

---
## Milestone 4: Indexing Setup -- Team 2
The following statements are implemented in milestone 4
* CREATE index
* SHOW index
* DROP index

On CS1,
```
$ cd cpsc5300
$ git clone https://github.com/klundeen/5300-Hyena.git
$ git checkout tags/Milestone4
$ make
$ ./sql5300 ~/cpsc5300/5300-Hyena
```
- Team 2's [handoff video](https://drive.google.com/file/d/1wXpLek5LobhSFXPhmmmRGCi58-gFLLfZ/view?usp=sharing) for milestone 3 and 4.

---
## Milestone 5: Insert, Delete, Simple Queries -- Team 3

On CS1,
```
$ cd cpsc5300
$ git clone https://github.com/klundeen/5300-Hyena.git
$ git checkout tags/Milestone5
$ make
$ ./sql5300 ~/cpsc5300/data
```

## Milestone 6: BTree Index -- Team 3

On CS1,
```
$ cd cpsc5300
$ git clone https://github.com/klundeen/5300-Hyena.git
$ git checkout tags/Milestone6
$ make
$ ./sql5300 ~/cpsc5300/data
```

- Team 3's Handoff Video: 