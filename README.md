# Real-Time-Data-Synchronization-and-Performance-Benchmarking-Between-PostgreSQL-and-CrateDB
Set up CrateDB with 5 related tables and loaded ~100K records each. Imported a PostgreSQL snapshot into CrateDB to compare query performance. Implemented real-time data sync from PostgreSQL to CrateDB to maintain consistency, enabling fast analytics on live transactional data.
# CrateDB Database Creation
Install and configure a new CrateDB instance.
Define 5 tables within CrateDB, ensuring they are linked via appropriate foreign key relationships.
Load each table with approximately 100,000 records.
# PostgreSQL Snapshot & Performance Testing
Take a snapshot of your existing PostgreSQL database and import it into CrateDB.
Run identical queries on both PostgreSQL and CrateDB versions, and demonstrate that CrateDB executes them faster.
# Data Synchronization
Make updates to the tables in PostgreSQL.
Ensure that those changes are replicated or propagated to the CrateDB instance to keep data in sync.
https://cratedb.com/ Use of this 
# I have use of Docker
docker run -d --name postgresql_new -e POSTGRES_PASSWORD=MyStrongP@ssw0rd! -p 5434:5432 -v pg_data_new:/var/lib/postgresql/data postgres:latest
docker run -d --name cratedb_new -p 4201:4200 -p 5435:5432 -e CRATE_HEAP_SIZE=2g -v cratedb_data_new:/data crate/crate:latest
This is for to establish a localHost on CrateDb and PSQL
Full Link is here
[Video_link] https://drive.google.com/file/d/1dbD8imjt-fe6h1_986uue3FWm7OHc9LV/view
