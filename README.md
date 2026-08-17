DKVS: High Performance Distributed Key Value Store
## Status: Active Development

## What DKVS Does
DKVS is a distributed key-value store that shards data across multiple nodes using consistent hashing. 
Built from scratch in Go, with a focus on performance and observability.


### Prerequisites
- Go 1.21+
- Empty `.db` files for each shard

## Getting Started
1. Create .toml file in which there will be shards in format 
[[shards]]
name="Belgrade"
idx=<index>
address = "localhost:<adress>"
2. create empty <NameOfShard>.db file
3. create lunch.sh file
Standard command for opening a single shard is
go run main.go -db-location=belgrade.db -http-addr=127.0.0.1:8080 -config-file=sharding.toml -shard=Belgrade
4. Run:
```bash
cd backend
./launch.sh
```


## Project Structure
```DKVS/
├── frontend/       #Includes Dashboard with real-time performances
└── backend/        # includes whole logic of store
    ├──config       # operations on shards
    ├──db\          # operations on database like get and set
    ├──web\         # http handler
    ├──lsm\         # LSM storage engine
    ├──metrics\     # metrics
    └──replication  # replication
```
### Completed
-Distributed get and Set opeartions
-Distributed sharding

### In Progress
-LSM-Tree implementation - Kleppmann
-Web dashboard

### Planned
Consistency guarantees - Kleppmann


## References and Documentation
1. [Yuriy Nasretdinov - Distributed Key-Value Database in Go](https://www.youtube.com/playlist?list=PLWwSgbaBp9XrMkjEhmTIC37WX2JfwZp7I) - primary guide
2. [*Designing Data-Intensive Applications* - Martin Kleppmann] - reference for LSM-Tree and consistency
3/ [medium.com -lsm trees the complete guide to wal memtables sstables compaction bloom filters -@harshithgowdakt ]
