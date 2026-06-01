package main

import (
	"dkvs/config"
	"dkvs/db"
	"dkvs/web"
	"flag"
	"log"
	"net/http"

	"github.com/BurntSushi/toml"
)

// importovati moje package
var (
	dbLocation = flag.String("db-location", "", "The path to database")
	httpAddr   = flag.String("http-addr", "127.0.0.1:8080", "HTTP host and port to listen on")
	configFile = flag.String("config-file", "sharding.toml", "Config file for static sharding")
	shardName  = flag.String("shard-name", "", "Shard name for this instance (required if config-file is provided)")
)

func parseFlags() {
	flag.Parse()

	if *dbLocation == "" {
		log.Fatal("db-location flag is required")
	}

	if *shardName != "" && *configFile == "" {
		log.Fatal("config-file flag is required when shard-name is provided")
	}
}

func main() {
	parseFlags()

	var c config.Config
	if _, err := toml.DecodeFile(*configFile, &c); err != nil {
		log.Fatalf("toml.DecodeFile(%q): %v", *configFile, err)
	}

	var shardCount int
	var shardIdx int = -1
	var addrs = make(map[int]string)

	for _, s := range c.Shards {
		addrs[s.Idx] = s.Address

		if s.Idx+1 > shardCount {
			shardCount = s.Idx + 1
		}
		if s.Name == *shardName {
			shardIdx = s.Idx
		}
	}

	if shardIdx < 0 {
		log.Fatalf("Shard %q was not found", *shardName)
	}
	log.Printf("Shard count is %d, current shard: %d", shardCount, shardIdx)

	db, close, err := db.NewDatabase(*dbLocation)
	if err != nil {
		log.Fatalf("NewDatabase(%q): %v", *dbLocation, err)
	}
	defer close()

	srv := web.NewServer(db, shardIdx, shardCount, addrs)

	http.HandleFunc("/get", srv.GetHandler)
	http.HandleFunc("/set", srv.SetHandler)

	log.Fatal(http.ListenAndServe(*httpAddr, nil))
}

//go run main.go -db-location my.db sharding.toml
//go run main.go -db-location my.db sharding.toml Belgrade
//go run main.go -db-location my.db -config-file sharding.toml -shard-name Belgrade
// curl "http://localhost:8080/set?key=foo&value=bar"
