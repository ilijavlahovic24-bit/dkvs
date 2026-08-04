package main

import (
	"dkvs/config"
	"dkvs/db"
	"dkvs/replication"
	"dkvs/web"
	"flag"
	"log"
	"net/http"
)

// importovati moje package
var (
	dbLocation = flag.String("db-location", "", "The path to the bolt db database")
	httpAddr   = flag.String("http-addr", "127.0.0.1:8080", "HTTP host and port")
	configFile = flag.String("config-file", "sharding.toml", "Config file for static sharding")
	shard      = flag.String("shard", "", "The name of the shard for the data")
	replica    = flag.Bool("replica", false, "Whether or not run as a read-only replica")
)

func parseFlags() {
	flag.Parse()

	if *dbLocation == "" {
		log.Fatalf("Must provide db-location")
	}

	if *shard == "" {
		log.Fatalf("Must provide shard")
	}
}

func main() {
	parseFlags()

	c, err := config.ParseFile(*configFile)
	if err != nil {
		log.Fatalf("Error parsing config %q: %v", *configFile, err)
	}

	shards, err := config.ParseShards(c.Shards, *shard)
	if err != nil {
		log.Fatalf("Error parsing shards config: %v", err)
	}

	log.Printf("Shard count is %d, current shard: %d", shards.Count, shards.CurIdx)

	db, close, err := db.NewDatabase(*dbLocation, *replica)
	if err != nil {
		log.Fatalf("Error creating %q: %v", *dbLocation, err)
	}
	defer close()

	if *replica {
		leaderAddr, ok := shards.Addrs[shards.CurIdx]
		if !ok {
			log.Fatalf("Could not find address for leader for shard %d", shards.CurIdx)
		}
		go replication.ClientLoop(db, leaderAddr)
	}

	srv := web.NewServer(db, shards)

	http.HandleFunc("/get", srv.GetHandler)
	http.HandleFunc("/set", srv.SetHandler)
	http.HandleFunc("/purge", srv.DeleteExtraKeysHandler)
	http.HandleFunc("/next-replication-key", srv.GetNextKeyForReplication)
	http.HandleFunc("/delete-replication-key", srv.DeleteReplicationKey)
	http.HandleFunc("/metrics", srv.MetricsHandler)
	log.Fatal(http.ListenAndServe(*httpAddr, nil))
}

//go run main.go -db-location my.db sharding.toml
//go run main.go -db-location my.db sharding.toml Belgrade
//go run main.go -db-location my.db -config-file sharding.toml -shard-name Belgrade
// curl "http://localhost:8080/set?key=foo&value=bar"
