package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"

	"github.com/go-redis/redis/v8"
)

// Define a struct to match the structure of your JSON objects
type DataObject struct {
	SellerID    string  `json:"seller_id"`
	Geo         string  `json:"geo"`
	SecretName  string  `json:"secretName"`
	VersionName string  `json:"versionName"`
	Payload     Payload `json:"payload"`
}

type Payload struct {
	AuthCode     string `json:"auth_code"`
	MWSToken     string `json:"mws_token"`
	AuthToken    string `json:"auth_token"`
	RefreshToken string `json:"refresh_token"`
}

func main() {
	// Load the JSON data from the "data.json" file
	jsonBytes, err := ioutil.ReadFile("data/data.json")
	if err != nil {
		log.Fatalf("Error reading JSON file: %v", err)
	}

	// Unmarshal the JSON data into an array of DataObject
	var dataObjects []DataObject
	if err := json.Unmarshal(jsonBytes, &dataObjects); err != nil {
		log.Fatalf("Error unmarshaling JSON: %v", err)
	}

	// Initialize a Redis client
	rdb := redis.NewClient(&redis.Options{
		Addr: "127.0.0.1:6379", // Change this to your Redis server address
		DB:   0,
	})

	// Iterate through the data and push it to Redis
	for _, obj := range dataObjects {
		// Convert the object to JSON
		jsonStr, err := json.Marshal(obj)
		if err != nil {
			log.Printf("Error marshaling JSON: %v", err)
			continue
		}

		ctx := context.Background()

		fmt.Println(string(jsonStr))
		// Push the JSON data to Redis
		err = rdb.LPush(ctx, "sp_"+obj.Geo+"_"+obj.SellerID, jsonStr).Err()
		if err != nil {
			log.Printf("Error pushing to Redis: %v", err)
		}
	}

	// Close the Redis client
	err = rdb.Close()
	if err != nil {
		log.Fatalf("Error closing Redis client: %v", err)
	}

	fmt.Println("Data pushed to Redis successfully.")
}
