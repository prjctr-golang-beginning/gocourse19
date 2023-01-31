package main

import (
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
)

var ctx = context.Background()

func main() {
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "mycomplicatedpassword",
		DB:       0, // use default DB
	})

	err := rdb.Set(ctx, "user:name", "Maks Morozov", 0).Err()
	if err != nil {
		panic(err)
	}

	val, err := rdb.Get(ctx, "user:name").Result()
	if err != nil {
		panic(err)
	}
	fmt.Println("user:name", val)

	val2, err := rdb.Get(ctx, "user:phone-number").Result()
	if err == redis.Nil {
		fmt.Println("user:phone-number does not exist")
	} else if err != nil {
		panic(err)
	} else {
		fmt.Println("user:phone-number", val2)
	}
}
