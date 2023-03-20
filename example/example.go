package main

import (
	"fmt"
	"time"

	"github.com/go-redis/redis"
)

func main() {
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	start := time.Now()

	for i := 0; i < 100000; i += 1 {
		key := fmt.Sprintf("key%+v", i)
		_, _ = rdb.Set(key, key, time.Duration(0)).Result()
	}

	for i := 0; i < 100000; i += 1 {
		key := fmt.Sprintf("key%+v", i)
		val, err := rdb.Get(key).Result()

		if val != key {
			fmt.Println(key, val, err)
			panic(i)
		}
	}

	fmt.Println(time.Since(start))

	rdb.Close()
}
