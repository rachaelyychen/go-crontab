package test

import (
	"errors"
	"github.com/go-redis/redis"
	"github.com/magiconair/properties/assert"
	"strings"
	"sync"
	"testing"
	"time"
)

var client *redis.Client

func init() {
	client = redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
	})
}

func TestMSetNX(t *testing.T) {
	// true, nil
	val, err := client.MSetNX("k1", "v1", "k2", "v2").Result()
	assert.Equal(t, val, true)
	assert.Equal(t, err, nil)
	// false, nil
	val, err = client.MSetNX("k1", "v3").Result()
	assert.Equal(t, val, false)
	assert.Equal(t, err, nil)
}

func TestMGet(t *testing.T) {
	// ["v1", "v2"], nil
	val, err := client.MGet("k1", "k2").Result()
	assert.Equal(t, len(val), 2)
	assert.Equal(t, err, nil)
	// [nil], nil
	val, err = client.MGet("k3").Result()
	assert.Equal(t, len(val), 1)
	assert.Equal(t, err, nil)
}

func TestGet(t *testing.T) {
	// "v1", nil
	val, err := client.Get("k1").Result()
	assert.Equal(t, val, "v1")
	assert.Equal(t, err, nil)
	// redis.Nil returns when key does not exist
	// "", redis.Nil
	val1, err1 := client.Get("k3").Result()
	if err1 == redis.Nil {
		assert.Equal(t, val1, "")
		t.Logf("k3 does not exist")
	} else if err1 != nil {
		t.Fatal("failed")
	} else {

	}
}

func TestExisting(t *testing.T) {
	// 1, nil
	val, err := client.Exists("k1", "k3").Result()
	assert.Equal(t, val, int64(1))
	assert.Equal(t, err, nil)
	// 0, nil
	val, err = client.Exists("k3").Result()
	assert.Equal(t, val, int64(0))
}

func TestDel(t *testing.T) {
	// 2, nil
	val, err := client.Del("k1", "k2").Result()
	assert.Equal(t, val, int64(2))
	assert.Equal(t, err, nil)
	// 0, nil
	val, err = client.Del("k1", "k2").Result()
	assert.Equal(t, val, int64(0))
	assert.Equal(t, err, nil)
}

func TestHMSet(t *testing.T) {
	keys := []string{"a", "b", "c", "d", "e"}
	test := make(map[string]interface{})
	for _, v := range keys {
		test[v] = strings.Repeat(v, 3)
	}
	// "OK", nil
	val, err := client.HMSet("khash", test).Result()
	assert.Equal(t, val, "OK")
	assert.Equal(t, err, nil)

	// test1 is empty
	test1 := make(map[string]interface{})
	// "", ERR wrong number of arguments for 'hmset' command
	val, err = client.HMSet("khash1", test1).Result()
	assert.Equal(t, val, "")
	assert.Equal(t, err != nil, true)
}

func TestHMGet(t *testing.T) {
	// ["aaa", "bbb", "ccc"], nil
	val, err := client.HMGet("khash", "a", "b", "c").Result()
	assert.Equal(t, len(val), 3)
	assert.Equal(t, err, nil)
	// field z not exists in khash
	// [nil], nil
	val, err = client.HMGet("khash", "z").Result()
	assert.Equal(t, len(val), 1)
	assert.Equal(t, err, nil)
	// key khash1 not exists in rds
	// [nil], nil
	val, err = client.HMGet("khash1", "z").Result()
	assert.Equal(t, len(val), 1)
	assert.Equal(t, err, nil)
}

func TestHDel(t *testing.T) {
	// 3, nil
	val, err := client.HDel("khash", "a", "b", "c").Result()
	assert.Equal(t, val, int64(3))
	assert.Equal(t, err, nil)
	// field z not exists in khash
	// 0, nil
	val, err = client.HDel("khash", "z").Result()
	assert.Equal(t, val, int64(0))
	assert.Equal(t, err, nil)
	// key khash1 not exists in rds
	// 0, nil
	val, err = client.HDel("khash1", "z").Result()
	assert.Equal(t, val, int64(0))
	assert.Equal(t, err, nil)
}

func TestHExists(t *testing.T) {
	// true, nil
	val, err := client.HExists("khash", "d").Result()
	assert.Equal(t, val, true)
	assert.Equal(t, err, nil)
	// field z not exists in khash
	// false, nil
	val, err = client.HExists("khash", "z").Result()
	assert.Equal(t, val, false)
	assert.Equal(t, err, nil)
	// key khash1 not exists in rds
	// false, nil
	val, err = client.HExists("khash1", "z").Result()
	assert.Equal(t, val, false)
	assert.Equal(t, err, nil)
}

func TestRpush(t *testing.T) {
	val, err := client.RPush("klist", "v1").Result()
	assert.Equal(t, val, int64(1))
	assert.Equal(t, err, nil)
}

func TestLpop(t *testing.T) {
	// "v1", nil
	val, err := client.LPop("klist").Result()
	assert.Equal(t, val, "v1")
	assert.Equal(t, err, nil)
	// empty list
	// "", redis.Nil
	val, err = client.LPop("klist").Result()
	assert.Equal(t, val, "")
	assert.Equal(t, err, redis.Nil)
	// key klist1 not exists in rds
	// "", redis.Nil
	val, err = client.LPop("klist1").Result()
	assert.Equal(t, val, "")
	assert.Equal(t, err, redis.Nil)
}

func TestPipeline(t *testing.T) {
	// multi commands exec in one round trip
	pipe := client.Pipeline()
	incr := pipe.Incr("tx_pipeline_counter")
	pipe.Expire("tx_pipeline_counter", time.Hour)
	_, err := pipe.Exec()
	assert.Equal(t, incr.Val(), int64(1))
	assert.Equal(t, err, nil)
}

func TestTx(t *testing.T) {
	const routineCount = 100
	// Transactionally increments key using GET and SET commands.
	increment := func(key string) error {
		txf := func(tx *redis.Tx) error {
			// get current value or zero
			n, err := tx.Get(key).Int()
			if err != nil && err != redis.Nil {
				return err
			}

			// actual operation (local in optimistic lock)
			n++

			// runs only if the watched keys remain unchanged
			_, err = tx.Pipelined(func(pipe redis.Pipeliner) error {
				// pipe handles the error case
				pipe.Set(key, n, 0)
				return nil
			})
			return err
		}

		for retries := routineCount; retries > 0; retries-- {
			err := client.Watch(txf, key)
			if err != redis.TxFailedErr {
				return err
			}
			// optimistic lock lost
		}
		return errors.New("increment reached maximum number of retries")
	}

	var wg sync.WaitGroup
	wg.Add(routineCount)
	for i := 0; i < routineCount; i++ {
		go func() {
			defer wg.Done()
			if err := increment("counter3"); err != nil {
				t.Log("increment error:", err)
			}
		}()
	}
	wg.Wait()
	n, err := client.Get("counter3").Int()
	t.Log("ended with", n, err)
}
