package redis

import (
	"errors"
	"fmt"
	"github.com/go-redis/redis"
	"github.com/lexkong/log"
	"github.com/spf13/viper"
	"gocrontab-worker/utils"
	"time"
)

const (
	DefaultRedisAddr         = "localhost:6379"
	DefaultRedisPassword     = ""
	DefaultRedisDialTimeout  = 5000
	DefaultRedisReadTimeout  = 3000
	DefaultRedisWriteTimeout = 3000
	// Default pool size is 10 connections per every CPU as reported by runtime.NumCPU.
)

var (
	rdsLogStr = fmt.Sprintf(utils.LOGSTR, "crontab-worker", "redis")
	client    *redis.Client
	config    *redisConfig
	RedisMgr  *RedisManager
)

type redisConfig struct {
	addr         string
	password     string
	dialTimeout  int
	readTimeout  int
	writeTimeout int
}

type RedisManager struct {
	Client *redis.Client
}

func Init() {
	config = &redisConfig{}
	initConfig()
	client = redis.NewClient(&redis.Options{
		Addr:         config.addr,
		Password:     config.password,
		DialTimeout:  time.Duration(config.dialTimeout),
		ReadTimeout:  time.Duration(config.readTimeout),
		WriteTimeout: time.Duration(config.writeTimeout),
	})

	// test connection
	pong, err := client.Ping().Result()
	if err != nil {
		log.Error(rdsLogStr, err)
		return
	}
	log.Infof("%s ping %s redis success: %s", rdsLogStr, config.addr, pong)
	log.Infof("redis connected")
	log.Infof("%s redis pool initial status: %+v", rdsLogStr, client.PoolStats())

	// set wrap process
	client.WrapProcess(func(old func(cmd redis.Cmder) error) func(cmd redis.Cmder) error {
		return func(cmd redis.Cmder) error {
			log.Debugf("%s redis starting process: %+v", rdsLogStr, cmd)
			tStart := time.Now().UnixNano() // unix nanosecond
			err := old(cmd)
			tFinish := time.Now().UnixNano()
			log.Debugf("%s redis finished process in %d ms", rdsLogStr, (tFinish-tStart)/1e6)
			return err
		}
	})
	client.WrapProcessPipeline(func(old func(cmd []redis.Cmder) error) func(cmd []redis.Cmder) error {
		return func(cmd []redis.Cmder) error {
			log.Debugf("%s redis starting pipeline process: %+v", rdsLogStr, cmd)
			tStart := time.Now().UnixNano()
			err := old(cmd)
			tFinish := time.Now().UnixNano()
			log.Debugf("%s redis finished pipeline in %d ms", rdsLogStr, (tFinish-tStart)/1e6)
			return err
		}
	})

	// single manager
	RedisMgr = &RedisManager{Client: client}
}

func initConfig() {
	config.addr = viper.GetString("redis.addr")
	config.password = viper.GetString("redis.password")
	config.dialTimeout = viper.GetInt("dial_timeout")
	config.readTimeout = viper.GetInt("read_timeout")
	config.writeTimeout = viper.GetInt("write_timeout")
}

func Close() {
	client.Close()
}

func (redisMgr *RedisManager) TxPipeProcess(pf func(pipe redis.Pipeliner) error) error {
	_, err := redisMgr.Client.TxPipelined(pf)
	return err
}

func (redisMgr *RedisManager) Exists(keys []string) (bool, error) {
	val, err := client.Exists(keys...).Result()
	if err != nil || int64(len(keys)) != val {
		return false, err
	}
	return true, nil
}

func (redisMgr *RedisManager) Delete(keys []string) (bool, error) {
	val, err := client.Del(keys...).Result()
	if err != nil || int64(len(keys)) != val {
		return false, err
	}
	return true, nil
}

// batch process
func (redisMgr *RedisManager) AddStrings(m map[string]interface{}) (bool, error) {
	args := make([]interface{}, 0, utils.BATCH)
	pipe := client.TxPipeline()
	for k, v := range m {
		if len(args) >= utils.BATCH {
			pipe.MSetNX(args...)
			args = make([]interface{}, 0, utils.BATCH)
		}
		args = append(append(args, k), v)
	}
	if len(args) > 0 {
		pipe.MSetNX(args...)
	}
	if val, err := pipe.Exec(); err != nil {
		log.Infof("%s msetnx pipeline error, see details: %+v", rdsLogStr, val)
		return false, err
	}
	return true, nil
}

func (redisMgr *RedisManager) GetStrings(keys []string) ([]interface{}, error) {
	return client.MGet(keys...).Result()
}

func (redisMgr *RedisManager) AddString(key string, value interface{}, exp time.Duration) (bool, error) {
	return client.SetNX(key, value, exp).Result()
}

func (redisMgr *RedisManager) GetString(key string) (string, error) {
	val, err := client.Get(key).Result()
	if err == redis.Nil {
		return "", errors.New(key + "does not exist")
	} else if err != nil {
		return "", err
	}
	return val, nil
}

// batch process
func (redisMgr *RedisManager) AddAllToHash(key string, m map[string]interface{}) (bool, error) {
	args := make(map[string]interface{}, utils.BATCH)
	pipe := client.TxPipeline()
	for k, v := range m {
		if len(args) >= utils.BATCH {
			pipe.HMSet(key, args)
			args = make(map[string]interface{}, utils.BATCH)
		}
		args[k] = v
	}
	if len(args) > 0 {
		pipe.HMSet(key, args)
	}
	if val, err := pipe.Exec(); err != nil {
		log.Infof("%s hmset pipeline error, see details: %+v", rdsLogStr, val)
		return false, err
	}
	return true, nil
}

func (redisMgr *RedisManager) DeleteAllFromHash(key string, fields []string) (bool, error) {
	val, err := client.HDel(key, fields...).Result()
	if err != nil || int64(len(fields)) != val {
		return false, err
	}
	return true, nil
}

func (redisMgr *RedisManager) AddToHash(key, field string, value interface{}) (bool, error) {
	return client.HSetNX(key, field, value).Result()
}

func (redisMgr *RedisManager) GetFromHash(key, field string) (string, error) {
	return client.HGet(key, field).Result()
}

func (redisMgr *RedisManager) DeleteFromHash(key, field string) (bool, error) {
	val, err := client.HDel(key, field).Result()
	if err != nil || int64(1) != val {
		return false, err
	}
	return true, nil
}

func (redisMgr *RedisManager) GetKeysFromHash(key string) ([]string, error) {
	return client.HKeys(key).Result()
}

func (redisMgr *RedisManager) ExistsInHash(key, field string) (bool, error) {
	return client.HExists(key, field).Result()
}

// batch process
func (redisMgr *RedisManager) AddAllToList(key string, values []interface{}) (bool, error) {
	args := make([]interface{}, utils.BATCH)
	pipe := client.TxPipeline()
	for _, v := range values {
		if len(args) >= utils.BATCH {
			pipe.RPush(key, args...)
			args = make([]interface{}, utils.BATCH)
		}
		args = append(args, v)
	}
	if len(args) > 0 {
		pipe.RPush(key, args...)
	}
	if val, err := pipe.Exec(); err != nil {
		log.Infof("%s rpush pipeline error, see details: %+v", rdsLogStr, val)
		return false, err
	}
	return true, nil
}

func (redisMgr *RedisManager) AddToList(key string, value interface{}) (bool, error) {
	val, err := client.RPush(key, value).Result()
	if err != nil || val != int64(1) {
		return false, err
	}
	return true, nil
}

func (redisMgr *RedisManager) DeleteFromList(key string) (string, error) {
	val, err := client.LPop(key).Result()
	if err == redis.Nil {
		return "", errors.New(key + " is empty or does not exist")
	} else if err != nil {
		return "", err
	}
	return val, nil
}
