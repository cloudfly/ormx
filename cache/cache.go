package cache

import (
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru"
)

var (
	lock sync.RWMutex
	// Assuming each item is 128 bytes, we allocate 25% of our available memory to the cache.
	defaultSize = GetMemoryLimit() / 4 / 128
	cache       *lru.TwoQueueCache
	finalizers  []Finalizer
)

type Finalizer func(any, any)

type Value struct {
	Data   interface{}
	Expire time.Time
}

// Init the cache size
func Init(fs ...Finalizer) (err error) {
	size := defaultSize
	if size == 0 {
		size = 1024 * 1024 * 64 // 64M
	}
	cache, err = lru.New2Q(int(size))
	if err != nil {
		return err
	}
	finalizers = fs
	go tick()
	return
}

func Contains(key interface{}) bool {
	return cache.Contains(key)
}

func Expire() {
	lock.Lock()
	defer lock.Unlock()
	now := time.Now()
	for _, key := range cache.Keys() {
		if v, ok := cache.Get(key); ok && v.(Value).Expire.Before(now) {
			cache.Remove(key)
			for _, f := range finalizers {
				f(key, v.(Value).Data)
			}
		}
	}
}

func getByKey(key interface{}) (interface{}, bool) {
	lock.Lock()
	defer lock.Unlock()
	value, ok := cache.Get(key)
	if !ok {
		return nil, false
	}
	ins := value.(Value)
	if ins.Expire.Before(time.Now()) {
		cache.Remove(key)
		for _, f := range finalizers {
			f(key, ins.Data)
		}
		return nil, false
	}
	return ins.Data, true
}

func Get(keys ...interface{}) (interface{}, bool) {
	key := joinSlice(keys, "/")
	return getByKey(key)
}

func Set(ttl time.Duration, keyAndValue ...any) {
	if len(keyAndValue) <= 2 {
		return
	}

	keys := keyAndValue[:len(keyAndValue)-1]
	value := keyAndValue[len(keyAndValue)-1]
	key := joinSlice(keys, "/")
	lock.Lock()
	defer lock.Unlock()
	cache.Add(key, Value{
		Data:   value,
		Expire: time.Now().Add(ttl),
	})
}

func Remove(keys ...any) {
	key := joinSlice(keys, "/")
	lock.Lock()
	defer lock.RUnlock()
	value, ok := cache.Get(key)
	if !ok {
		return
	}
	cache.Remove(key)
	for _, f := range finalizers {
		f(key, value.(Value).Data)
	}
}

func Try(dest any, fallback func() error, ttl time.Duration, keys ...any) error {
	key := joinSlice(keys, "/")
	value, ok := getByKey(key)
	if !ok {
		if err := fallback(); err != nil {
			return err
		}
		cache.Add(key, Value{
			Data:   reflect.ValueOf(dest).Elem().Interface(),
			Expire: time.Now().Add(ttl),
		})
	} else if dest == nil {
		return fmt.Errorf("dest is nil")
	} else {
		reflect.ValueOf(dest).Elem().Set(reflect.ValueOf(value))
	}
	return nil
}

func Len() int {
	return cache.Len()
}

func tick() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for range ticker.C {
		Expire()
	}
}

func joinSlice[T int | string | int64 | int32 | int16 | int8 | uint32 | uint64 | uint16 | uint8 | float64 | float32 | any](data []T, split string) string {
	builder := &strings.Builder{}
	for i, item := range data {
		builder.WriteString(fmt.Sprintf("%v", item))
		if i < len(data)-1 {
			builder.WriteString(split)
		}
	}
	return builder.String()
}
