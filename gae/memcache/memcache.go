package memcache

import (
	"bytes"
	"crypto/sha1"
	"encoding/base64"
	"fmt"
	"io"
	"math/rand"
	"reflect"
	"time"

	"github.com/soundtrackyourbrand/utils"

	"appengine"
	"appengine/memcache"
)

var MemcacheEnabled = true

type TransactionContext interface {
	appengine.Context
	InTransaction() bool
	AfterTransaction(interface{}) error
}

const (
	regular = iota
	nilCache
)

var Codec = memcache.Gob
var ErrCacheMiss = memcache.ErrCacheMiss

/*
Keyify will create a memcache-safe key from k by hashing and base64-encoding it.
*/
func Keyify(k string) (result string, err error) {
	buf := new(bytes.Buffer)
	enc := base64.NewEncoder(base64.URLEncoding, buf)
	h := sha1.New()
	io.WriteString(h, k)
	sum := h.Sum(nil)
	wrote, err := enc.Write(sum)
	if err != nil {
		return
	} else if wrote != len(sum) {
		err = fmt.Errorf("Tried to write %v bytes but wrote %v bytes", len(sum), wrote)
		return
	}
	if err = enc.Close(); err != nil {
		return
	}
	result = string(buf.Bytes())
	return
}

func Incr(c TransactionContext, key string, delta int64, initial uint64) (newValue uint64, err error) {
	k, err := Keyify(key)
	if err != nil {
		return
	}
	return memcache.Increment(c, k, delta, initial)
}

func IncrExisting(c TransactionContext, key string, delta int64) (newValue uint64, err error) {
	k, err := Keyify(key)
	if err != nil {
		return
	}
	return memcache.IncrementExisting(c, k, delta)
}

/*
Del will delete the keys from memcache.

If c is InTransaction it will put the actual deletion inside c.AfterTransaction, otherwise
the deletion will execute immediately.
*/
func Del(c TransactionContext, keys ...string) (err error) {
	if !MemcacheEnabled {
		return
	}
	if c.InTransaction() {
		return c.AfterTransaction(func(c TransactionContext) error {
			return del(c, keys...)
		})
	}
	return del(c, keys...)
}

/*
del will delete the keys from memcache.
*/
func del(c TransactionContext, keys ...string) (err error) {
	for index, key := range keys {
		var k string
		k, err = Keyify(key)
		if err != nil {
			return
		}
		keys[index] = k
	}
	if err = memcache.DeleteMulti(c, keys); err != nil {
		if multiError, ok := err.(appengine.MultiError); ok {
			for _, singleError := range multiError {
				if singleError != memcache.ErrCacheMiss {
					err = singleError
					return
				}
			}
		} else {
			return
		}
	}
	return nil
}

/*
Get will lookup key and load it into val.

If c is in a transaction no lookup will take place.
*/
func Get(c TransactionContext, key string, val interface{}) (found bool, err error) {
	if !MemcacheEnabled {
		return
	}
	if c.InTransaction() {
		return
	}
	k, err := Keyify(key)
	if err != nil {
		return
	}
	_, err = Codec.Get(c, k, val)
	if err == memcache.ErrCacheMiss {
		err = nil
		found = false
	}
	return
}

/*
CAS will replace expected with replacement in memcache if expected is the current value.
*/
func CAS(c TransactionContext, key string, expected, replacement interface{}) (success bool, err error) {
	keyHash, err := Keyify(key)
	if err != nil {
		return
	}
	var item *memcache.Item
	if item, err = memcache.Get(c, keyHash); err != nil {
		if err == memcache.ErrCacheMiss {
			err = nil
		}
		return
	}
	var encoded []byte
	if encoded, err = Codec.Marshal(expected); err != nil {
		return
	}
	if bytes.Compare(encoded, item.Value) != 0 {
		success = false
		return
	}
	if encoded, err = Codec.Marshal(replacement); err != nil {
		return
	}
	item.Value = encoded
	if err = memcache.CompareAndSwap(c, item); err != nil && err == memcache.ErrCASConflict {
		success = false
		err = nil
	} else if err == nil {
		success = true
	}
	return
}

/*
Put will put val under key.
*/
func Put(c TransactionContext, key string, val interface{}) (err error) {
	return putUntil(c, nil, key, val)
}

/*
PutUntil will put val under key for at most until.
*/
func PutUntil(c TransactionContext, until time.Duration, key string, val interface{}) (err error) {
	return putUntil(c, &until, key, val)
}

func putUntil(c TransactionContext, until *time.Duration, key string, val interface{}) (err error) {
	if !MemcacheEnabled {
		return
	}
	k, err := Keyify(key)
	if err != nil {
		return
	}
	item := &memcache.Item{
		Key:    k,
		Object: val,
	}
	if until != nil {
		item.Expiration = *until
	}
	return Codec.Set(c, item)
}

/*
Memoize will lookup super and generate a new key from its contents and key. If super is missing a new random value will be inserted there.

It will then lookup that key and load it into destinatinoPointer. A missing value will be generated by the generatorFunction and saved in memcache.

It returns whether the value was nil (either from memcache or from the generatorFunction).

Deleting super will invalidate all keys under it due to the composite keys being impossible to regenerate again.
*/
func Memoize2(c TransactionContext, super, key string, destP interface{}, f func() (interface{}, error)) (err error) {
	superH, err := Keyify(super)
	if err != nil {
		return
	}
	var seed string
	var item *memcache.Item
	if item, err = memcache.Get(c, superH); err != nil && err != memcache.ErrCacheMiss {
		return
	}
	if err == memcache.ErrCacheMiss {
		seed = fmt.Sprint(rand.Int63())
		item = &memcache.Item{
			Key:   superH,
			Value: []byte(seed),
		}
		if err = memcache.Set(c, item); err != nil {
			return
		}
	} else {
		seed = string(item.Value)
	}
	return Memoize(c, fmt.Sprintf("%v@%v", key, seed), destP, f)
}

/*
MemoizeDuring will lookup key and load it into destinatinoPointer. A missing value will be generated by the generatorFunction and saved in memcache with a timeout of duration.
*/
func MemoizeDuring(c TransactionContext, key string, duration time.Duration, cacheNil bool, destP interface{}, f func() (interface{}, error)) (err error) {
	errSlice := memoizeMulti(c, []string{key}, duration, cacheNil, []interface{}{destP}, []func() (interface{}, error){f})
	return errSlice[0]
}

/*
Memoize will lookup key and load it into destinatinoPointer. A missing value will be generated by the generatorFunction and saved in memcache.
*/
func Memoize(c TransactionContext, key string, destinationPointer interface{}, generatorFunction func() (interface{}, error)) (err error) {
	errSlice := MemoizeMulti(c, []string{key}, []interface{}{destinationPointer}, []func() (interface{}, error){generatorFunction})
	return errSlice[0]
}

/*
memGetMulti will look for all provided keys, and load them into the destinatinoPointers.

It will return the memcache.Items it found, and any errors the lookups caused.

If c is within a transaction no lookup will take place and errors will be slice of memcache.ErrCacheMiss.
*/
func memGetMulti(c TransactionContext, keys []string, destinationPointers []interface{}) (items []*memcache.Item, errors appengine.MultiError) {
	items = make([]*memcache.Item, len(keys))
	errors = make(appengine.MultiError, len(keys))
	if !MemcacheEnabled || c.InTransaction() {
		for index, _ := range errors {
			errors[index] = memcache.ErrCacheMiss
		}
		return
	}

	itemHash, err := memcache.GetMulti(c, keys)
	if err != nil {
		for index, _ := range errors {
			errors[index] = err
		}
		return
	}

	var item *memcache.Item
	var ok bool
	for index, keyHash := range keys {
		if item, ok = itemHash[keyHash]; ok {
			items[index] = item
			if err := Codec.Unmarshal(item.Value, destinationPointers[index]); err != nil {
				errors[index] = err
			}
		} else {
			errors[index] = memcache.ErrCacheMiss
		}
	}
	return
}

/*
MemoizeMulti will look for all provided keys, and load them into the destinationPointers.

Any missing values will be generated using the generatorFunctions and put in memcache without a timeout.
*/
func MemoizeMulti(c TransactionContext, keys []string, destinationPointers []interface{}, generatorFunctions []func() (interface{}, error)) (errors appengine.MultiError) {
	return memoizeMulti(c, keys, 0, true, destinationPointers, generatorFunctions)
}

/*
memoizeMulti will look for all provided keys, and load them into the destinationPointers.

Any missing values will be generated using the generatorFunctions and put in memcache with a duration timeout.

If cacheNil is true, nil results or memcache.ErrCacheMiss errors from the generator function will be cached.

It returns a slice of bools that show whether each value was found (either from memcache or from the genrator function).
*/
func memoizeMulti(
	c TransactionContext,
	keys []string,
	duration time.Duration,
	cacheNil bool,
	destinationPointers []interface{},
	generatorFunctions []func() (interface{}, error)) (errors appengine.MultiError) {

	keyHashes := make([]string, len(keys))
	for index, key := range keys {
		k, err := Keyify(key)
		if err != nil {
			errors = appengine.MultiError{err}
			return
		}
		keyHashes[index] = k
	}

	t := time.Now()
	var items []*memcache.Item
	items, errors = memGetMulti(c, keyHashes, destinationPointers)
	if d := time.Now().Sub(t); d > time.Millisecond*10 {
		c.Debugf("SLOW memGetMulti(%v): %v", keys, d)
	}

	panicChan := make(chan interface{}, len(items))

	for i, item := range items {

		index := i
		err := errors[index]
		keyHash := keyHashes[index]
		destinationPointer := destinationPointers[index]
		if err == memcache.ErrCacheMiss {
			go func() (err error) {
				defer func() {
					errors[index] = err
					panicChan <- recover()
				}()
				var result interface{}
				found := true
				if result, err = generatorFunctions[index](); err != nil {
					if err != memcache.ErrCacheMiss {
						return
					} else {
						found = false
					}
				} else {
					found = !utils.IsNil(result)
					if !found {
						err = memcache.ErrCacheMiss
					}
				}
				if !c.InTransaction() && (found || cacheNil) {
					obj := result
					var flags uint32
					if !found {
						obj = reflect.Indirect(reflect.ValueOf(destinationPointer)).Interface()
						flags = nilCache
					}
					if err2 := Codec.Set(c, &memcache.Item{
						Key:        keyHash,
						Flags:      flags,
						Object:     obj,
						Expiration: duration,
					}); err2 != nil {
						err = err2
						return
					}
				}
				if found {
					utils.ReflectCopy(result, destinationPointer)
				}
				return
			}()
		} else if err != nil {
			panicChan <- nil
		} else {
			if item.Flags&nilCache == nilCache {
				errors[index] = memcache.ErrCacheMiss
			}
			panicChan <- nil
		}
	}

	panics := []interface{}{}
	for _, _ = range items {
		if e := <-panicChan; e != nil {
			panics = append(panics, e)
		}
	}
	if len(panics) > 0 {
		panic(panics)
	}
	return
}
