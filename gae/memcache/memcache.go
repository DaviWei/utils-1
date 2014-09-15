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
		err = utils.Errorf("Tried to write %v bytes but wrote %v bytes", len(sum), wrote)
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
	if newValue, err = memcache.Increment(c, k, delta, initial); err != nil {
		err = utils.Errorf("Error doing Increment %#v: %v", k, err)
		return
	}
	return
}

func IncrExisting(c TransactionContext, key string, delta int64) (newValue uint64, err error) {
	k, err := Keyify(key)
	if err != nil {
		return
	}
	if newValue, err = memcache.IncrementExisting(c, k, delta); err != nil {
		err = utils.Errorf("Error doing IncrementExisting %#v: %v", k, err)
		return
	}
	return
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
			return delWithRetry(c, keys...)
		})
	}
	return delWithRetry(c, keys...)
}

/*
delWithRetry will delete the keys from memcache. If it fails, it will retry a few times.
*/
func delWithRetry(c TransactionContext, keys ...string) (err error) {
	waitTime := time.Millisecond * 10

	for waitTime < 1*time.Second {
		err = del(c, keys...)
		if err == nil {
			return
		}
		time.Sleep(waitTime)
		waitTime = waitTime * 2
	}
	return
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
		if merr, ok := err.(appengine.MultiError); ok {
			errors := make(appengine.MultiError, len(merr))
			actualErrors := 0
			for index, serr := range merr {
				if serr != memcache.ErrCacheMiss {
					errors[index] = utils.Errorf("Error doing DeleteMulti: %v", serr)
					actualErrors++
				}
			}
			if actualErrors > 0 {
				err = errors
				return
			} else {
				err = nil
			}
		} else {
			if err == ErrCacheMiss {
				err = nil
			} else {
				err = utils.Errorf("Error doing DeleteMulti: %v", err)
				return
			}
		}
	}
	return
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
	} else {
		c.Errorf("Error doing Get %#v: %v", err)
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
		} else {
			err = utils.Errorf("Error doing Get %#v: %v", keyHash, err)
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
	if err = memcache.CompareAndSwap(c, item); err != nil {
		if err == memcache.ErrCASConflict {
			err = nil
		} else {
			marshalled, _ := Codec.Marshal(replacement)
			err = utils.Errorf("Error doing CompareAndSwap %#v to %v bytes: %v", item.Key, len(marshalled), err)
		}
		return
	}
	success = true
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

func codecSet(c TransactionContext, codec memcache.Codec, item *memcache.Item) (err error) {
	waitTime := time.Millisecond * 10

	for waitTime < 1*time.Second {
		err = codec.Set(c, item)
		if err == nil {
			return
		}
		time.Sleep(waitTime)
		waitTime *= 2
	}
	marshalled, _ := codec.Marshal(item.Object)
	err = utils.Errorf("Error doing Codec.Set %#v with %v bytes: %v", item.Key, len(marshalled), err)
	return
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
	return codecSet(c, Codec, item)
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
		c.Errorf("Error doing Get %#v: %v", superH, err)
		err = memcache.ErrCacheMiss
	}
	if err == memcache.ErrCacheMiss {
		seed = fmt.Sprint(rand.Int63())
		item = &memcache.Item{
			Key:   superH,
			Value: []byte(seed),
		}
		if err = memcache.Set(c, item); err != nil {
			err = utils.Errorf("Error doing Set %#v with %v bytes: %v", item.Key, len(item.Value), err)
			return
		}
	} else {
		seed = string(item.Value)
	}
	return Memoize(c, fmt.Sprintf("%v@%v", key, seed), destP, f)
}

/*
MemoizeDuringSmart will lookup key and load it into destinatinoPointer. A missing value will be generated by the generatorFunction and saved in memcache with a timeout of duration.
*/
func MemoizeDuringSmart(c TransactionContext, key string, cacheNil bool, destP interface{}, f func() (interface{}, time.Duration, error)) (err error) {
	errSlice := memoizeMulti(c, []string{key}, cacheNil, []interface{}{destP}, []func() (interface{}, time.Duration, error){f})
	return errSlice[0]
}

/*
MemoizeDuring will lookup key and load it into destinatinoPointer. A missing value will be generated by the generatorFunction and saved in memcache with a timeout of duration.
*/
func MemoizeDuring(c TransactionContext, key string, duration time.Duration, cacheNil bool, destP interface{}, f func() (interface{}, error)) (err error) {
	errSlice := memoizeMulti(c, []string{key}, cacheNil, []interface{}{destP}, []func() (interface{}, time.Duration, error){
		func() (res interface{}, dur time.Duration, err error) {
			res, err = f()
			dur = duration
			return
		},
	})
	return errSlice[0]
}

/*
Memoize will lookup key and load it into destinatinoPointer. A missing value will be generated by the generatorFunction and saved in memcache.
*/
func Memoize(c TransactionContext, key string, destP interface{}, f func() (interface{}, error)) (err error) {
	errSlice := memoizeMulti(c, []string{key}, true, []interface{}{destP}, []func() (interface{}, time.Duration, error){
		func() (res interface{}, dur time.Duration, err error) {
			res, err = f()
			return
		},
	})
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
		c.Errorf("Error doing GetMulti: %v", err)
		for index, _ := range errors {
			errors[index] = ErrCacheMiss
		}
		err = errors
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
	newFunctions := make([]func() (interface{}, time.Duration, error), len(generatorFunctions))
	for index, gen := range generatorFunctions {
		genCpy := gen
		newFunctions[index] = func() (res interface{}, dur time.Duration, err error) {
			res, err = genCpy()
			return
		}
	}
	return memoizeMulti(c, keys, true, destinationPointers, newFunctions)
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
	cacheNil bool,
	destinationPointers []interface{},
	generatorFunctions []func() (interface{}, time.Duration, error)) (errors appengine.MultiError) {

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
					if e := recover(); e != nil {
						c.Infof("Panic: %v", e)
						panicChan <- fmt.Errorf("%v\n%v", e, utils.Stack())
					} else {
						panicChan <- nil
					}
				}()
				var result interface{}
				var duration time.Duration
				found := true
				if result, duration, err = generatorFunctions[index](); err != nil {
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
					if err2 := codecSet(c, Codec, &memcache.Item{
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
