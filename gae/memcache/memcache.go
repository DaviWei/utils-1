package memcache

import (
	"appengine"
	"appengine/memcache"
	"bytes"
	"crypto/sha1"
	"encoding/base64"
	"fmt"
	"github.com/soundtrackyourbrand/gaeutils/gaecontext"
	"io"
	"math/rand"
	"reflect"
	"time"
)

const (
	regular = iota
	nilCache
)

var Codec = memcache.Gob

/*
Keyify will create a memcache-safe key from k by hashing and base64-encoding it.
*/
func Keyify(k string) string {
	buf := new(bytes.Buffer)
	enc := base64.NewEncoder(base64.URLEncoding, buf)
	h := sha1.New()
	io.WriteString(h, k)
	sum := h.Sum(nil)
	if wrote, err := enc.Write(sum); err != nil {
		panic(err)
	} else if wrote != len(sum) {
		panic(fmt.Errorf("Tried to write %v bytes but wrote %v bytes", len(sum), wrote))
	}
	if err := enc.Close(); err != nil {
		panic(err)
	}
	return string(buf.Bytes())
}

func Incr(c gaecontext.GAEContext, key string, delta int64, initial uint64) (newValue uint64, err error) {
	return memcache.Increment(c, Keyify(key), delta, initial)
}

func IncrExisting(c gaecontext.GAEContext, key string, delta int64) (newValue uint64, err error) {
	return memcache.IncrementExisting(c, Keyify(key), delta)
}

/*
Del will delete the keys from memcache.
*/
func Del(c gaecontext.GAEContext, keys ...string) error {
	for index, key := range keys {
		keys[index] = Keyify(key)
	}
	if err := memcache.DeleteMulti(c, keys); err != nil {
		if multiError, ok := err.(appengine.MultiError); ok {
			for _, singleError := range multiError {
				if singleError != memcache.ErrCacheMiss {
					return singleError
				}
			}
		} else {
			return err
		}
	}
	return nil
}

/*
Get will lookup key and load it into val.

If c is in a transaction no lookup will take place.
*/
func Get(c gaecontext.GAEContext, key string, val interface{}) (found bool, err error) {
	if c.InTransaction() {
		return
	}
	_, err = Codec.Get(c, Keyify(key), val)
	if err == memcache.ErrCacheMiss {
		err = nil
		found = false
	}
	return
}

/*
CAS will replace expected with replacement in memcache if expected is the current value.
*/
func CAS(c gaecontext.GAEContext, key string, expected, replacement interface{}) (success bool, err error) {
	keyHash := Keyify(key)
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
func Put(c gaecontext.GAEContext, key string, val interface{}) error {
	return Codec.Set(c, &memcache.Item{
		Key:    Keyify(key),
		Object: val,
	})
}

/*
Memoize will lookup super and generate a new key from its contents and key. If super is missing a new random value will be inserted there.

It will then lookup that key and load it into destinatinoPointer. A missing value will be generated by the generatorFunction and saved in memcache.

It returns whether the value was nil (either from memcache or from the generatorFunction).

Deleting super will invalidate all keys under it due to the composite keys being impossible to regenerate again.
*/
func Memoize2(c gaecontext.GAEContext, super, key string, destP interface{}, f func() interface{}) (exists bool, err error) {
	superH := Keyify(super)
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
reflectCopy will copy the contents of source to the destinationPointer.
*/
func reflectCopy(srcValue reflect.Value, source, destinationPointer interface{}) {
	if reflect.PtrTo(reflect.TypeOf(source)) == reflect.TypeOf(destinationPointer) {
		reflect.ValueOf(destinationPointer).Elem().Set(srcValue)
	} else {
		reflect.ValueOf(destinationPointer).Elem().Set(reflect.Indirect(srcValue))
	}
}

/*
MemoizeDuring will lookup key and load it into destinatinoPointer. A missing value will be generated by the generatorFunction and saved in memcache with a timeout of duration.

It returns whether the value was nil (either from memcache or from the generatorFunction).
*/
func MemoizeDuring(c gaecontext.GAEContext, key string, duration time.Duration, cacheNil bool, destP interface{}, f func() interface{}) (exists bool, err error) {
	existSlice, errSlice := memoizeMulti(c, []string{key}, duration, cacheNil, []interface{}{destP}, []func() interface{}{f})
	return existSlice[0], errSlice[0]
}

/*
Memoize will lookup key and load it into destinatinoPointer. A missing value will be generated by the generatorFunction and saved in memcache.

It returns whether the value was nil (either from memcache or from the generatorFunction).
*/
func Memoize(c gaecontext.GAEContext, key string, destinationPointer interface{}, generatorFunction func() interface{}) (exists bool, err error) {
	existSlice, errSlice := MemoizeMulti(c, []string{key}, []interface{}{destinationPointer}, []func() interface{}{generatorFunction})
	return existSlice[0], errSlice[0]
}

/*
memGetMulti will look for all provided keys, and load them into the destinatinoPointers.

It will return the memcache.Items it found, and any errors the lookups caused.

If c is within a transaction no lookup will take place and errors will be slice of memcache.ErrCacheMiss.
*/
func memGetMulti(c gaecontext.GAEContext, keys []string, destinationPointers []interface{}) (items []*memcache.Item, errors appengine.MultiError) {
	items = make([]*memcache.Item, len(keys))
	errors = make(appengine.MultiError, len(keys))
	if c.InTransaction() {
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

It returns a slice of bools that show whether each value was nil (either from memcache or from the genrator function).
*/
func MemoizeMulti(c gaecontext.GAEContext, keys []string, destinationPointers []interface{}, generatorFunctions []func() interface{}) (exists []bool, errors appengine.MultiError) {
	return memoizeMulti(c, keys, 0, true, destinationPointers, generatorFunctions)
}

/*
memoizeMulti will look for all provided keys, and load them into the destinationPointers.

Any missing values will be generated using the generatorFunctions and put in memcache with a duration timeout.

If cacheNil nil return values from the generatorFunctions will be cached.

It returns a slice of bools that show whether each value was nil (either from memcache or from the genrator function).
*/
func memoizeMulti(
	c gaecontext.GAEContext,
	keys []string,
	duration time.Duration,
	cacheNil bool,
	destinationPointers []interface{},
	generatorFunctions []func() interface{}) (exists []bool, errors appengine.MultiError) {

	exists = make([]bool, len(keys))
	keyHashes := make([]string, len(keys))
	for index, key := range keys {
		keyHashes[index] = Keyify(key)
	}

	t := time.Now()
	var items []*memcache.Item
	items, errors = memGetMulti(c, keyHashes, destinationPointers)
	if d := time.Now().Sub(t); d > time.Millisecond*10 {
		c.Debugf("SLOW memGetMulti(%v): %v", keys, d)
	}

	panicChan := make(chan interface{}, len(items))
	errorChan := make(chan error, len(items))

	for i, item := range items {
		index := i
		err := errors[index]
		keyHash := keyHashes[index]
		destinationPointer := destinationPointers[index]
		if err == memcache.ErrCacheMiss {
			go func() (err error) {
				defer func() {
					panicChan <- recover()
					errorChan <- err
				}()
				result := generatorFunctions[index]()
				resultValue := reflect.ValueOf(result)
				if resultValue.IsNil() {
					if cacheNil {
						nilObj := reflect.Indirect(reflect.ValueOf(destinationPointer)).Interface()
						if err = Codec.Set(c, &memcache.Item{
							Key:        keyHash,
							Flags:      nilCache,
							Object:     nilObj,
							Expiration: duration,
						}); err != nil {
							return
						}
					}
					exists[index] = false
				} else {
					if err = Codec.Set(c, &memcache.Item{
						Key:        keyHash,
						Object:     result,
						Expiration: duration,
					}); err != nil {
						return
					} else {
						reflectCopy(resultValue, result, destinationPointer)
						exists[index] = true
					}
				}
				return
			}()
		} else if err != nil {
			panicChan <- nil
			errorChan <- err
		} else {
			if item.Flags&nilCache == nilCache {
				exists[index] = false
			} else {
				exists[index] = true
			}
			panicChan <- nil
			errorChan <- nil
		}
	}

	panics := []interface{}{}
	errors = make(appengine.MultiError, len(items))
	for i, _ := range items {
		if e := <-panicChan; e != nil {
			panics = append(panics, e)
		}
		errors[i] = <-errorChan
	}
	if len(panics) > 0 {
		panic(panics)
	}
	return
}
