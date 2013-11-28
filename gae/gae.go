package gae

import (
	"appengine"
	"appengine/datastore"
	"fmt"
	"github.com/soundtrackyourbrand/utils/gae/key"
	"github.com/soundtrackyourbrand/utils/gae/memcache"
	"net/http"
	"reflect"
)

const (
	idFieldName = "Id"
)

/*
MemcacheKeys will append to oldKeys, and also return as newKeys, any memcache keys this package knows about that would
result in the provided model being found.

It will use the id based key, and any memcache keys provided by finders created by Finder or AncestorFinder.
*/
func MemcacheKeys(c memcache.TransactionContext, model interface{}, oldKeys *[]string) (newKeys []string, err error) {
	if oldKeys == nil {
		oldKeys = &[]string{}
	}
	*oldKeys = append(*oldKeys, keyById(model))
	for _, finder := range registeredFinders[reflect.TypeOf(model).Elem().Name()] {
		if _, err = finder.cacheKeys(c, model, oldKeys); err != nil {
			return
		}
	}
	newKeys = *oldKeys
	return
}

// getTypeAndId will validate that the model is a pointer to a struct, and that it has a *key.Key field name Id.
func getTypeAndId(model interface{}) (typ reflect.Type, id *key.Key, err error) {
	val := reflect.ValueOf(model)
	if val.Kind() != reflect.Ptr {
		err = fmt.Errorf("%+v is not a pointer", model)
		return
	}
	if val.Elem().Kind() != reflect.Struct {
		err = fmt.Errorf("%+v is not a pointer to a struct", model)
		return
	}
	typ = val.Elem().Type()
	idField := val.Elem().FieldByName(idFieldName)
	if !idField.IsValid() {
		err = fmt.Errorf("%+v does not have a field named Id", model)
		return
	}
	if !idField.Type().AssignableTo(reflect.TypeOf(&key.Key{})) {
		err = fmt.Errorf("%+v does not have a field named Id that is a *key.Key", model)
		return
	}
	id = idField.Interface().(*key.Key)
	return
}

// keyById will return the memcache key used to find dst by id.
func keyById(dst interface{}) string {
	elem := reflect.ValueOf(dst).Elem()
	return fmt.Sprintf("%s{Id:%v}", elem.Type().Name(), elem.FieldByName(idFieldName).Interface())
}

/*
FilterOkErrors will return nil if the provided error is a FieldMismatch, one of the accepted errors, or an appengine.MultiError combination thereof, Otherwise it will return err.
*/
func FilterOkErrors(err error, accepted ...error) error {
	acceptedMap := map[string]bool{}
	for _, e := range accepted {
		acceptedMap[e.Error()] = true
	}
	if err != nil {
		if merr, ok := err.(appengine.MultiError); ok {
			for _, serr := range merr {
				if serr != nil {
					if _, ok := serr.(*datastore.ErrFieldMismatch); !ok && !acceptedMap[serr.Error()] {
						return err
					}
				}
			}
		} else if _, ok := err.(*datastore.ErrFieldMismatch); !ok && !acceptedMap[err.Error()] {
			return err
		}
	}
	return nil
}

/*
ErrNoSuchEntity is just an easily identifiable way of determining that we didn't find what we were looking for, while still providing something the httpcontext types can render as an http response.
*/
type ErrNoSuchEntity struct {
	Type  string
	Cause error
	Id    *key.Key
}

func (self ErrNoSuchEntity) Error() string {
	return fmt.Sprintf("No %v with id %v found", self.Type, self.Id)
}

func (self ErrNoSuchEntity) Write(w http.ResponseWriter) (err error) {
	_, err = fmt.Fprint(w, self.Error())
	return
}
func (self ErrNoSuchEntity) GetLocation() string {
	return ""
}
func (self ErrNoSuchEntity) GetStatus() int {
	return 404
}

func newError(dst interface{}, cause error) (err error) {
	var typ reflect.Type
	var id *key.Key
	if typ, id, err = getTypeAndId(dst); err != nil {
		return
	}
	return ErrNoSuchEntity{
		Type:  typ.Name(),
		Cause: cause,
		Id:    id,
	}
}

/*
Del will delete src from datastore and invalidate it from memcache.
*/
func Del(c memcache.TransactionContext, src interface{}) (err error) {
	var id *key.Key
	var typ reflect.Type
	if typ, id, err = getTypeAndId(src); err != nil {
		return
	}
	if id == nil {
		err = fmt.Errorf("%+v doesn't have an Id", src)
		return
	}
	gaeKey := id.ToGAE(c)
	if !gaeKey.Incomplete() {
		old := reflect.New(typ)
		old.Elem().FieldByName(idFieldName).Set(reflect.ValueOf(id))
		err = GetById(c, old.Interface())
		if _, ok := err.(ErrNoSuchEntity); ok {
			err = nil
		} else if err == nil {
			if err = datastore.Delete(c, gaeKey); err != nil {
				return
			}
			memKeys := []string{}
			if memKeys, err = MemcacheKeys(c, old.Interface(), nil); err != nil {
				return
			}
			if err = memcache.Del(c, memKeys...); err != nil {
				return
			}
		}
	}
	return
}

/*
Put will save src in datastore after having cache invalidated anything that was there before. Then it will invalidate src as well.

Before saving src, it will run its BeforeCreate or BeforeUpdate func, if any, depending on whether there was a matching model in
the datastore before.

It will also (after the BeforeUpdate/BeforeCreate functions) run BeforeSave.
*/
func Put(c memcache.TransactionContext, src interface{}) (err error) {
	var id *key.Key
	var typ reflect.Type
	if typ, id, err = getTypeAndId(src); err != nil {
		return
	}
	if id == nil {
		err = fmt.Errorf("%+v doesn't have an Id", src)
		return
	}
	isNew := false
	gaeKey := id.ToGAE(c)
	memcacheKeys := []string{}
	if gaeKey.Incomplete() {
		isNew = true
	} else {
		old := reflect.New(typ)
		old.Elem().FieldByName(idFieldName).Set(reflect.ValueOf(id))
		err = GetById(c, old.Interface())
		if _, ok := err.(ErrNoSuchEntity); ok {
			err = nil
			isNew = true
		} else if err == nil {
			isNew = false
			if _, err = MemcacheKeys(c, old.Interface(), &memcacheKeys); err != nil {
				return
			}
		} else {
			return
		}
	}
	if isNew {
		if err = runProcess(c, src, beforeCreateName); err != nil {
			return
		}
	} else {
		if err = runProcess(c, src, beforeUpdateName); err != nil {
			return
		}
	}
	if err = runProcess(c, src, beforeSaveName); err != nil {
		return
	}
	if id, err = key.FromGAErr(datastore.Put(c, gaeKey, src)); err != nil {
		return
	}
	reflect.ValueOf(src).Elem().FieldByName(idFieldName).Set(reflect.ValueOf(id))
	if _, err = MemcacheKeys(c, src, &memcacheKeys); err != nil {
		return
	}
	return memcache.Del(c, memcacheKeys...)
}

// findById will find dst in the datastore and set its id.
func findById(c memcache.TransactionContext, dst interface{}) (err error) {
	var id *key.Key
	if _, id, err = getTypeAndId(dst); err != nil {
		return
	}
	if err = datastore.Get(c, id.ToGAE(c), dst); err == datastore.ErrNoSuchEntity {
		err = newError(dst, err)
		return
	}
	if err = FilterOkErrors(err); err != nil {
		return
	}
	return
}

/*
GetById will find memoize finding dst in the datastore, setting its id and running its AfterLoad function, if any.
*/
func GetById(c memcache.TransactionContext, dst interface{}) (err error) {
	if err = memcache.Memoize(c, keyById(dst), dst, func() (result interface{}, err error) {
		err = findById(c, dst)
		if _, ok := err.(ErrNoSuchEntity); ok {
			err = memcache.ErrCacheMiss
		}
		if err != nil {
			return
		}
		result = dst
		return
	}); err == nil {
		err = runProcess(c, dst, afterLoadName)
	} else if err == memcache.ErrCacheMiss {
		err = newError(dst, datastore.ErrNoSuchEntity)
	}
	return
}
