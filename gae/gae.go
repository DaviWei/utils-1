package gae

import (
	"appengine"
	"appengine/datastore"
	"fmt"
	"github.com/soundtrackyourbrand/utils/gae/gaecontext"
	"github.com/soundtrackyourbrand/utils/gae/key"
	"github.com/soundtrackyourbrand/utils/gae/memcache"
	"net/http"
	"reflect"
)

const (
	idFieldName = "Id"
)

func MemcacheKeys(c gaecontext.GAEContext, model interface{}, oldKeys *[]string) (newKeys []string, err error) {
	if oldKeys == nil {
		oldKeys = &[]string{}
	}
	*oldKeys = append(*oldKeys, keyById(model))
	for _, finder := range registeredFinders[reflect.TypeOf(model).Elem().Name()] {
		*oldKeys = append(*oldKeys, finder.cacheKey(c, model))
	}
	newKeys = *oldKeys
	return
}

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

func keyById(dst interface{}) string {
	elem := reflect.ValueOf(dst).Elem()
	return fmt.Sprintf("%s{Id:%v}", elem.Type().Name(), elem.FieldByName(idFieldName).Interface())
}

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

func Del(c gaecontext.GAEContext, src interface{}) (err error) {
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

func Put(c gaecontext.GAEContext, src interface{}) (err error) {
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

func findById(c gaecontext.GAEContext, dst interface{}) (err error) {
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

func GetById(c gaecontext.GAEContext, dst interface{}) (err error) {
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
