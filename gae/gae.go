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

func KeyById(dst interface{}) string {
	elem := reflect.ValueOf(dst).Elem()
	return fmt.Sprintf("%s{Id:%v}", elem.Type().Name(), elem.FieldByName("Id").Interface())
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
	if typ, id, err = typeAndId(dst); err != nil {
		return
	}
	return ErrNoSuchEntity{
		Type:  typ.Name(),
		Cause: cause,
		Id:    id,
	}
}

func typeAndId(dst interface{}) (typ reflect.Type, id *key.Key, err error) {
	val := reflect.ValueOf(dst)
	if val.Kind() != reflect.Ptr {
		err = fmt.Errorf("%+v is not a pointer", dst)
		return
	}
	if val.Elem().Kind() != reflect.Struct {
		err = fmt.Errorf("%+v is not a pointer to a struct", dst)
		return
	}
	typ = val.Elem().Type()
	idField := val.Elem().FieldByName("Id")
	if !idField.IsValid() {
		err = fmt.Errorf("%+v does not have a field named Id", dst)
		return
	}
	if !idField.Type().AssignableTo(reflect.TypeOf((*key.Key)(nil))) {
		err = fmt.Errorf("%+v does not have a field named Id that is a *key.Key", dst)
		return
	}
	id = idField.Interface().(*key.Key)
	return
}

func findById(c gaecontext.GAEContext, dst interface{}) (result interface{}, err error) {
	var id *key.Key
	if _, id, err = typeAndId(dst); err != nil {
		return
	}
	if err = datastore.Get(c, id.ToGAE(c), dst); err == datastore.ErrNoSuchEntity {
		err = newError(dst, err)
		return
	}
	if err = FilterOkErrors(err); err != nil {
		return
	}
	result = dst
	return
}

func Put(c gaecontext.GAEContext, src interface{}) (err error) {
	var id *key.Key
	if _, id, err = typeAndId(src); err != nil {
		return
	}
	if id == nil {
		err = fmt.Errorf("%+v doesn't have an Id", src)
		return
	}
	var newId *key.Key
	if newId, err = key.FromGAErr(datastore.Put(c, id.ToGAE(c), src)); err != nil {
		return
	}
	if !newId.Equal(id) {
		if err = memcache.Del(c, KeyById(src)); err != nil {
			return
		}
	}
	reflect.ValueOf(src).Elem().FieldByName("Id").Set(reflect.ValueOf(newId))
	return memcache.Del(c, KeyById(src))
}

func GetById(c gaecontext.GAEContext, dst interface{}) (result interface{}, err error) {
	if err = memcache.Memoize(c, KeyById(dst), dst, func() (result interface{}, err error) {
		result, err = findById(c, dst)
		if _, ok := err.(ErrNoSuchEntity); ok {
			err = memcache.ErrCacheMiss
		}
		if err != nil {
			return
		}
		return
	}); err == nil {
		val := reflect.ValueOf(dst)
		if process := val.MethodByName("Process"); process.IsValid() {
			processType := process.Type()
			if processType.NumIn() != 1 {
				err = fmt.Errorf("%+v#Process doesn't take exactly one argument", dst)
				return
			}
			if !reflect.TypeOf(c).AssignableTo(processType.In(0)) {
				err = fmt.Errorf("%+v#Process doesn't take a %v as argument", dst, reflect.TypeOf(c))
				return
			}
			if processType.NumOut() < 1 {
				err = fmt.Errorf("%+v#Process doesn't produce at least one return value", dst)
				return
			}
			if !processType.Out(0).AssignableTo(val.Type()) {
				err = fmt.Errorf("%+v#Process doesn't return a %v as first return value", dst, reflect.TypeOf(dst))
				return
			}
			if processType.NumOut() > 1 && !processType.Out(processType.NumOut()-1).AssignableTo(reflect.TypeOf((error)(nil))) {
				err = fmt.Errorf("%+v#Process doesn't return an error as last return value", dst)
				return
			}
			results := process.Call([]reflect.Value{reflect.ValueOf(c)})
			if len(results) > 1 && !results[len(results)-1].IsNil() {
				err = results[len(results)-1].Interface().(error)
				return
			}
			result = results[0].Interface()
		} else {
			result = val.Interface()
		}
	} else if err == memcache.ErrCacheMiss {
		err = newError(dst, datastore.ErrNoSuchEntity)
	}
	return
}
