package gae

import (
	"appengine"
	"appengine/datastore"
	"fmt"
	"github.com/soundtrackyourbrand/utils/gae/gaecontext"
	"github.com/soundtrackyourbrand/utils/gae/key"
	"github.com/soundtrackyourbrand/utils/gae/memcache"
	"reflect"
)

type finder struct {
	fields []string
	model  interface{}
}

var registeredFinders = map[string][]finder{}

func newFinder(model interface{}, fields ...string) (result finder) {
	typ, _, err := getTypeAndId(model)
	if err != nil {
		panic(err)
	}
	if err := validateProcessors(model); err != nil {
		panic(err)
	}
	val := reflect.ValueOf(model).Elem()
	for _, field := range fields {
		if f := val.FieldByName(field); !f.IsValid() {
			panic(fmt.Errorf("%+v doesn't have a field named %#v", field))
		}
	}
	result = finder{
		fields: fields,
		model:  model,
	}
	name := typ.Name()
	registeredFinders[name] = append(registeredFinders[name], result)
	return
}

func Finder(model interface{}, fields ...string) func(c gaecontext.GAEContext, dst interface{}, values ...interface{}) error {
	return newFinder(model, fields...).get
}

func AncestorFinder(model interface{}, fields ...string) func(c gaecontext.GAEContext, dst interface{}, ancestor *key.Key, values ...interface{}) error {
	return newFinder(model, fields...).getWithAncestor
}

func (self finder) find(c gaecontext.GAEContext, dst interface{}, ancestor *key.Key, values []interface{}) (err error) {
	q := datastore.NewQuery(reflect.TypeOf(self.model).Elem().Name())
	if ancestor != nil {
		q = q.Ancestor(ancestor.ToGAE(c))
	}
	for index, value := range values {
		q = q.Filter(fmt.Sprintf("%v=", self.fields[index]), value)
	}
	var ids []*datastore.Key
	ids, err = q.GetAll(c, dst)
	if err = FilterOkErrors(err); err != nil {
		return
	}
	for index, id := range ids {
		reflect.ValueOf(dst).Elem().Index(index).FieldByName(idFieldName).Set(reflect.ValueOf(key.FromGAE(id)))
	}
	return
}

func (self finder) keyForValues(values []interface{}) string {
	return fmt.Sprintf("%v{%+v:%+v}", reflect.TypeOf(self.model).Name(), self.fields, values)
}

func (self finder) cacheKey(c gaecontext.GAEContext, model interface{}) string {
	values := make([]interface{}, len(self.fields))
	val := reflect.ValueOf(model).Elem()
	for index, field := range self.fields {
		values[index] = val.FieldByName(field).Interface()
	}
	return self.keyForValues(values)
}

func (self finder) get(c gaecontext.GAEContext, dst interface{}, values ...interface{}) (err error) {
	return self.getWithAncestor(c, dst, nil, values...)
}

func (self finder) getWithAncestor(c gaecontext.GAEContext, dst interface{}, ancestor *key.Key, values ...interface{}) (err error) {
	if len(values) != len(self.fields) {
		err = fmt.Errorf("%+v does not match %+v", values, self.fields)
		return
	}
	// We can't really cache finders that don't use ancestor fields, since they are eventually consistent which might fill the cache with inconsistent data
	if ancestor == nil {
		if err = self.find(c, dst, nil, values); err != nil {
			return
		}
	} else {
		if err = memcache.Memoize(c, self.keyForValues(values), dst, func() (result interface{}, err error) {
			if err = self.find(c, dst, ancestor, values); err == nil {
				result = dst
			}
			return
		}); err != nil {
			return
		}
	}
	val := reflect.ValueOf(dst).Elem()
	errors := appengine.MultiError{}
	for i := 0; i < val.Len(); i++ {
		el := val.Index(i)
		if err = runProcess(c, el.Addr().Interface(), afterLoadName); err != nil {
			errors = append(errors, err)
		}
	}
	if len(errors) > 0 {
		err = errors
	}
	return
}
