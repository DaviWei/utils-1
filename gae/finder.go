package gae

import (
	"appengine"
	"appengine/datastore"
	"fmt"
	"github.com/soundtrackyourbrand/utils/gae/key"
	"github.com/soundtrackyourbrand/utils/gae/memcache"
	"reflect"
)

// finder encapsulates the knowledge that a model type is findable by a given set of fields.
type finder struct {
	fields []string
	model  Identified
}

// registeredFinders is used to find what cache keys to invalidate when a model is CRUDed.
var registeredFinders = map[string][]finder{}

// newFinder returns an optionally registered finder after having validated the correct type of input data.
func newFinder(model Identified, register bool, fields ...string) (result finder) {
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
	if register {
		name := reflect.TypeOf(model).Elem().Name()
		registeredFinders[name] = append(registeredFinders[name], result)
	}
	return
}

/*
Finder will return a finder function that runs a datastore query to find matching models.

The returned function will set the Id field of all found models, and call their AfterLoad functions if any.
*/
func Finder(model Identified, fields ...string) func(c PersistenceContext, dst interface{}, values ...interface{}) error {
	return newFinder(model, false, fields...).get
}

/*
AncestorFinder will return a finder function that memoizes running a datastore query to find matching models.

It will also register the finder so that MemcacheKeys will return keys to invalidate the result each time a matching model is CRUDed.

The returned function will set the Id field of all found models, and call their AfterLoad functions if any.
*/
func AncestorFinder(model Identified, fields ...string) func(c PersistenceContext, dst interface{}, ancestor *key.Key, values ...interface{}) error {
	return newFinder(model, true, fields...).getWithAncestor
}

// find runs a datastore query, if ancestor != nil an ancestor query, and sets the id of all found models.
func (self finder) find(c PersistenceContext, dst interface{}, ancestor *key.Key, values []interface{}) (err error) {
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
	dstElem := reflect.ValueOf(dst).Elem()
	var element reflect.Value
	for index, id := range ids {
		element = dstElem.Index(index)
		if element.Kind() == reflect.Ptr {
			element = element.Elem()
		}
		element.FieldByName(idFieldName).Set(reflect.ValueOf(key.FromGAE(id)))
	}
	return
}

// keyForValues returns the memcache key to use for the given ancestor and values searched for
func (self finder) keyForValues(ancestor *key.Key, values []interface{}) string {
	return fmt.Sprintf("%v{Ancestor:%v,%+v:%+v}", reflect.TypeOf(self.model).Name(), ancestor, self.fields, values)
}

// cacheKeys will append to oldKeys, and also return as newKeys, all cache keys this finder may use to find the provided model.
// the reason there may be multiple keys is that we don't know which ancestor will be used when finding the model.
func (self finder) cacheKeys(c PersistenceContext, model Identified, oldKeys *[]string) (newKeys []string, err error) {
	id := model.GetId()
	values := make([]interface{}, len(self.fields))
	val := reflect.ValueOf(model).Elem()
	for index, field := range self.fields {
		values[index] = val.FieldByName(field).Interface()
	}
	if oldKeys == nil {
		oldKeys = &[]string{}
	}
	for id != nil {
		*oldKeys = append(*oldKeys, self.keyForValues(id.Parent(), values))
		id = id.Parent()
	}
	newKeys = *oldKeys
	return
}

// see Finder
func (self finder) get(c PersistenceContext, dst interface{}, values ...interface{}) (err error) {
	return self.getWithAncestor(c, dst, nil, values...)
}

// see AncestorFinder
func (self finder) getWithAncestor(c PersistenceContext, dst interface{}, ancestor *key.Key, values ...interface{}) (err error) {
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
		if err = memcache.Memoize(c, self.keyForValues(ancestor, values), dst, func() (result interface{}, err error) {
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
		if err = runProcess(c, el.Addr().Interface().(Identified), AfterLoadName); err != nil {
			errors = append(errors, err)
		}
	}
	if len(errors) > 0 {
		err = errors
	}
	return
}
