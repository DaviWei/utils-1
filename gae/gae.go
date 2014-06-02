package gae

import (
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/soundtrackyourbrand/utils"
	"github.com/soundtrackyourbrand/utils/gae/memcache"
	"github.com/soundtrackyourbrand/utils/key"
	"github.com/soundtrackyourbrand/utils/key/gaekey"
	"github.com/soundtrackyourbrand/utils/web/httpcontext"

	"appengine"
	"appengine/datastore"
	"appengine/log"
)

const (
	idFieldName = "Id"
)

func GetKinds(c appengine.Context) (result []string, err error) {
	ids, err := datastore.NewQuery("__Stat_Kind__").KeysOnly().GetAll(c, nil)
	if err != nil {
		return
	}
	for _, id := range ids {
		result = append(result, id.StringID())
	}
	return
}

type PersistenceContext interface {
	memcache.TransactionContext
	AfterCreate(interface{}) error
	AfterSave(interface{}) error
	AfterUpdate(interface{}) error
	BeforeCreate(interface{}) error
	BeforeSave(interface{}) error
	BeforeUpdate(interface{}) error
	AfterLoad(interface{}) error
	AfterDelete(interface{}) error
	BeforeDelete(interface{}) error
}

type StatusMap map[int32]int

func (self StatusMap) MarshalJSON() (b []byte, err error) {
	tmpMap := map[string]interface{}{}
	for status, num := range self {
		tmpMap[fmt.Sprint(status)] = num
	}
	return json.Marshal(tmpMap)
}

type LogStats struct {
	Records      int
	From         time.Time
	To           time.Time
	Max          int
	Statuses     StatusMap
	TotalLatency time.Duration
	MaxLatency   time.Duration
	MinLatency   time.Duration
	TotalCost    float64
	MaxCost      float64
	MinCost      float64
}

func GetLogStats(c appengine.Context, from, to time.Time, max int, includeDelayTasks bool) (result *LogStats) {
	result = &LogStats{
		Statuses: StatusMap{},
		From:     from,
		To:       to,
		Max:      max,
	}
	query := &log.Query{StartTime: from, EndTime: to}
	res := query.Run(c)
	for rec, err := res.Next(); err == nil; rec, err = res.Next() {
		if includeDelayTasks || rec.Resource != "/_ah/queue/go/delay" {
			result.Records++
			result.Statuses[rec.Status]++
			result.TotalLatency += rec.Latency
			if result.MaxLatency == 0 || rec.Latency > result.MaxLatency {
				result.MaxLatency = rec.Latency
			}
			if result.MinLatency == 0 || rec.Latency < result.MinLatency {
				result.MinLatency = rec.Latency
			}
			result.TotalCost += rec.Cost
			if result.MaxCost == 0 || rec.Cost > result.MaxCost {
				result.MaxCost = rec.Cost
			}
			if result.MinCost == 0 || rec.Cost < result.MinCost {
				result.MinCost = rec.Cost
			}
			if result.Records >= max {
				break
			}
		}
	}
	return
}

// getTypeAndId will validate that the model is a pointer to a struct, and that it has a key.Key field name Id.
func getTypeAndId(model interface{}) (typ reflect.Type, id key.Key, err error) {
	val := reflect.ValueOf(model)
	if val.Kind() != reflect.Ptr {
		err = utils.Errorf("%+v is not a pointer", model)
		return
	}
	if val.Elem().Kind() != reflect.Struct {
		err = utils.Errorf("%+v is not a pointer to a struct", model)
		return
	}
	typ = val.Elem().Type()
	idField := val.Elem().FieldByName(idFieldName)
	if !idField.IsValid() {
		err = utils.Errorf("%+v does not have a field named Id", model)
		return
	}
	if !idField.Type().AssignableTo(reflect.TypeOf(key.Key(""))) {
		err = utils.Errorf("%+v has an Id field of type %v, that isn't assignable to key.Key", model, idField.Type())
		return
	}
	id = idField.Interface().(key.Key)
	return
}

/*
MemcacheKeys will append to oldKeys, and also return as newKeys, any memcache keys this package knows about that would
result in the provided model being found.

It will use the id based key, and any memcache keys provided by finders created by Finder or AncestorFinder.
*/
func MemcacheKeys(c PersistenceContext, model interface{}, oldKeys *[]string) (newKeys []string, err error) {
	if oldKeys == nil {
		oldKeys = &[]string{}
	}
	newKey, err := keyById(model)
	if err != nil {
		return
	}
	*oldKeys = append(*oldKeys, newKey)
	for _, finder := range registeredFinders[reflect.TypeOf(model).Elem().Name()] {
		if _, err = finder.cacheKeys(c, model, oldKeys); err != nil {
			return
		}
	}
	newKeys = *oldKeys
	return
}

func MemcacheDel(c PersistenceContext, model interface{}) (err error) {
	var keys []string
	if keys, err = MemcacheKeys(c, model, nil); err != nil {
		return
	}
	return memcache.Del(c, keys...)
}

// keyById will return the memcache key used to find dst by id.
func keyById(dst interface{}) (result string, err error) {
	typ, id, err := getTypeAndId(dst)
	if err != nil {
		return
	}
	result = fmt.Sprintf("%s{Id:%v}", typ.Name(), id)
	return
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
	Id    key.Key
}

func (self ErrNoSuchEntity) Error() string {
	return fmt.Sprintf("No %v with id %v found", self.Type, self.Id)
}

func (self ErrNoSuchEntity) Respond(c httpcontext.HTTPContextLogger) (err error) {
	c.Resp().WriteHeader(404)
	_, err = fmt.Fprint(c.Resp(), self.Error())
	return
}

func newError(dst interface{}, cause error) (err error) {
	var typ reflect.Type
	var id key.Key
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
func Del(c PersistenceContext, src interface{}) (err error) {
	var typ reflect.Type
	var id key.Key
	if typ, id, err = getTypeAndId(src); err != nil {
		return
	}
	if id == "" {
		err = utils.Errorf("%+v doesn't have an Id", src)
		return
	}
	gaeKey := gaekey.ToGAE(c, id)
	if !gaeKey.Incomplete() {
		old := reflect.New(typ)
		old.Elem().FieldByName(idFieldName).Set(reflect.ValueOf(id))
		err = GetById(c, old.Interface())
		if _, ok := err.(ErrNoSuchEntity); ok {
			err = nil
		} else if err == nil {
			if err = runProcess(c, old.Interface(), BeforeDeleteName, nil); err != nil {
				return
			}
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
		if err = runProcess(c, old.Interface(), AfterDeleteName, nil); err != nil {
			return
		}
	}
	return
}

/*
PutMulti will save src in datastore, invalidating cache and running hooks.
This requires the loading of any old versions currently in the datastore, which will
cause some extra work.
*/
func PutMulti(c PersistenceContext, src interface{}) (err error) {
	// validate
	srcVal := reflect.ValueOf(src)
	if srcVal.Kind() != reflect.Slice {
		err = utils.Errorf("%+v is not a slice", src)
		return
	}
	if srcVal.Type().Elem().Kind() != reflect.Ptr {
		err = utils.Errorf("%+v is not a slice of pointers", src)
		return
	}
	if srcVal.Type().Elem().Elem().Kind() != reflect.Struct {
		err = utils.Errorf("%+v is not a slice of struct pointers", src)
		return
	}
	// build required data for loading old entities
	gaeKeys := make([]*datastore.Key, srcVal.Len())
	ids := make([]key.Key, srcVal.Len())
	keysToLoad := []*datastore.Key{}
	indexMapping := []int{}
	for i := 0; i < srcVal.Len(); i++ {
		var id key.Key
		if _, id, err = getTypeAndId(srcVal.Index(i).Interface()); err != nil {
			return
		}
		if id == "" {
			err = utils.Errorf("%+v doesn't have an id")
			return
		}
		ids[i] = id
		gaeKey := gaekey.ToGAE(c, id)
		gaeKeys[i] = gaeKey
		if !gaeKey.Incomplete() {
			keysToLoad = append(keysToLoad, gaeKey)
			indexMapping = append(indexMapping, i)
		}
	}
	// load old entities
	memcacheKeys := []string{}
	oldIfs := make([]interface{}, srcVal.Len())
	if len(keysToLoad) > 0 {
		oldEntities := reflect.MakeSlice(reflect.SliceOf(srcVal.Type().Elem().Elem()), len(keysToLoad), len(keysToLoad))
		getErr := datastore.GetMulti(c, keysToLoad, oldEntities.Interface())
		// check which entities weren't in the database
		notFound := make([]bool, len(keysToLoad))
		if getErr != nil {
			if multiErr, ok := getErr.(appengine.MultiError); ok {
				for index, e := range multiErr {
					if e == datastore.ErrNoSuchEntity {
						notFound[index] = true
					} else {
						err = e
						return
					}
				}
			} else {
				err = getErr
				return
			}
		}
		// put entities inside oldIfs, run AfterLoad, add memcache keys from the old entities
		for index, _ := range keysToLoad {
			if !notFound[index] {
				if idField := oldEntities.Index(index).FieldByName(idFieldName); idField.IsValid() {
					idField.Set(reflect.ValueOf(ids[indexMapping[index]]))
				}
				oldIf := oldEntities.Index(index).Addr().Interface()
				oldIfs[indexMapping[index]] = oldIf
				if err = runProcess(c, oldIf, AfterLoadName, nil); err != nil {
					return
				}
				if _, err = MemcacheKeys(c, oldIf, &memcacheKeys); err != nil {
					return
				}
			}
		}
	}
	// run the before hooks
	for i := 0; i < srcVal.Len(); i++ {
		if oldIfs[i] == nil {
			if err = runProcess(c, srcVal.Index(i).Interface(), BeforeCreateName, nil); err != nil {
				return
			}
		} else {
			if err = runProcess(c, srcVal.Index(i).Interface(), BeforeUpdateName, oldIfs[i]); err != nil {
				return
			}
		}
		if err = runProcess(c, srcVal.Index(i).Interface(), BeforeSaveName, oldIfs[i]); err != nil {
			return
		}
	}
	// actually save
	if gaeKeys, err = datastore.PutMulti(c, gaeKeys, src); err != nil {
		return
	}
	// set ids and add memcache keys from the new entities
	for i := 0; i < srcVal.Len(); i++ {
		if ids[i], err = gaekey.FromGAE(gaeKeys[i]); err != nil {
			return
		}
		srcVal.Index(i).Elem().FieldByName(idFieldName).Set(reflect.ValueOf(ids[i]))
		if _, err = MemcacheKeys(c, srcVal.Index(i).Interface(), &memcacheKeys); err != nil {
			return
		}
	}
	// clear memcache
	if err = memcache.Del(c, memcacheKeys...); err != nil {
		return
	}
	// run the after hooks
	for i := 0; i < srcVal.Len(); i++ {
		if oldIfs[i] == nil {
			if err = runProcess(c, srcVal.Index(i).Interface(), AfterCreateName, nil); err != nil {
				return
			}
		} else {
			if err = runProcess(c, srcVal.Index(i).Interface(), AfterUpdateName, oldIfs[i]); err != nil {
				return
			}
		}
		if err = runProcess(c, srcVal.Index(i).Interface(), AfterSaveName, oldIfs[i]); err != nil {
			return
		}
	}
	return
}

/*
Put will save src in datastore, invalidating cache and running hooks.
This requires the loading of any old versions currently in the datastore, which will
cause some extra work.
*/
func Put(c PersistenceContext, src interface{}) (err error) {
	var id key.Key
	if _, id, err = getTypeAndId(src); err != nil {
		return
	}
	if id == "" {
		err = utils.Errorf("%+v doesn't have an Id", src)
		return
	}
	gaeKey := gaekey.ToGAE(c, id)
	memcacheKeys := []string{}
	var oldIf interface{}
	if !gaeKey.Incomplete() {
		old := reflect.New(reflect.TypeOf(src).Elem())
		old.Elem().FieldByName(idFieldName).Set(reflect.ValueOf(id))
		err = GetById(c, old.Interface())
		if _, ok := err.(ErrNoSuchEntity); ok {
			err = nil
		} else if err == nil {
			oldIf = old.Interface()
			if _, err = MemcacheKeys(c, oldIf, &memcacheKeys); err != nil {
				return
			}
		} else {
			return
		}
	}
	if oldIf == nil {
		if err = runProcess(c, src, BeforeCreateName, nil); err != nil {
			return
		}
	} else {
		if err = runProcess(c, src, BeforeUpdateName, oldIf); err != nil {
			return
		}
	}
	if err = runProcess(c, src, BeforeSaveName, oldIf); err != nil {
		return
	}
	if id, err = gaekey.FromGAErr(datastore.Put(c, gaeKey, src)); err != nil {
		return
	}
	reflect.ValueOf(src).Elem().FieldByName(idFieldName).Set(reflect.ValueOf(id))
	if _, err = MemcacheKeys(c, src, &memcacheKeys); err != nil {
		return
	}
	if err = memcache.Del(c, memcacheKeys...); err != nil {
		return
	}
	if oldIf == nil {
		if err = runProcess(c, src, AfterCreateName, nil); err != nil {
			return
		}
	} else {
		if err = runProcess(c, src, AfterUpdateName, oldIf); err != nil {
			return
		}
	}
	return runProcess(c, src, AfterSaveName, oldIf)
}

// findById will find dst in the datastore and set its id.
func findById(c PersistenceContext, dst interface{}) (err error) {
	var id key.Key
	if _, id, err = getTypeAndId(dst); err != nil {
		return
	}
	if err = datastore.Get(c, gaekey.ToGAE(c, id), dst); err == datastore.ErrNoSuchEntity {
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
func GetById(c PersistenceContext, dst interface{}) (err error) {
	k, err := keyById(dst)
	if err != nil {
		return
	}
	if err = memcache.Memoize(c, k, dst, func() (result interface{}, err error) {
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
		err = runProcess(c, dst, AfterLoadName, nil)
	} else if err == memcache.ErrCacheMiss {
		err = newError(dst, datastore.ErrNoSuchEntity)
	}
	return
}

func DelAll(c PersistenceContext, src interface{}) (err error) {
	srcTyp := reflect.TypeOf(src)
	if srcTyp.Kind() != reflect.Ptr {
		err = utils.Errorf("%+v is not a pointer", src)
		return
	}
	if srcTyp.Elem().Kind() != reflect.Struct {
		err = utils.Errorf("%+v is not a pointer to a struct", src)
		return
	}
	return DelQuery(c, src, datastore.NewQuery(reflect.TypeOf(src).Elem().Name()))
}

func GetMulti(c PersistenceContext, ids []key.Key, src interface{}) (err error) {
	dsIds := make([]*datastore.Key, len(ids))
	for index, id := range ids {
		dsIds[index] = gaekey.ToGAE(c, id)
	}
	if err = datastore.GetMulti(c, dsIds, src); err != nil {
		return
	}
	srcVal := reflect.ValueOf(src)
	for index, id := range ids {
		el := srcVal.Index(index)
		el.FieldByName("Id").Set(reflect.ValueOf(id))
		if err = runProcess(c, el.Addr().Interface(), AfterLoadName, nil); err != nil {
			return
		}
	}
	return
}

func GetAll(c PersistenceContext, src interface{}) (err error) {
	srcTyp := reflect.TypeOf(src)
	if srcTyp.Kind() != reflect.Ptr {
		err = utils.Errorf("%+v is not a pointer", src)
		return
	}
	if srcTyp.Elem().Kind() != reflect.Slice {
		err = utils.Errorf("%+v is not a pointer to a slice", src)
		return
	}
	if srcTyp.Elem().Elem().Kind() != reflect.Ptr {
		err = utils.Errorf("%+v is not a pointer to a slice of pointers", src)
		return
	}
	if srcTyp.Elem().Elem().Elem().Kind() != reflect.Struct {
		err = utils.Errorf("%+v is not a pointer to a slice of struct pointers", src)
		return
	}
	return GetQuery(c, src, datastore.NewQuery(reflect.TypeOf(src).Elem().Elem().Elem().Name()))
}

func GetQuery(c PersistenceContext, src interface{}, q *datastore.Query) (err error) {
	var dataIds []*datastore.Key
	dataIds, err = q.GetAll(c, src)
	if err = FilterOkErrors(err); err != nil {
		return
	}
	srcVal := reflect.ValueOf(src)
	for index, dataId := range dataIds {
		el := srcVal.Elem().Index(index)
		var k key.Key
		if k, err = gaekey.FromGAE(dataId); err != nil {
			return
		}
		el.Elem().FieldByName("Id").Set(reflect.ValueOf(k))
		if err = runProcess(c, el.Interface(), AfterLoadName, nil); err != nil {
			return
		}
	}
	return
}

// DelQuery will delete (from datastore and memcache) all entities of type src that matches q.
// src must be a pointer to a struct type.
func DelQuery(c PersistenceContext, src interface{}, q *datastore.Query) (err error) {
	var dataIds []*datastore.Key
	results := reflect.New(reflect.SliceOf(reflect.TypeOf(src).Elem()))
	dataIds, err = q.GetAll(c, results.Interface())
	if err = FilterOkErrors(err); err != nil {
		return
	}
	memcacheKeys := []string{}
	var el reflect.Value
	resultsSlice := results.Elem()
	for index, dataId := range dataIds {
		el = resultsSlice.Index(index)
		var k key.Key
		if k, err = gaekey.FromGAE(dataId); err != nil {
			return
		}
		el.FieldByName("Id").Set(reflect.ValueOf(k))
		if _, err = MemcacheKeys(c, el.Addr().Interface(), &memcacheKeys); err != nil {
			return
		}
		if err = runProcess(c, el.Addr().Interface(), BeforeDeleteName, nil); err != nil {
			return
		}
	}
	if err = datastore.DeleteMulti(c, dataIds); err != nil {
		return
	}
	for index, _ := range dataIds {
		el = resultsSlice.Index(index)
		if err = runProcess(c, el.Addr().Interface(), AfterDeleteName, nil); err != nil {
			return
		}
	}
	return memcache.Del(c, memcacheKeys...)
}
