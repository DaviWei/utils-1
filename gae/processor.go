package gae

import (
	"fmt"
	"github.com/soundtrackyourbrand/utils/gae/memcache"
	"reflect"
)

const (
	beforeCreateName = "BeforeCreate"
	beforeUpdateName = "BeforeUpdate"
	beforeSaveName   = "BeforeSave"
	afterLoadName    = "AfterLoad"
)

var processors = []string{
	beforeCreateName,
	beforeUpdateName,
	beforeSaveName,
	afterLoadName,
}

func runProcess(c memcache.TransactionContext, model interface{}, name string) error {
	if process, found, err := getProcess(model, name); err != nil {
		return err
	} else if found {
		results := process.Call([]reflect.Value{reflect.ValueOf(c)})
		if len(results) > 1 {
			if !results[len(results)-1].IsNil() {
				return results[len(results)-1].Interface().(error)
			}
		}
	}
	return nil
}

func getProcess(model interface{}, name string) (process reflect.Value, found bool, err error) {
	val := reflect.ValueOf(model)
	if process = val.MethodByName(name); process.IsValid() {
		processType := process.Type()
		if processType.NumIn() != 1 {
			err = fmt.Errorf("%+v#%v doesn't take exactly one argument", model, name)
			return
		}
		if !processType.In(0).Implements(reflect.TypeOf((*memcache.TransactionContext)(nil)).Elem()) {
			err = fmt.Errorf("%+v#%v takes a %v, not a %v as argument", model, name, processType.In(0))
			return
		}
		if processType.NumOut() < 1 {
			err = fmt.Errorf("%+v#%v doesn't produce at least one return value", model, name)
			return
		}
		if !processType.Out(0).AssignableTo(val.Type()) {
			err = fmt.Errorf("%+v#%v doesn't return a %v as first return value", model, name, reflect.TypeOf(model))
			return
		}
		if processType.NumOut() > 1 && !processType.Out(processType.NumOut()-1).AssignableTo(reflect.TypeOf((*error)(nil)).Elem()) {
			err = fmt.Errorf("%+v#%v doesn't return an error as last return value", model, name)
			return
		}
		found = true
	}
	return
}

func validateProcessors(model interface{}) (err error) {
	for _, name := range processors {
		if _, _, err = getProcess(model, name); err != nil {
			return
		}
	}
	return
}
