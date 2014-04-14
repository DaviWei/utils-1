package gae

import (
	"fmt"
	"reflect"
)

const (
	BeforeCreateName = "BeforeCreate"
	BeforeUpdateName = "BeforeUpdate"
	BeforeSaveName   = "BeforeSave"
	BeforeDeleteName = "BeforeDelete"
	AfterCreateName  = "AfterCreate"
	AfterUpdateName  = "AfterUpdate"
	AfterSaveName    = "AfterSave"
	AfterLoadName    = "AfterLoad"
	AfterDeleteName  = "AfterDelete"
)

var processors = []string{
	BeforeCreateName,
	BeforeUpdateName,
	BeforeSaveName,
	BeforeDeleteName,
	AfterCreateName,
	AfterUpdateName,
	AfterSaveName,
	AfterLoadName,
	AfterDeleteName,
}

func runProcess(c PersistenceContext, model interface{}, name string, arg interface{}) error {
	contextFunc := reflect.ValueOf(c).MethodByName(name).Interface().(func(interface{}) error)
	if err := contextFunc(model); err != nil {
		return err
	}
	if process, found, err := getProcess(model, name, arg); err != nil {
		return err
	} else if found {
		var results []reflect.Value
		if process.Type().NumIn() == 2 {
			results = process.Call([]reflect.Value{reflect.ValueOf(c), reflect.ValueOf(arg)})
		} else {
			results = process.Call([]reflect.Value{reflect.ValueOf(c)})
		}
		if len(results) > 1 {
			if !results[len(results)-1].IsNil() {
				return results[len(results)-1].Interface().(error)
			}
		}
	}
	return nil
}

func getProcess(model interface{}, name string, arg interface{}) (process reflect.Value, found bool, err error) {
	val := reflect.ValueOf(model)
	if process = val.MethodByName(name); process.IsValid() {
		processType := process.Type()
		if processType.NumIn() == 2 {
			if arg == nil {
				err = fmt.Errorf("%+v#%v takes two arguments, but we don't have a second argument!", model, name)
			}
			if !processType.In(0).Implements(reflect.TypeOf((*PersistenceContext)(nil)).Elem()) {
				err = fmt.Errorf("%+v#%v takes a %v, not a PersistenceContext as first argument", model, name, processType.In(0))
				return
			}
			if !reflect.TypeOf(arg).AssignableTo(processType.In(1)) {
				err = fmt.Errorf("%+v#%v takes a %v, not a %v as second argument", model, name, processType.In(0), reflect.TypeOf(arg))
				return
			}
		} else if processType.NumIn() == 1 {
			if !processType.In(0).Implements(reflect.TypeOf((*PersistenceContext)(nil)).Elem()) {
				err = fmt.Errorf("%+v#%v takes a %v, not a %v as argument", model, name, processType.In(0))
				return
			}

		}
		if processType.NumOut() != 1 {
			err = fmt.Errorf("%+v#%v doesn't produce exactly one return value", model, name)
			return
		}
		if !processType.Out(0).AssignableTo(reflect.TypeOf((*error)(nil)).Elem()) {
			err = fmt.Errorf("%+v#%v doesn't return an error", model, name)
			return
		}
		found = true
	}
	return
}
