package gae

import (
	"fmt"
	"reflect"
)

const (
	BeforeCreateName = "BeforeCreate"
	BeforeUpdateName = "BeforeUpdate"
	BeforeSaveName   = "BeforeSave"
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
	AfterCreateName,
	AfterUpdateName,
	AfterSaveName,
	AfterLoadName,
	AfterDeleteName,
}

func runProcess(c PersistenceContext, model Identified, name string) error {
	contextFunc := reflect.ValueOf(c).MethodByName(name).Interface().(func(Identified) error)
	if err := contextFunc(model); err != nil {
		return err
	}
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

func getProcess(model Identified, name string) (process reflect.Value, found bool, err error) {
	val := reflect.ValueOf(model)
	if process = val.MethodByName(name); process.IsValid() {
		processType := process.Type()
		if processType.NumIn() != 1 {
			err = fmt.Errorf("%+v#%v doesn't take exactly one argument", model, name)
			return
		}
		if !processType.In(0).Implements(reflect.TypeOf((*PersistenceContext)(nil)).Elem()) {
			err = fmt.Errorf("%+v#%v takes a %v, not a %v as argument", model, name, processType.In(0))
			return
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

func validateProcessors(model Identified) (err error) {
	for _, name := range processors {
		if _, _, err = getProcess(model, name); err != nil {
			return
		}
	}
	return
}
