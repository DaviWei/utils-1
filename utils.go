package utils

import (
	"encoding/json"
	"fmt"
	"reflect"
)

func Prettify(obj interface{}) string {
	b, err := json.MarshalIndent(obj, "", "  ")
	if err != nil {
		return fmt.Sprintf("%+v", obj)
	}
	return string(b)
}

func InSlice(slice interface{}, needle interface{}) bool {
	sliceValue := reflect.ValueOf(slice)
	if sliceValue.Kind() != reflect.Slice {
		panic(fmt.Errorf("%#v is not a slice", slice))
	}
	if sliceValue.Type().Elem() != reflect.TypeOf(needle) {
		panic(fmt.Errorf("%#v is a slice of %#v", slice, needle))
	}
	for i := 0; i < sliceValue.Len(); i++ {
		if reflect.DeepEqual(sliceValue.Index(i).Interface(), needle) {
			return true
		}
	}
	return false
}
