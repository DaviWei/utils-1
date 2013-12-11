package utils

import (
	"bytes"
	"crypto/sha512"
	"crypto/subtle"
	"encoding/base64"
	"encoding/gob"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
	"time"
)

func Prettify(obj interface{}) string {
	b, err := json.MarshalIndent(obj, "", "  ")
	if err != nil {
		return fmt.Sprintf("%+v", obj)
	}
	return string(b)
}

func InSlice(slice interface{}, needle interface{}) (result bool, err error) {
	sliceValue := reflect.ValueOf(slice)
	if sliceValue.Kind() != reflect.Slice {
		err = fmt.Errorf("%#v is not a slice", slice)
	}
	if sliceValue.Type().Elem() != reflect.TypeOf(needle) {
		err = fmt.Errorf("%#v is a slice of %#v", slice, needle)
	}
	for i := 0; i < sliceValue.Len(); i++ {
		if reflect.DeepEqual(sliceValue.Index(i).Interface(), needle) {
			result = true
			return
		}
	}
	return
}

type AccessToken interface {
	Encode() ([]byte, error)
}

type tokenEnvelope struct {
	ExpiresAt time.Time
	Hash      []byte
	Token     AccessToken
}

var secret []byte

func ParseAccessTokens(s []byte, token AccessToken) {
	secret = s
	gob.Register(token)
}

func EncodeToken(token AccessToken, timeout time.Duration) (result string, err error) {
	envelope := &tokenEnvelope{
		ExpiresAt: time.Now().Add(timeout),
		Token:     token,
	}
	h, err := envelope.generateHash()
	if err != nil {
		return
	}
	envelope.Hash = h
	b := &bytes.Buffer{}
	b64Enc := base64.NewEncoder(base64.URLEncoding, b)
	gobEnc := gob.NewEncoder(b64Enc)
	if err = gobEnc.Encode(envelope); err != nil {
		return
	}
	if err = b64Enc.Close(); err != nil {
		return
	}
	result = string(b.Bytes())
	return
}

func (self *tokenEnvelope) generateHash() (result []byte, err error) {
	hash := sha512.New()
	tokenCode, err := self.Token.Encode()
	if err != nil {
		return
	}
	if _, err = hash.Write(tokenCode); err != nil {
		return
	}
	if _, err = hash.Write(secret); err != nil {
		return
	}
	result = hash.Sum(nil)
	return
}

func ParseAccessToken(d string, dst interface{}) (err error) {
	envelope := &tokenEnvelope{}
	dec := gob.NewDecoder(base64.NewDecoder(base64.URLEncoding, bytes.NewBufferString(d)))
	if err = dec.Decode(&envelope); err != nil {
		err = fmt.Errorf("Invalid AccessToken: %v, %v", d, err)
		return
	}
	if envelope.ExpiresAt.Before(time.Now()) {
		err = fmt.Errorf("Expired AccessToken: %v", envelope)
		return
	}
	wantedHash, err := envelope.generateHash()
	if err != nil {
		return
	}
	if len(wantedHash) != len(envelope.Hash) || subtle.ConstantTimeCompare(envelope.Hash, wantedHash) != 1 {
		err = fmt.Errorf("Invalid AccessToken hash: %v should be %v", hex.EncodeToString(envelope.Hash), hex.EncodeToString(wantedHash))
		return
	}
	dstVal := reflect.ValueOf(dst)
	tokenVal := reflect.ValueOf(envelope.Token)
	if dstVal.Kind() != reflect.Ptr {
		err = fmt.Errorf("%#v is not a pointer", dst)
		return
	}
	if tokenVal.Kind() != reflect.Ptr {
		err = fmt.Errorf("%#v is not a pointer", tokenVal.Interface())
		return
	}
	if dstVal.Type() != tokenVal.Type() {
		err = fmt.Errorf("Can't load a %v into a %v", tokenVal.Type(), dstVal.Type())
		return
	}
	dstVal.Elem().Set(tokenVal.Elem())
	return
}

func ValidateFuncOutput(f interface{}, out []reflect.Type) error {
	fVal := reflect.ValueOf(f)
	if fVal.Kind() != reflect.Func {
		return fmt.Errorf("%v is not a func", f)
	}
	fType := fVal.Type()
	if fType.NumOut() != len(out) {
		return fmt.Errorf("%v should take %v arguments", f, len(out))
	}
	for index, outType := range out {
		if !fType.Out(index).AssignableTo(outType) {
			return fmt.Errorf("Return value %v for %v (%v) should be assignable to %v", index, f, fType.Out(index), outType)
		}
	}
	return nil
}

func ValidateFuncOutputs(f interface{}, outs ...[]reflect.Type) (errs []error) {
	for _, out := range outs {
		if err := ValidateFuncOutput(f, out); err != nil {
			errs = append(errs, err)
		}
	}
	return
}

func ValidateFuncInput(f interface{}, in []reflect.Type) error {
	fVal := reflect.ValueOf(f)
	if fVal.Kind() != reflect.Func {
		return fmt.Errorf("%v is not a func", f)
	}
	fType := fVal.Type()
	if fType.NumIn() != len(in) {
		return fmt.Errorf("%v should take %v arguments", f, len(in))
	}
	for index, inType := range in {
		if !fType.In(index).AssignableTo(inType) {
			return fmt.Errorf("Argument %v for %v (%v) should be assignable to %v", index, f, fType.In(index), inType)
		}
	}
	return nil
}

func ValidateFuncInputs(f interface{}, ins ...[]reflect.Type) (errs []error) {
	for _, in := range ins {
		if err := ValidateFuncInput(f, in); err != nil {
			errs = append(errs, err)
		}
	}
	return
}
