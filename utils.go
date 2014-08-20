package utils

import (
	"bytes"
	"crypto/sha512"
	"crypto/subtle"
	"encoding/base64"
	"encoding/gob"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"math/big"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"text/template"
	"time"

	"net/http"

	"github.com/soundtrackyourbrand/utils/run"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

const (
	randomChars            = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	NonConfusingCharacters = "356789ABCDEFGHJLMNOPRSTUVWYZabcdefghijkmnopqrstuvwxyz"
)

var camelRegUl = regexp.MustCompile("^([A-Z0-9][a-z0-9]*)(.*)$")
var camelReglU = regexp.MustCompile("^([a-z0-9]*)(.*)$")
var camelRegUUx = regexp.MustCompile("^([A-Z0-9][A-Z0-9]+)$")
var camelRegUU = regexp.MustCompile("^([A-Z0-9][A-Z0-9]+)(.*)$")

func IsNil(i interface{}) bool {
	if i == nil {
		return true
	}
	val := reflect.ValueOf(i)
	switch val.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Ptr, reflect.Slice:
		return val.IsNil()
	}
	return false
}

func Stack() string {
	buf := make([]byte, 1<<14)
	generated := runtime.Stack(buf, true)
	return string(buf[:generated])
}

func CamelToSnake(s string) (string, error) {
	resultSlice := []string{}
	i := 0
	for len(s) > 0 {
		i++
		if i > 50 {
			return s, Errorf("%#v doesn't seem possible to convert to snake case?", s)
		}
		if match := camelRegUUx.FindStringSubmatch(s); match != nil {
			resultSlice = append(resultSlice, strings.ToLower(match[1]))
			s = ""
		} else if match := camelRegUU.FindStringSubmatch(s); match != nil {
			resultSlice = append(resultSlice, strings.ToLower(match[1][:len(match[1])-1]))
			s = match[1][len(match[1])-1:] + match[2]
		} else if match := camelRegUl.FindStringSubmatch(s); match != nil {
			resultSlice = append(resultSlice, strings.ToLower(match[1]))
			s = match[2]
		} else if match := camelReglU.FindStringSubmatch(s); match != nil {
			resultSlice = append(resultSlice, match[1])
			s = match[2]
		}
	}
	return strings.Join(resultSlice, "_"), nil
}

func RandomString(i int) string {
	buf := new(bytes.Buffer)
	for buf.Len() < i {
		fmt.Fprintf(buf, "%c", randomChars[rand.Intn(len(randomChars))])
	}
	return string(buf.Bytes())
}

func RandomStringFrom(chars string, i int) string {
	buf := new(bytes.Buffer)
	for buf.Len() < i {
		fmt.Fprintf(buf, "%c", chars[rand.Intn(len(chars))])
	}
	return string(buf.Bytes())
}

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
		err = Errorf("%#v is not a slice", slice)
	}
	if sliceValue.Type().Elem() != reflect.TypeOf(needle) {
		err = Errorf("%#v is a slice of %#v", slice, needle)
	}
	for i := 0; i < sliceValue.Len(); i++ {
		if reflect.DeepEqual(sliceValue.Index(i).Interface(), needle) {
			result = true
			return
		}
	}
	return
}

func ReflectCopy(source, destinationPointer interface{}) {
	srcValue := reflect.ValueOf(source)
	if reflect.PtrTo(reflect.TypeOf(source)) == reflect.TypeOf(destinationPointer) {
		reflect.ValueOf(destinationPointer).Elem().Set(srcValue)
	} else {
		reflect.ValueOf(destinationPointer).Elem().Set(reflect.Indirect(srcValue))
	}
}

type AccessToken interface {
	Encode() ([]byte, error)
	Scopes() []string
}

type tokenEnvelope struct {
	ExpiresAt time.Time
	Hash      []byte
	Token     AccessToken
}

var secret []byte
var accessTokenType reflect.Type

func ParseAccessTokens(s []byte, token AccessToken) {
	secret = s
	accessTokenType = reflect.TypeOf(token)
	if accessTokenType.Kind() != reflect.Ptr || accessTokenType.Elem().Kind() != reflect.Struct {
		panic(Errorf("%v is not a pointer to a struct", token))
	}
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
	result = strings.Replace(string(b.Bytes()), "=", ".", -1)
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

/*
ParseAccessToken will return the AccessToken encoded in d. If dst is provided it will encode into it.
*/
func ParseAccessToken(d string, dst AccessToken) (result AccessToken, err error) {
	if dst == nil {
		dst = reflect.New(accessTokenType.Elem()).Interface().(AccessToken)
	}
	result = dst
	envelope := &tokenEnvelope{}
	dec := gob.NewDecoder(base64.NewDecoder(base64.URLEncoding, bytes.NewBufferString(strings.Replace(d, ".", "=", -1))))
	if err = dec.Decode(&envelope); err != nil {
		err = Errorf("Invalid AccessToken: %v, %v", d, err)
		return
	}
	if envelope.ExpiresAt.Before(time.Now()) {
		err = Errorf("Expired AccessToken: %v", envelope)
		return
	}
	wantedHash, err := envelope.generateHash()
	if err != nil {
		return
	}
	if len(wantedHash) != len(envelope.Hash) || subtle.ConstantTimeCompare(envelope.Hash, wantedHash) != 1 {
		err = Errorf("Invalid AccessToken: hash of %+v should be %v but was %v", envelope.Token, hex.EncodeToString(envelope.Hash), hex.EncodeToString(wantedHash))
		return
	}
	dstVal := reflect.ValueOf(dst)
	tokenVal := reflect.ValueOf(envelope.Token)
	if dstVal.Kind() != reflect.Ptr {
		err = Errorf("%#v is not a pointer", dst)
		return
	}
	if tokenVal.Kind() != reflect.Ptr {
		err = Errorf("%#v is not a pointer", tokenVal.Interface())
		return
	}
	if dstVal.Type() != tokenVal.Type() {
		err = Errorf("Can't load a %v into a %v", tokenVal.Type(), dstVal.Type())
		return
	}
	dstVal.Elem().Set(tokenVal.Elem())
	return
}

type StackError interface {
	GetStack() string
	Error() string
}

type DefaultStackError struct {
	Source error
	Stack  string
}

func (self DefaultStackError) Error() string {
	return self.Source.Error() + "\n" + self.Stack
}

func (self DefaultStackError) GetStack() string {
	return self.Stack
}

func NewError(source error) StackError {
	if stackError, ok := source.(StackError); ok {
		return stackError
	}
	return DefaultStackError{
		Source: source,
		Stack:  Stack(),
	}
}

func Errorf(f string, args ...interface{}) StackError {
	return DefaultStackError{
		Source: fmt.Errorf(f, args...),
		Stack:  Stack(),
	}
}

func ValidateFuncOutput(f interface{}, out []reflect.Type) error {
	fVal := reflect.ValueOf(f)
	if fVal.Kind() != reflect.Func {
		return Errorf("%v is not a func", f)
	}
	fType := fVal.Type()
	if fType.NumOut() != len(out) {
		return Errorf("%v should take %v arguments", f, len(out))
	}
	for index, outType := range out {
		if !fType.Out(index).AssignableTo(outType) {
			return Errorf("Return value %v for %v (%v) should be assignable to %v", index, f, fType.Out(index), outType)
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
		return Errorf("%v is not a func", f)
	}
	fType := fVal.Type()
	if fType.NumIn() != len(in) {
		return Errorf("%v should take %v arguments", f, len(in))
	}
	for index, inType := range in {
		if !fType.In(index).AssignableTo(inType) {
			return Errorf("Argument %v for %v (%v) should be assignable to %v", index, f, fType.In(index), inType)
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

func Example(t reflect.Type) (result interface{}) {
	return example(t, map[string]int{})
}

func example(t reflect.Type, seen map[string]int) (result interface{}) {
	seen[t.Name()]++
	switch t.Kind() {
	case reflect.Slice:
		val := reflect.MakeSlice(t, 1, 1)
		result = val.Interface()
		if seen[t.Name()] > 2 {
			return
		}
		val.Index(0).Set(reflect.ValueOf(example(t.Elem(), seen)))
		result = val.Interface()
	case reflect.Ptr:
		val := reflect.New(t.Elem())
		result = val.Interface()
		if seen[t.Name()] > 2 {
			return
		}
		x := example(t.Elem(), seen)
		val.Elem().Set(reflect.ValueOf(x))
		result = val.Interface()
	case reflect.Interface:
		result = struct{}{}
	case reflect.String:
		result = reflect.ValueOf("[...]").Convert(t).Interface()
	case reflect.Int:
		result = reflect.ValueOf(1).Convert(t).Interface()
	case reflect.Int64:
		result = reflect.ValueOf(int64(1)).Convert(t).Interface()
	case reflect.Float64:
		result = reflect.ValueOf(float64(1)).Convert(t).Interface()
	case reflect.Bool:
		result = reflect.ValueOf(true).Convert(t).Interface()
	default:
		val := reflect.New(t)
		result = val.Elem().Interface()
		if seen[t.Name()] > 2 {
			return
		}
		if t.Kind() == reflect.Struct {
			for i := 0; i < t.NumField(); i++ {
				field := t.Field(i)
				if field.PkgPath == "" {
					val.Elem().Field(i).Set(reflect.ValueOf(example(field.Type, seen)))
				}
			}
		}
		result = val.Elem().Interface()
	}
	return
}

var revisionTemplate = template.Must(template.New("").Parse(`package {{.Package}}
import "time"

const (
	GitRevision = "{{.Revision}}"
	GitBranch = "{{.Branch}}"
)
var GitRevisionAt = time.Unix(0, {{.Time}})
`))

func GitCommitted(dir string) (result bool, err error) {
	_, _, err = run.RunAndReturn("git", "diff-index", "--quiet", "HEAD", "--")
	if err != nil {
		if _, ok := err.(*exec.ExitError); ok {
			err = nil
		}
		return
	}
	result = true
	return
}

func GitRevision(dir string) (rev string, err error) {
	revisionResult, _, err := run.RunAndReturn("git", "--git-dir", filepath.Join(dir, ".git"), "--work-tree", dir, "rev-parse", "HEAD")
	if err != nil {
		return
	}
	rev = strings.TrimSpace(revisionResult)
	return
}

func GitBranch(dir string) (branch string, err error) {
	branchResult, _, err := run.RunAndReturn("git", "--git-dir", filepath.Join(dir, ".git"), "--work-tree", dir, "rev-parse", "--abbrev-ref", "HEAD")
	if err != nil {
		return
	}
	branch = strings.TrimSpace(branchResult)
	return
}

func UpdateGitRevision(dir, destination string) (err error) {
	rev, err := GitRevision(dir)
	if err != nil {
		return
	}
	branch, err := GitBranch(dir)
	if err != nil {
		return
	}
	tmpDest := fmt.Sprintf("%v_%v", destination, rand.Int63())
	outfile, err := os.Create(tmpDest)
	if err != nil {
		return
	}
	if err = revisionTemplate.Execute(outfile, map[string]interface{}{
		"Package":  filepath.Base(filepath.Dir(destination)),
		"Revision": rev,
		"Branch":   branch,
		"Time":     time.Now().UnixNano(),
	}); err != nil {
		return
	}
	if err = outfile.Close(); err != nil {
		return
	}
	if err = os.Rename(tmpDest, destination); err != nil {
		return
	}
	return
}

type JSONTime time.Time

const (
	ISO8601DayTimeFormat  = "150405"
	ISO8601DateTimeFormat = "20060102150405"
	ISO8601DateFormat     = "20060102"
)

func (self JSONTime) MarshalJSON() ([]byte, error) {
	if time.Time(self).IsZero() {
		return json.Marshal(nil)
	}
	return json.Marshal(time.Time(self).Format(ISO8601DateTimeFormat))
}

func (self *JSONTime) UnmarshalJSON(b []byte) (err error) {
	var s string
	if err = json.Unmarshal(b, &s); err == nil {
		if s != "" {
			var t time.Time
			if t, err = time.Parse(ISO8601DateTimeFormat, s); err == nil {
				*self = JSONTime(t)
			}
		} else {
			*self = JSONTime(time.Time{})
		}
	}
	return
}

type Base64String string

func (self Base64String) Bytes() (result []byte, err error) {
	return base64.StdEncoding.DecodeString(string(self))
}

func (self Base64String) MarshalJSON() (result []byte, err error) {
	if _, err = base64.StdEncoding.DecodeString(string(self)); err != nil {
		return
	}
	return json.Marshal(string(self))
}

func (self Base64String) String() string {
	return string(self)
}

func (self *Base64String) UnmarshalJSON(b []byte) (err error) {
	if err = json.Unmarshal(b, self); err != nil {
		return err
	}
	_, err = base64.StdEncoding.DecodeString(string(*self))
	return
}

type ByteString struct {
	Bytes []byte
}

func (self ByteString) MarshalJSON() ([]byte, error) {
	return json.Marshal(string(self.Bytes))
}

func (self ByteString) String() string {
	return string(self.Bytes)
}

func (self *ByteString) UnmarshalJSON(b []byte) error {
	s := ""
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	self.Bytes = []byte(s)
	return nil
}

func EncodeBigInt(chars string, bigInt *big.Int) string {
	if bigInt.Cmp(big.NewInt(int64(len(chars)))) < 0 {
		return string(chars[bigInt.Int64()])
	}
	mod := big.NewInt(0)
	rest := big.NewInt(0)
	rest.DivMod(bigInt, big.NewInt(int64(len(chars))), mod)
	return EncodeBigInt(chars, rest) + string(chars[mod.Int64()])
}

func EncodeBytes(chars string, b []byte) string {
	bigInt := big.NewInt(int64(0))
	bigInt.SetBytes(b)
	return EncodeBigInt(chars, bigInt)
}

func DecodeBigInt(chars, encoded string) *big.Int {
	if len(encoded) == 0 {
		return big.NewInt(0)
	}
	least := big.NewInt(int64(strings.Index(chars, string(encoded[0]))))
	base := big.NewInt(int64(len(chars)))
	for i := 1; i < len(encoded); i++ {
		least.Mul(least, base)
	}
	return least.Add(least, DecodeBigInt(chars, encoded[1:]))
}

func DecodeBytes(chars, encoded string) []byte {
	return DecodeBigInt(chars, encoded).Bytes()
}

func ConstantTimeEqualString(s1, s2 string) bool {
	return ConstantTimeEqualBytes([]byte(s1), []byte(s2))
}

func ConstantTimeEqualBytes(b1, b2 []byte) bool {
	return len(b1) == len(b2) && subtle.ConstantTimeCompare(b1, b2) == 1
}

// For debugging use. Converts a http.Request to a curl string for copy'n'paste to terminal
func ToCurl(req *http.Request) string {
	bodyPart := ""
	if req.Body != nil {
		b, _ := ioutil.ReadAll(req.Body)
		req.Body = ioutil.NopCloser(bytes.NewBuffer(b))
		bodyPart = fmt.Sprintf(" -d %#v", string(b))
	}
	curl := fmt.Sprintf("curl -v -X%s %q%v", req.Method, req.URL, bodyPart)
	for header, vals := range req.Header {
		for _, val := range vals {
			curl = fmt.Sprintf(`%s -H "%s: %+v"`, curl, header, val)
		}
	}
	return curl
}

func GenerateFlags(i interface{}) (result []string, err error) {
	v := reflect.ValueOf(i)
	t := v.Type()

	if t.Kind() != reflect.Ptr {
		err = Errorf("Unable to ParseFlags into %v, it is not a pointer to a struct", v)
		return
	}

	t = t.Elem()
	v = v.Elem()

	if t.Kind() != reflect.Struct {
		err = Errorf("Unable to ParseFlags into %v, it is not a pointer to a struct", v)
		return
	}

	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		flagName := f.Name
		if explicitFlagName := f.Tag.Get("flag"); explicitFlagName != "" {
			flagName = explicitFlagName
		}
		result = append(result, fmt.Sprintf("-%v=%v", flagName, v.Field(i).Interface()))
	}

	return
}

func ParseFlags(i interface{}, defaultMap map[string]string) (err error) {
	v := reflect.ValueOf(i)
	t := v.Type()

	if t.Kind() != reflect.Ptr {
		err = Errorf("Unable to ParseFlags into %v, it is not a pointer to a struct", v)
		return
	}

	t = t.Elem()
	v = v.Elem()

	if t.Kind() != reflect.Struct {
		err = Errorf("Unable to ParseFlags into %v, it is not a pointer to a struct", v)
		return
	}

	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		flagName := f.Name
		if explicitFlagName := f.Tag.Get("flag"); explicitFlagName != "" {
			flagName = explicitFlagName
		}
		flagDesc := f.Name
		if explicitFlagDesc := f.Tag.Get("flag_desc"); explicitFlagDesc != "" {
			flagDesc = explicitFlagDesc
		}
		switch f.Type.Kind() {
		case reflect.Int:
			flagDefault := 0
			explicitFlagDefault := f.Tag.Get("flag_default")
			if providedFlagDefault, found := defaultMap[f.Name]; found {
				explicitFlagDefault = providedFlagDefault
			}
			if explicitFlagDefault != "" {
				if flagDefault, err = strconv.Atoi(explicitFlagDefault); err != nil {
					return
				}
			}
			flag.IntVar(v.Field(i).Addr().Interface().(*int), flagName, flagDefault, flagDesc)
		case reflect.String:
			flagDefault := ""
			if explicitFlagDefault := f.Tag.Get("flag_default"); explicitFlagDefault != "" {
				flagDefault = explicitFlagDefault
			}
			if providedFlagDefault, found := defaultMap[f.Name]; found {
				flagDefault = providedFlagDefault
			}
			flag.StringVar(v.Field(i).Addr().Interface().(*string), flagName, flagDefault, flagDesc)
		case reflect.Bool:
			flagDefault := false
			if explicitFlagDefault := f.Tag.Get("flag_default"); explicitFlagDefault != "" {
				flagDefault = explicitFlagDefault == "true"
			}
			if providedFlagDefault, found := defaultMap[f.Name]; found {
				flagDefault = providedFlagDefault == "true"
			}
			flag.BoolVar(v.Field(i).Addr().Interface().(*bool), flagName, flagDefault, flagDesc)
		default:
			err = Errorf("Unrecognized flag type for field %v of %v", f, v)
			return
		}
	}

	flag.Parse()
	return
}

type Parallelizer struct {
	funcs []func() error
	c     chan error
}

func (self *Parallelizer) Start(f func() error) {
	if self.c == nil {
		self.c = make(chan error)
	}
	self.funcs = append(self.funcs, f)
	go func() {
		self.c <- f()
	}()
}

type MultiError []error

func (self MultiError) Error() string {
	s := make([]string, len(self))
	for index, err := range self {
		s[index] = err.Error()
	}
	return strings.Join(s, ", ")
}

func (self *Parallelizer) Wait() (err error) {
	merr := MultiError{}
	for _, _ = range self.funcs {
		if e := <-self.c; e != nil {
			merr = append(merr, e)
		}
	}
	if len(merr) > 0 {
		err = merr
		return
	}
	return
}
