package gaekey

import (
	"appengine"
	"appengine/datastore"
	"github.com/soundtrackyourbrand/utils/key"
)

func FromGAErr(k *datastore.Key, err error) (result key.Key, err2 error) {
	err2 = err
	if err2 == nil {
		return FromGAE(k)
	}
	return
}

func FromGAE(k *datastore.Key) (result key.Key, err error) {
	if k == nil {
		return key.Key(""), nil
	}
	parent, err := FromGAE(k.Parent())
	if err != nil {
		return
	}
	return key.New(k.Kind(), k.StringID(), k.IntID(), parent)
}

func ToGAE(c appengine.Context, k key.Key) *datastore.Key {
	if len(k) < 1 {
		return nil
	}
	kind, stringID, intID, parent := k.Split()
	return datastore.NewKey(c, kind, stringID, intID, ToGAE(c, key.Key(parent)))
}
