package goavro

type cursor []byte

func (f *cursor) DecodeBoolean() (v bool, e error) {
	var i interface{}
	if i, *f, e = booleanDecoder(*f); e == nil {
		v = i.(bool)
	}
	return
}

func (f *cursor) EncodeBoolean(v bool) error {
	*f, _ = booleanEncoder(*f, v) // only fails for bad type
	return nil
}
