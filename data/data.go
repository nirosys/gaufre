package data

type Data interface {
	GetValue() (DataValue, bool)
}

// DataValue is implemented by the application built ontop of gaufre.
type DataValue interface {
	Value() interface{}
}

type DataWrapper struct {
	Data interface{}
}

func (d *DataWrapper) Value() interface{} {
	if d == nil {
		return nil
	}
	return d.Data
}

func (d *DataWrapper) Tag() *string {
	return nil
}

func (d *DataWrapper) IsMap() bool {
	return false
}

func (d *DataWrapper) GetValue() (DataValue, bool) {
	return d, true
}

func (d *DataWrapper) GetNamedValue(n string) (DataValue, bool) {
	return nil, false
}
