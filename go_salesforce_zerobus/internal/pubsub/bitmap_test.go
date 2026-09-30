package pubsub

import (
	"reflect"
	"testing"
)

const bitmapSchemaJSON = `{"type":"record","name":"AccountChangeEvent","fields":[
 {"name":"ChangeEventHeader","type":"string"},
 {"name":"Name","type":["null","string"]},
 {"name":"Type","type":["null","string"]},
 {"name":"BillingAddress","type":["null",{"type":"record","name":"Address","fields":[
   {"name":"Street","type":["null","string"]},{"name":"City","type":["null","string"]},{"name":"State","type":["null","string"]}]}]},
 {"name":"Phone","type":["null","string"]}
]}`

func TestBitmapFieldNames(t *testing.T) {
	bs, err := ParseBitmapSchema(bitmapSchemaJSON)
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name    string
		bitmaps []string
		want    []string
	}{
		{"empty", nil, nil},
		{"single bit", []string{"0x02"}, []string{"Name"}},
		{"multiple bits", []string{"0x16"}, []string{"Name", "Type", "Phone"}},
		{"multi-digit ordering", []string{"0x10"}, []string{"Phone"}},
		{"nested", []string{"0x08", "3-0x06"}, []string{"BillingAddress", "BillingAddress.City", "BillingAddress.State"}},
		{"out of range ignored", []string{"0xFF00"}, nil},
		{"bad nested parent", []string{"x-0x01", "9-0x01"}, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := bs.FieldNames(tt.bitmaps); !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("FieldNames(%v) = %v, want %v", tt.bitmaps, got, tt.want)
			}
		})
	}
}

func TestSetBitsMatchesReversedBinary(t *testing.T) {
	// 0x0102 -> binary 0000000100000010 -> reversed: bits 1 and 8 set.
	if got := setBits("0x0102"); !reflect.DeepEqual(got, []int{1, 8}) {
		t.Fatalf("setBits = %v", got)
	}
}
