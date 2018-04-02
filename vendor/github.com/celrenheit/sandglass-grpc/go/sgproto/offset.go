package sgproto

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	time "time"
)

const Size = 6 + 8

type Offset [Size]byte

var (
	Nil       Offset
	MaxOffset Offset = [Size]byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}
)

func NewOffset(index uint64, displayTime time.Time) Offset {
	var o Offset
	o.setIndex(index)
	ts := uint64(displayTime.UTC().UnixNano()) / uint64(time.Millisecond)
	o.setTimestamp(ts)
	return o
}

func (d Offset) Marshal() ([]byte, error) {
	return d[:], nil
}

func (d Offset) Index() uint64 {
	return binary.BigEndian.Uint64(d[6:])
}

func (d *Offset) setIndex(index uint64) {
	binary.BigEndian.PutUint64((*d)[6:], index)
}

func (o *Offset) setTimestamp(ts uint64) {
	for i := 0; i < 6; i++ {
		(*o)[i] = byte(ts >> uint64((6-i-1)*8))
	}
}

func (o Offset) Time() time.Time {
	var ts uint64
	for i := 0; i < 6; i++ {
		ts |= uint64(uint64(o[i]) << uint64((6-i-1)*8))
	}
	return time.Unix(0, int64(ts)*int64(time.Millisecond)).UTC()
}

func (d Offset) MarshalTo(dst []byte) (int, error) {
	src, err := d.Marshal()
	if err != nil {
		return 0, err
	}

	copy(dst, src)

	return Size, nil
}

func (d *Offset) Unmarshal(b []byte) error {
	if len(b) != 6+8 {
		return fmt.Errorf("length of offset should be %d but got %d", 6+8, len(b))
	}

	copy((*d)[:], b)
	return nil
}

func (this Offset) Equal(that Offset) bool {
	return this.Index() == that.Index() && this.Time().Equal(that.Time())
}

func (this Offset) Before(that Offset) bool {
	return this.Index() < that.Index() || this.Time().Before(that.Time())
}

func (this Offset) After(that Offset) bool {
	return this.Index() > that.Index() || this.Time().After(that.Time())
}

func (_ Offset) Size() int {
	return Size
}

func (o Offset) Bytes() []byte {
	return o[:]
}

func (o Offset) String() string { // change me from hex
	return hex.EncodeToString(o.Bytes())
}
