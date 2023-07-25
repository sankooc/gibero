package gibero

import (
	"encoding/binary"
	"time"
)

type TbTimestamp [12]byte

func (ts *TbTimestamp) year() int {
	var v1, v2 int
	v1 = int(255 & ts[0])
	v2 = int(255 & ts[1])
	return (v1-100)*100 + (v2 - 100)
}

func (ts *TbTimestamp) month() int {
	return int(255 & ts[2])
}
func (ts *TbTimestamp) day() int {
	return int(255 & ts[3])
}
func (ts *TbTimestamp) hour() int {
	return int(255 & ts[4])
}
func (ts *TbTimestamp) minut() int {
	return int(255 & ts[5])
}
func (ts *TbTimestamp) second() int {
	return int(255 & ts[6])
}
func (ts *TbTimestamp) nano() int {
	return int(binary.BigEndian.Uint32(ts[8:12]))
}

func (ts *TbTimestamp) toDate() *time.Time {
	if ts[0] == 0 {
		return nil
	}
	var m time.Month = time.Month(ts.month())
	date := time.Date(ts.year(), m, ts.day(), ts.hour(), ts.minut(), ts.second(), ts.nano(), time.Local)
	return &date
}
