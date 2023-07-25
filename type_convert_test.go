package gibero

import (
	"encoding/hex"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestConvert(t *testing.T) {
	assert := require.New(t)
	var ts int64 = 1689905720186
	bt := make([]byte, 12)
	fromTimestamps(bt[:], ts)
	printFormat("hex :\t [%s] \n", hex.EncodeToString(bt[:]))
	assert.Equal(hex.EncodeToString(bt[:]), "787b07150a0f14000b162280", "ts")
	bt = make([]byte, 8)
	fromDates(bt[:], ts)
	printFormat("hex :\t [%s] \n", hex.EncodeToString(bt[:]))
	assert.Equal(hex.EncodeToString(bt[:]), "787b071500000000", "date")
}

func TestScan(t *testing.T) {
	assert := require.New(t)
	{
		var scaner scanType = StringScan("test-test")
		var v123 string
		scaner.scan(&v123)
		assert.Equal("test-test", v123)
	}
	{
		var scaner scanType = IntegerScan(1235)
		var v123 int32
		scaner.scan(&v123)
		assert.Equal(int32(1235), v123)
	}
	{
		var scaner scanType = IntegerScan(1235)
		var v123 int64
		scaner.scan(&v123)
		assert.Equal(int64(1235), v123)
	}
	{
		to, _ := time.Parse(time.RFC3339, "2023-01-02T20:18:01+08:00")
		expect := to.UnixMilli()
		var tmp [12]byte
		fromTimestamp(tmp[:], &to)
		var scanner scanType = TimestempScan(tmp)
		ti := &time.Time{}
		scanner.scan(ti)
		assert.Equal(expect, ti.UnixMilli(), "date")
		var str string
		scanner.scan(&str)
		assert.Equal("2023-01-02T20:18:01", str, "date")
	}
}
