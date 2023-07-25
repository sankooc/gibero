package gibero

import (
	"encoding/hex"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func Test_types(t *testing.T) {
	assert := require.New(t)
	writer := CreateWriter()
	var id uint32 = 12
	var val string = "1233"
	{
		tb := TbclntInfoParam{ClntParamId: id, ClntParamVal: val}
		tb.serialize(writer)
	}
	data := writer.Data()
	reader := CreateReader(nil, data, 0)
	{
		tb := TbclntInfoParam{}
		tb.deserialize(reader)
		assert.Equal(tb.ClntParamId, id, "id")
		assert.Equal(tb.ClntParamVal, val, "val")
	}
}

func Test_prepareStatement(t *testing.T) {
	assert := require.New(t)
	sql := "insert into TEST_TABLE(NUM,VAR_B, TIMESTAMP, DATESS) values ( ?, ?, ?, ? )"
	var ps PrepareStatement = PrepareStatement{sql: sql, flag: false, autoComit: 1, prefetch: 64000}
	ps.setFloat32(0.33)
	ps.setString("soloickod")
	var ts int64 = 1689905720186
	ti := time.Unix(ts/1000, (ts%1000)*1000000)
	ps.setTimestamp(ti)
	ps.setDate(ti)
	data := ps.deserialize()
	{
		expect := "000000070000009c00000000000000000000004a696e7365727420696e746f20544553545f5441424c45284e554d2c5641525f422c2054494d455354414d502c20444154455353292076616c7565732028203f2c203f2c203f2c203f20290000000000010000fa0000000004000001010302c0a1000000000000030109736f6c6f69636b6f640000000007010c787b07150a0f14000b1622800000000000050108787b071500000000000000"
		assert.Equal(expect, hex.EncodeToString(data[:]))
	}
}

func TestCMD(t *testing.T) {
	assert := require.New(t)
	{
		buf := PKExchangeCmd()
		expect := "0000011a000000000000000000000000"
		assert.Equal(expect, hex.EncodeToString(buf[:]), "PKExchangeCmd")
	}
	{

		// pem, _ := os.ReadFile("../packet/tibero.pub")
		// buf := AuthRequestCmd("sankooc", "sankoo", "was", "tibero-go", pem)
		// log.Printf("%s \n", hex.EncodeToString(buf[:]))
		//
		// assert.Equal(expect, hex.EncodeToString(buf[:]), "PKExchangeCmd")
	}
}
