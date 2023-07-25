package gibero

import (
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"log"
	"strings"
	"time"

	"golang.org/x/text/encoding"
	"golang.org/x/text/encoding/korean"
	"golang.org/x/text/transform"
)

type DBByte32 []byte

type TbColumnDesc struct {
	name      string
	dataType  uint32
	precision uint32
	scale     uint32
	etcMeta   uint32
	maxSize   uint32
}

func (desc *TbColumnDesc) deserialize(reader *ByteReader) {
	desc.name = reader.ReadDBString()
	desc.dataType = reader.read32Big()
	desc.precision = reader.read32Big()
	desc.scale = reader.read32Big()
	desc.etcMeta = reader.read32Big()
	desc.maxSize = reader.read32Big()
}

type Message struct {
	MsgType     uint32
	MsgBodySize uint32
	Tsn         uint64
}

func (msg *Message) deserialize(reader *ByteReader) {
	msg.MsgType = reader.read32Big()
	msg.MsgBodySize = reader.read32Big()
	msg.Tsn = reader.read64Big()
}

func (msg *Message) DeserializeFromBytes(data []byte) {
	msg.MsgType = binary.BigEndian.Uint32(data[0:4])
	msg.MsgBodySize = binary.BigEndian.Uint32(data[4:8])
	msg.Tsn = binary.BigEndian.Uint64(data[8:16])
}

type OkReply struct {
	*Message
	warningMsg string
}

func (msg *OkReply) deserialize(reader *ByteReader) {
	msg.warningMsg = reader.ReadDBString()
}

type ConnectMessage struct {
	*Message
	protocolMajor    uint32
	protocolMinor    uint32
	charset          uint32
	svrIsBigendian   uint32
	svrIsNanobase    uint32
	tbMajor          uint32
	tbMinor          uint32
	tbProductName    *DBByte32
	tbProductVersion *DBByte32
	mthrPid          uint32
	cps              uint32
	ncharset         uint32
	flags            uint32
}

func UDecodeString(decorder *encoding.Decoder, size uint32, data []byte, atEOF bool) []byte {
	var buf []byte = make([]byte, size)
	decorder.Transformer.Transform(buf, data, atEOF)
	return buf
}

func DecodePadString(size uint32, data []byte) []byte {
	return UDecodeString(korean.EUCKR.NewDecoder(), size, data, true)
}
func EncodePadString(data []byte) []byte {
	reader := transform.NewReader(bytes.NewReader(data), korean.EUCKR.NewEncoder())
	d, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil
	}
	return d
}
func (msg *ConnectMessage) deserialize(reader *ByteReader) {
	//nn
	msg.protocolMajor = reader.read32Big()
	msg.protocolMinor = reader.read32Big()
	msg.charset = reader.read32Big()
	msg.svrIsBigendian = reader.read32Big()
	msg.svrIsNanobase = reader.read32Big()
	msg.tbMajor = reader.read32Big()
	msg.tbMinor = reader.read32Big()
	msg.tbProductName = reader.readDBByte32(true)
	msg.tbProductVersion = reader.readDBByte32(true)
	msg.mthrPid = reader.read32Big()
	msg.cps = reader.read32Big()
	msg.ncharset = reader.read32Big()
	msg.flags = reader.read32Big()
}

type PKExchangeMessage struct {
	*Message
	SessKey *DBByte32
}

func (msg *PKExchangeMessage) deserialize(reader *ByteReader) {
	msg.SessKey = reader.readDBByte32(true)
}

type SessionInfoMessage struct {
	*Message
	sessionId uint32
	serialNo  uint32
	nlsData   []*TbclntInfoParam
}

func (msg *SessionInfoMessage) deserialize(reader *ByteReader) {
	msg.sessionId = reader.read32Big()
	msg.serialNo = reader.read32Big()
	size := reader.read32Big()
	msg.nlsData = make([]*TbclntInfoParam, size)
	for a := 0; a < int(size); a++ {
		tb := TbclntInfoParam{}
		tb.deserialize(reader)
		msg.nlsData[a] = &tb
	}
}

type SQLException struct {
	reason     string
	sqlState   string
	vendorCode uint32
}

type TbResultSet struct {
	values []interface{}
}

func (rs *TbResultSet) Scan(param ...any) {
	size := len(param)
	if size > len(rs.values) {
		return
	}
	for a := 0; a < size; a++ {
		// param[a]
		// type(values[a])
	}
}

type TbMsgExecuteCountReply struct {
	*Message
	ppid    *[8]byte
	cntHigh uint32
	cntLow  uint32
}

func (msg *TbMsgExecuteCountReply) deserialize(reader *ByteReader) {
	msg.ppid = (*[8]byte)(reader.read(8))
	msg.cntHigh = reader.read32Big()
	msg.cntLow = reader.read32Big()
}

type TbMsgExecutePrefetchReply struct {
	*Message
	ppid             *[8]byte
	affectedCnt      uint32
	csrId            uint32
	colCnt           uint32
	hiddenColCnt     uint32
	colMetaArrayCnt  uint32
	colMeta          []*TbColumnDesc
	rowCnt           uint32
	isFetchCompleted uint32
	rowChunkSize     uint32
	resultIndex      uint32
	resultSet        []*TbResultSet
}

func (msg *TbMsgExecutePrefetchReply) deserialize(reader *ByteReader) {
	msg.ppid = (*[8]byte)(reader.read(8))
	msg.affectedCnt = reader.read32Big()
	msg.csrId = reader.read32Big()
	msg.colCnt = reader.read32Big()
	msg.hiddenColCnt = reader.read32Big()
	size := reader.read32Big()
	msg.colMeta = make([]*TbColumnDesc, size)
	for a := 0; a < int(size); a++ {
		tb := &TbColumnDesc{}
		tb.deserialize(reader)
		msg.colMeta[a] = tb
		printFormat("tpype[%d]\n", tb.dataType)
	}
	msg.rowCnt = reader.read32Big()
	msg.isFetchCompleted = reader.read32Big()
	msg.rowChunkSize = reader.read32Big()
	reader = reader.reBuild(msg.rowChunkSize)
	reader.moveCursor(1)
	msg.resultIndex = 0
	msg.resultSet = make([]*TbResultSet, int(msg.rowCnt))
	for row := 0; row < int(msg.rowCnt); row++ {
		msg.resultSet[row] = msg.readRow(reader)
	}
	reader.moveCursor(1)
}

func (msg *TbMsgExecutePrefetchReply) nextRow() *TbResultSet {
	if msg.resultIndex >= msg.rowCnt {
		return nil
	}
	ts := msg.resultSet[msg.resultIndex]
	if ts != nil {
		msg.resultIndex += 1
		return ts
	}
	return nil
}
func (msg *TbMsgExecutePrefetchReply) readRow(reader *ByteReader) *TbResultSet {
	reader.moveCursor(3)
	size := len(msg.colMeta)
	item := &TbResultSet{}
	item.values = make([]interface{}, size)
	for a := 0; a < size; a++ {
		var inx uint32 = uint32(reader.readByte())
		if inx <= 250 {
		} else {
			inx = uint32(reader.read16Big())
		}
		ttype := msg.colMeta[a].dataType
		item.values[a] = des(reader, ttype, inx)
	}
	return item
}

type scanType interface {
	scan(val any)
}

func unexpectTypeErr(b any) {
	log.Println("unexpect type")

}

type StringScan string

func (scaner StringScan) scan(b any) {
	p1, ok := b.(*string)
	if ok {
		*p1 = string(scaner)
		return
	}
	unexpectTypeErr(b)
}

type IntegerScan int64

func (scaner IntegerScan) scan(b any) {
	p1, ok := b.(*int64)
	if ok {
		*p1 = int64(scaner)
		return
	}
	p2, ok := b.(*int32)
	if ok {
		*p2 = int32(scaner)
		return
	}
	unexpectTypeErr(b)
}

type FloatScan float64

func (scaner FloatScan) scan(b any) {
	p1, ok := b.(*float32)
	if ok {
		*p1 = float32(scaner)
		return
	}
	p2, ok := b.(*float64)
	if ok {
		*p2 = float64(scaner)
		return
	}
	unexpectTypeErr(b)
}

type TimestempScan [12]byte

func (scaner TimestempScan) scan(b any) {
	p1, ok := b.(*time.Time)
	if ok {
		var data = [12]byte(scaner)
		ti := toTimestamp(data[:])
		*p1 = *ti
		return
	}
	p2, ok := b.(*string)
	if ok {
		var data = [12]byte(scaner)
		ti := toTimestamp(data[:])
		// dstr := ti.Format("2006-01-02T15:04:05")
		*p2 = ti.Format("2006-01-02T15:04:05")
		return
	}
	unexpectTypeErr(b)
}

func des(reader *ByteReader, dtype uint32, length uint32) interface{} {
	tmp := reader.read(length)
	reader = CreateReader(nil, tmp, 0)
	switch dtype {
	case 1:
		bt := reader.read(length)
		printFormat("num %s\n", DecodeNumber(bt))
		return DecodeNumber(bt)
	case 2, 3:
		str := reader.read32String(length)
		return str
	case 12:
		// head := reader.readByte()
		// var len uint32
		// if head > 250 {
		// 	len = uint32(reader.read16Big())
		// } else {
		// 	len = uint32(head)
		// }
		// bt := reader.read(len)
		return nil
	case 7:
		//timestamp
		bt := reader.read(length)
		var ts TbTimestamp = [12]byte{}
		copy(ts[:], bt)
		// return &ts
		// log.Printf("date %+v\n", ts)
		if ts.toDate() != nil {
			return ts.toDate()
		}
	}
	return nil
}

type EReply struct {
	*Message
	noError       bool
	exceptions    []*SQLException
	flag          uint32
	errorStack    []byte
	errorStackLen uint32
}

func (msg *EReply) deserialize(reader *ByteReader) {
	msg.flag = reader.read32Big()
	exists := reader.read32Big()
	if exists == 0 {
		msg.noError = true
		reader.moveCursor(4)
	} else {
		reader.moveCursor(8)
		size := reader.read32Big()
		if size <= 0 {
			msg.noError = true
		} else {
			reader.moveCursor(4)
			msg.exceptions = make([]*SQLException, size)
			for a := 0; a < int(size); a++ {
				reader.moveCursor(12)
				vendorCode := reader.read32Big()
				reader.moveCursor(9)
				sqlState := strings.TrimSpace(reader.read32String(6))

				reason := strings.TrimSpace(reader.read32String(712))
				// fmt.Printf("%s---%d", f1, tt)
				reader.moveCursor(5)
				reader.moveCursor(8)
				reader.moveCursor(96)
				reader.moveCursor(84)
				msg.exceptions[a] = &SQLException{reason: reason, sqlState: sqlState, vendorCode: vendorCode}
			}
		}

	}

}

func RSA_Encrypt(plainText []byte, publickey []byte) ([]byte, error) {
	pk, err := BytesToPublicKey(publickey)
	if err != nil {
		return nil, err
	}
	return EncryptWithPublicKey(plainText, pk), nil
}

func FormatPEM(public_key string) []byte {
	ac, _ := base64.StdEncoding.DecodeString(public_key)
	raw := string(ac)
	subfix := "-----END PUBLIC KEY-----"
	lastIndex := strings.Index(raw, subfix)
	if lastIndex > 0 {
		return []byte(raw[0 : lastIndex+len(subfix)+1])
	}
	return ac
}

func getMD5(data []byte) string {
	hash := md5.New()
	hash.Write(data)
	sum := hash.Sum(nil)
	return fmt.Sprintf("%x\n", sum)
}
