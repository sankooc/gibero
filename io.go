package gibero

import (
	"encoding/binary"
	"io"
	"time"
)

type ByteReader struct {
	Cur    uint32
	Data   []byte
	Total  uint32
	reader io.Reader
}

func (reader *ByteReader) read(size uint32) []byte {
	start := reader.Cur
	last := start + size
	if last > reader.Total {
		panic("outofIndex")
	}
	reader.Cur += size
	return reader.Data[start:last]
}
func (reader *ByteReader) read32Big() uint32 {
	start := reader.Cur
	last := start + 4
	if last > reader.Total {
		panic("outofIndex")
	}
	reader.Cur += 4
	return binary.BigEndian.Uint32(reader.Data[start:last])
}

func (reader *ByteReader) read16Big() uint16 {
	start := reader.Cur
	last := start + 2
	if last > reader.Total {
		panic("outofIndex")
	}
	reader.Cur += 2
	return binary.BigEndian.Uint16(reader.Data[start:last])
}

func (reader *ByteReader) readByte() uint8 {
	start := reader.Cur
	last := start + 1
	if last > reader.Total {
		panic("outofIndex")
	}
	reader.Cur += 1
	return reader.Data[start]
}

func (reader *ByteReader) read64Big() uint64 {
	start := reader.Cur
	last := start + 8
	if last > reader.Total {
		panic("outofIndex")
	}
	reader.Cur += 8
	return binary.BigEndian.Uint64(reader.Data[start:last])
}

func (reader *ByteReader) moveCursor(offset uint32) {
	reader.Cur += offset
}
func (reader *ByteReader) read32String(len uint32) string {
	start := reader.Cur
	reader.Cur += len
	return string(reader.Data[start : start+len])
}

func (reader *ByteReader) readDBByte32(padding bool) *DBByte32 {
	size := reader.read32Big()
	pad := size % 4
	leng := size
	if pad > 0 {
		leng = size + 4 - pad
	}
	start := reader.Cur
	last := start + leng
	if last > reader.Total {
		panic("outofIndex")
	}
	reader.Cur += leng
	bt := DecodePadString(size, reader.Data[start:last])
	return (*DBByte32)(&bt)
}
func (reader *ByteReader) ReadDBString() string {
	leng := reader.read32Big()
	str := reader.read32String(leng)
	ext := pad(leng)
	reader.moveCursor(ext)
	return str
}

func (reader *ByteReader) reBuild(size uint32) *ByteReader {
	data := make([]byte, size)
	reader.reader.Read(data)
	return CreateReader(reader.reader, data, 0)
}

type ByteWriter struct {
	buf []byte
	n   uint32
	cur uint32
}

func (writer *ByteWriter) Size() uint32 {
	return writer.n
}

func (writer *ByteWriter) resize(nextlen uint32) {
	for writer.cur+nextlen >= writer.n {
		writer.buf = append(writer.buf, writer.buf...)
		writer.n = uint32(len(writer.buf))
	}
}

func (writer *ByteWriter) buffer(length uint32) []byte {
	writer.resize(length)
	return writer.buf[writer.cur:(writer.cur + length)]
}

func (writer *ByteWriter) Data() []byte {
	return writer.buf[0:writer.cur]
}
func (writer *ByteWriter) WriteByte(val byte) {
	var ll uint32 = 1
	writer.buffer(ll)[0] = val
	writer.cur = writer.cur + ll
}
func (writer *ByteWriter) WriteBig32(val uint32) {
	var ll uint32 = 4
	binary.BigEndian.PutUint32(writer.buffer(ll), val)
	writer.cur = writer.cur + ll
}
func (writer *ByteWriter) InsertBig32(val uint32, start uint32) {
	binary.BigEndian.PutUint32(writer.buf[start:start+4], val)
}

func (writer *ByteWriter) WriteBig64(val uint64) {
	var ll uint32 = 8
	binary.BigEndian.PutUint64(writer.buffer(ll), val)
	writer.cur = writer.cur + ll
}
func pad(leng uint32) uint32 {
	v := (4 - leng%4) % 4
	if v == 0 {
		return 4
	}
	return v
}
func (writer *ByteWriter) putPad(padding uint32) {
	target := writer.buffer(padding)
	for a := 0; a < int(padding); a++ {
		target[a] = 0
	}
	writer.cur = writer.cur + padding
}
func (writer *ByteWriter) WriteDBString(val string) {
	data := []byte(val)
	strLen := uint32(len(val))
	writer.WriteBig32(strLen)
	copy(writer.buffer(strLen), data)
	writer.cur = writer.cur + strLen
	writer.putPad(pad(strLen))
}
func (writer *ByteWriter) WriteDBMinLenString(val string) {
	data := []byte(val)
	strLen := byte(len(val))
	writer.WriteByte(strLen)
	copy(writer.buffer(uint32(strLen)), data)
	writer.cur = writer.cur + uint32(strLen)
	writer.putPad(pad(uint32(strLen) + 1))
}

func (writer *ByteWriter) WriteDBInteger(val int64) {
	data := EncodeInt64(val)
	strLen := byte(len(data))
	writer.WriteByte(strLen + 1)
	writer.WriteByte(strLen)
	copy(writer.buffer(uint32(strLen)), data)
	writer.cur = writer.cur + uint32(strLen)
	writer.putPad(pad(uint32(strLen) + 2))
}
func (writer *ByteWriter) WriteDBFloat(val float64, bitSize int) {
	data, _ := EncodeFloat(val, bitSize)
	strLen := byte(len(data))
	writer.WriteByte(strLen + 1)
	writer.WriteByte(strLen)
	copy(writer.buffer(uint32(strLen)), data)
	writer.cur = writer.cur + uint32(strLen)
	writer.putPad(pad(uint32(strLen) + 2))
}
func (writer *ByteWriter) WriteTimestamp(ti *time.Time) {
	var size byte = 12
	writer.WriteByte(size)
	fromTimestamp(writer.buffer(uint32(size)), ti)
	writer.cur = writer.cur + uint32(size)
	writer.putPad(3)
}

func (writer *ByteWriter) WriteDate(ti *time.Time) {
	var size byte = 8
	writer.WriteByte(size)
	fromDate(writer.buffer(uint32(size)), ti)
	writer.cur = writer.cur + uint32(size)
	writer.putPad(3)
}
func (writer *ByteWriter) WriteCLNT(size uint32, params []*TbclntInfoParam) {
	writer.WriteBig32(size)
	for _, param := range params {
		param.serialize(writer)
		// writer.WriteBig32(param.ClntParamId)
		// writer.WriteDBString(param.ClntParamVal)
	}
}
func (writer *ByteWriter) WriteNLS(size uint32, params []*TbclntInfoParam) {
	writer.WriteBig32(size)
	for _, param := range params {
		param.serialize(writer)
		// writer.WriteBig32(param.ClntParamId)
		// writer.WriteDBString(param.ClntParamVal)
	}
}

func CreateWriter() *ByteWriter {
	return &ByteWriter{cur: 0, n: 16, buf: make([]byte, 16)}
}

func CreateReader(reader io.Reader, data []byte, offset int) *ByteReader {
	return &ByteReader{reader: reader, Cur: 0, Data: data[offset:], Total: uint32(len(data) - offset)}
}
