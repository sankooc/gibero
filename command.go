package gibero

import (
	"container/list"
	"encoding/base64"
	"os"
	"os/user"
	"time"
)

type Tibero_CMD_CODE uint32

func (code Tibero_CMD_CODE) code() uint32 {
	return uint32(code)
}

const (
	CLOSE_CSR     Tibero_CMD_CODE = 22
	CLOSE_SESSION Tibero_CMD_CODE = 28
	CLOSE_LOB     Tibero_CMD_CODE = 50
	CLOSE_XA      Tibero_CMD_CODE = 67
	CLOSE_TID     Tibero_CMD_CODE = 226
)

type TbclntInfoParam struct {
	ClntParamId  uint32
	ClntParamVal string
}

func (tb *TbclntInfoParam) serialize(writer *ByteWriter) {
	writer.WriteBig32(tb.ClntParamId)
	writer.WriteDBString(tb.ClntParamVal)
}
func (tb *TbclntInfoParam) deserialize(reader *ByteReader) {
	tb.ClntParamId = reader.read32Big()
	tb.ClntParamVal = reader.ReadDBString()
}

func CLOSE_CSR_CMD(v uint32) []byte {
	writer := CreateWriter()
	writer.WriteBig32(CLOSE_CSR.code())
	writer.WriteBig32(0)
	writer.WriteBig64(0)
	writer.WriteBig32(v)
	return CMDtail(writer)
}
func CLOSE_SESSION_CMD(v uint32) []byte {
	writer := CreateWriter()
	writer.WriteBig32(CLOSE_SESSION.code())
	writer.WriteBig32(0)
	writer.WriteBig64(0)
	writer.WriteBig32(v)
	return CMDtail(writer)
}
func CLOSE_LOB_CMD(v uint32) []byte {
	writer := CreateWriter()
	writer.WriteBig32(CLOSE_LOB.code())
	writer.WriteBig32(0)
	writer.WriteBig64(0)
	writer.WriteBig32(v)
	return CMDtail(writer)
}
func CLOSE_XA_CMD(v uint32) []byte {
	writer := CreateWriter()
	writer.WriteBig32(CLOSE_XA.code())
	writer.WriteBig32(0)
	writer.WriteBig64(0)
	writer.WriteBig32(v)
	return CMDtail(writer)
}
func CLOSE_TID_CMD() []byte {
	writer := CreateWriter()
	writer.WriteBig32(CLOSE_TID.code())
	writer.WriteBig32(0)
	writer.WriteBig64(0)
	return CMDtail(writer)
}

func PKExchangeCmd() []byte {
	writer := CreateWriter()
	writer.WriteBig32(282)
	writer.WriteBig32(0)
	writer.WriteBig64(0)
	return writer.Data()
}

func SQLCMD(autoComit uint32, prefetch uint32, sql string) []byte {
	writer := CreateWriter()
	writer.WriteBig32(6)
	writer.WriteBig32(0)
	writer.WriteBig64(0)
	writer.WriteBig32(autoComit)
	writer.WriteBig32(prefetch)
	writer.WriteDBString(sql)
	return CMDtail(writer)
}

func PrepareStatementCMD(sql string, autoComit uint32, prefetch uint32, flag bool) {
	var paramCount uint32 = 1
	writer := CreateWriter()
	if flag {
		writer.WriteBig32(5)
	} else {
		writer.WriteBig32(7)
	}
	writer.WriteBig32(0)
	writer.WriteBig64(0)
	if flag {
		// write ppid
		// writer.WriteBig32()
	} else {
		writer.WriteDBString(sql)
	}
	writer.WriteBig32(autoComit)
	writer.WriteBig32(prefetch)
	writer.WriteBig32(paramCount)
}

func generateC(id uint32, val string) *TbclntInfoParam {
	return &TbclntInfoParam{ClntParamId: id, ClntParamVal: val}
}

// AUTH_REQ_WITH_VER
func AuthRequestCmd(username string, password string, dbname string, program string, publicK []byte) []byte {
	user, _ := user.Current()
	suser := user.Username
	hostname, _ := os.Hostname()
	printFormat("user %s %s", suser, hostname)
	var params [7]*TbclntInfoParam
	params[0] = generateC(0, "-1")
	params[1] = generateC(1, program)
	params[2] = generateC(2, "") // nil
	params[3] = generateC(3, suser)
	params[4] = generateC(4, hostname)
	params[5] = generateC(5, "")
	params[6] = generateC(0, "")
	var params2 [11]*TbclntInfoParam
	params2[0] = generateC(0, "")
	params2[1] = generateC(1, "")
	params2[2] = generateC(2, "")
	params2[3] = generateC(6, "")
	params2[4] = generateC(3, "AMERICAN")
	params2[5] = generateC(4, "")
	params2[6] = generateC(5, "")
	params2[7] = generateC(7, "Asia/Shanghai")
	params2[8] = generateC(0, "")
	params2[9] = generateC(0, "")
	params2[10] = generateC(0, "")

	enPassword, _ := RSA_Encrypt([]byte(password), publicK)
	base64Password := base64.StdEncoding.EncodeToString(enPassword)
	writer := CreateWriter()
	writer.WriteBig32(137)
	writer.WriteBig32(0)
	writer.WriteBig64(0)
	// 2 16 0
	writer.WriteBig32(2)
	writer.WriteBig32(16)
	writer.WriteBig32(0)
	writer.WriteDBString(username)
	writer.WriteDBString(dbname)
	writer.WriteDBString(base64Password)
	writer.WriteCLNT(7, params[:])
	writer.WriteNLS(11, params2[:])
	//
	writer.WriteBig32(1)
	return CMDtail(writer)
}

func CommitCMD() []byte {
	writer := CreateWriter()
	writer.WriteBig32(23)
	writer.WriteBig32(0)
	writer.WriteBig64(0)
	return writer.Data()
}

func CMDtail(writer *ByteWriter) []byte {
	innerLen := writer.cur - 16
	writer.InsertBig32(uint32(innerLen), 4)
	return writer.Data()
}
func RollbackCMD(str string) []byte {
	writer := CreateWriter()
	writer.WriteBig32(24)
	writer.WriteBig32(0)
	writer.WriteBig64(0)
	writer.WriteDBString(str)
	return CMDtail(writer)
}

type PrepareStatement struct {
	sql       string
	params    *list.List
	flag      bool
	autoComit uint32
	prefetch  uint32
	tibero    *Tibero
}

func (ps *PrepareStatement) addParam(binder ParamBinder) {
	if ps.params == nil {
		ps.params = list.New()
	}
	ps.params.PushBack(binder)
}
func (ps *PrepareStatement) setString(param string) {
	ps.addParam(StringBinder(param))
}

func (ps *PrepareStatement) setInteger(param int64) {
	ps.addParam(IntegerBinder(param))
}
func (ps *PrepareStatement) setFloat32(param float32) {
	ps.addParam(Float32Binder(param))
}
func (ps *PrepareStatement) setFloat64(param float64) {
	ps.addParam(Float64Binder(param))
}

func (ps *PrepareStatement) setTimestamp(param time.Time) {
	ps.addParam(TimestampBinder(param))
}

func (ps *PrepareStatement) setDate(param time.Time) {
	ps.addParam(DateBinder(param))
}

func (ps *PrepareStatement) deserialize() []byte {
	var paramCount uint32 = 0
	writer := CreateWriter()
	if ps.flag {
		writer.WriteBig32(5)
	} else {
		writer.WriteBig32(7)
	}
	writer.WriteBig32(0)
	writer.WriteBig64(0)
	if ps.flag {
		// write ppid
		// writer.WriteBig32()
	} else {
		writer.WriteDBString(ps.sql)
	}
	if ps.params != nil {
		paramCount = uint32(ps.params.Len())
	}
	writer.WriteBig32(ps.autoComit)
	writer.WriteBig32(ps.prefetch)
	writer.WriteBig32(paramCount)
	if paramCount > 0 {
		index := 0
		ele := ps.params.Front()
		for {
			binder := (ele.Value).(ParamBinder)
			paramMode := 1
			var ptype uint32 = uint32((paramMode & 255) | (int(binder.paramType()) << 8))
			writer.WriteBig32(ptype)
			binder.deserialize(writer)
			if ele.Next() == nil {
				break
			}
			ele = ele.Next()
			index++
		}
	}

	return CMDtail(writer)
}

func (ps *PrepareStatement) exec() (interface{}, error) {
	raw := ps.deserialize()
	return ps.tibero.write(raw)
}

func (ps *PrepareStatement) doQuery() (*TbMsgExecutePrefetchReply, error) {
	ps.prefetch = 64000
	msg, err := ps.exec()
	if err != nil {
		return nil, err
	}
	info, ok := msg.(*TbMsgExecutePrefetchReply)
	if ok {
		return info, nil
	}
	return nil, nil
}

func (ps *PrepareStatement) doExec() (*TbMsgExecuteCountReply, error) {
	ps.prefetch = 0
	msg, err := ps.exec()
	if err != nil {
		return nil, err
	}
	info, ok := msg.(*TbMsgExecuteCountReply)
	if ok {
		return info, nil
	}
	return nil, nil
}

type ParamBinder interface {
	deserialize(writer *ByteWriter)
	paramType() byte
}

type BytesBinder []byte

func (binder BytesBinder) paramType() byte {
	return 4
}

func (binder BytesBinder) deserialize(writer *ByteWriter) {
	// writer.WriteDBMinLenString(string(binder))
}

type StringBinder string

func (binder StringBinder) paramType() byte {
	return 3
}

func (binder StringBinder) deserialize(writer *ByteWriter) {
	writer.WriteDBMinLenString(string(binder))
}

type IntegerBinder int64

func (binder IntegerBinder) paramType() byte {
	return 1
}

func (binder IntegerBinder) deserialize(writer *ByteWriter) {
	writer.WriteDBFloat(float64(binder), 32)
}

type Float32Binder float32

func (binder Float32Binder) paramType() byte {
	return 1
}

func (binder Float32Binder) deserialize(writer *ByteWriter) {
	writer.WriteDBFloat(float64(binder), 32)
}

type Float64Binder float64

func (binder Float64Binder) paramType() byte {
	return 1
}

func (binder Float64Binder) deserialize(writer *ByteWriter) {
	writer.WriteDBFloat(float64(binder), 64)
}

type TimestampBinder time.Time

func (binder TimestampBinder) deserialize(writer *ByteWriter) {
	var ti time.Time = time.Time(binder)
	writer.WriteTimestamp(&ti)
}
func (binder TimestampBinder) paramType() byte {
	return 7
}

type DateBinder time.Time

func (binder DateBinder) deserialize(writer *ByteWriter) {
	var ti time.Time = time.Time(binder)
	writer.WriteDate(&ti)
}
func (binder DateBinder) paramType() byte {
	return 5
}
