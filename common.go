package gibero

import (
	"encoding/hex"
	"log"
	"net"
)

type Serializable interface {
	serialize(writer *ByteWriter)
}
type Deserializable interface {
	deserialize(reader *ByteReader)
}

type DBServer interface {
	Connect()
	readMessage() (*Message, *ByteReader)
	writeTo(data []byte)
	flush()
}

func printFormat(format string, v ...any) {
	if TB_VERBOSE {
		log.Printf(format, v...)
	}
}
func printLine(str string) {
	if TB_VERBOSE {
		log.Println(str)
	}

}
func PrintHex(tag string, data []byte) {
	hexStr := hex.EncodeToString(data)
	size := len(data)
	printFormat("tag [%s] size [%d] hex:[%s] \n", tag, size, hexStr)
}
func ReadMsg(reader *ByteReader) (*Message, *EReply) {
	meta := &Message{}
	meta.deserialize(reader)
	printFormat("msg-type:=[%d]", meta.MsgType)
	if meta.MsgType == 76 {
		return nil, buildErr(meta, reader)
	}
	return meta, nil
}
func buildErr(meta *Message, reader *ByteReader) *EReply {
	rp := &EReply{Message: meta}
	rp.deserialize(reader)
	printFormat("response error [%+v]\n", rp)
	return rp
}

type TiberoDSN struct {
	protocal string
	username string
	password string
	address  string
	dbname   string
}

type Tibero struct {
	DBServer
	pem         []byte
	dsn         *TiberoDSN
	client      string
	connectInfo *ConnectMessage
}

func handle(meta *Message, reader *ByteReader) (interface{}, error) {
	switch meta.MsgType {
	case 0:
		msg := &ConnectMessage{Message: meta}
		msg.deserialize(reader)
		return msg, nil
	case 2:
		msg := &SessionInfoMessage{Message: meta}
		msg.deserialize(reader)
		return msg, nil
	case 11:
		msg := &TbMsgExecutePrefetchReply{Message: meta}
		msg.deserialize(reader)
		return msg, nil
	case 13:
		msg := &TbMsgExecuteCountReply{Message: meta}
		msg.deserialize(reader)
		return msg, nil
	case 75:
		msg := &OkReply{Message: meta}
		msg.deserialize(reader)
		return msg, nil
	case 76:
		msg := &EReply{Message: meta}
		msg.deserialize(reader)
		return msg, nil
	case 283:
		msg := &PKExchangeMessage{Message: meta}
		msg.deserialize(reader)
		return msg, nil
	}

	return nil, nil
}

func (tibero *Tibero) write(cmd []byte) (interface{}, error) {
	tibero.DBServer.writeTo(cmd)
	meta, reader := tibero.DBServer.readMessage()
	return handle(meta, reader)
}

func (tibero *Tibero) connect() *EReply {
	{
		printFormat("ready to connect")
		tibero.DBServer.Connect()
		meta, reader := tibero.DBServer.readMessage()
		msg, _ := handle(meta, reader)
		inf, ok := msg.(*ConnectMessage)
		if ok {
			tibero.connectInfo = inf
		} else {
			panic("failed to connect")
		}
	}
	{
		printFormat("ready to public key exchange")
		cmd := PKExchangeCmd()
		msg, _ := tibero.write(cmd)
		inf, ok := msg.(*PKExchangeMessage)
		if ok {
			publicK := string([]byte(*inf.SessKey))
			tibero.pem = FormatPEM(publicK)
			printFormat("public key [%s]", string(tibero.pem))
		}
	}
	{
		printFormat("ready to auth req")
		info := tibero.dsn
		cmd := AuthRequestCmd(info.username, info.password, info.dbname, tibero.client, tibero.pem)
		msg, _ := tibero.write(cmd)
		_, ok := msg.(*SessionInfoMessage)
		if ok {
			printLine("auth success")
		}
		// SessionInfoMessage
		// raw := tibero.DBServer.RequestAuth(cmd)
		// PrintHex("connect-res", raw)
		// res, err := reactAuthReq(raw)
		// if err != nil {
		// 	return err
		// }
		// log.Printf("object [%+v]\n", res)
	}
	// tibero.DBServer.state = 1
	return nil
}

func (tibero *Tibero) executeDirct(sql string) (interface{}, error) {
	printFormat("do direct sql statement")
	//TYPE_FORWARD_ONLY 1003
	//CONCUR_READ_ONLY 1007
	cmd := SQLCMD(1, 64000, sql)
	return tibero.write(cmd)
}

func (tibero *Tibero) commit() (interface{}, error) {
	printFormat("do commit")
	cmd := CommitCMD()
	return tibero.write(cmd)
}

func (tibero *Tibero) rollback() (interface{}, error) {
	printFormat("do rollback")
	var savepoint string = ""
	cmd := RollbackCMD(savepoint)
	return tibero.write(cmd)
}

func (tibero *Tibero) createPrepareStatement(sql string, autoComit uint32) *PrepareStatement {
	printFormat("create sql %s autocomit %d", sql, autoComit)
	return &PrepareStatement{tibero: tibero, sql: sql, flag: false, autoComit: autoComit, prefetch: 0}
}

func CreateDB(host string, username string, password string, dbname string) *Tibero {
	dsn := &TiberoDSN{username: username, password: password, address: "", dbname: dbname, protocal: "tcp"}
	addr, _ := net.ResolveTCPAddr("tcp", host)
	server := &Singleton{TiberoDSN: dsn, addr: addr}
	return &Tibero{client: "go-tibero", DBServer: server, dsn: dsn}
}
