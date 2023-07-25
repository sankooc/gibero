package gibero

import (
	"encoding/hex"
	"io/ioutil"
	"log"
	"net"
)

type Singleton struct {
	*TiberoDSN
	addr  *net.TCPAddr
	conn  net.Conn
	state int
}

func (m *Singleton) flush() {
	bt, _ := ioutil.ReadAll(m.conn)
	printFormat("extra %s \n", hex.EncodeToString(bt[:]))
}
func (m *Singleton) checkConnect() {
	// if m.state < 1 {
	// 	panic("disconnect")
	// }
}

func (m *Singleton) readMessage() (*Message, *ByteReader) {
	var mbt [16]byte
	_, err := m.conn.Read(mbt[:])
	if err != nil {
		//log.Println("recv failed, err:", err)
		panic(err)
	}
	msg := &Message{}
	msg.DeserializeFromBytes(mbt[:])
	printFormat("msg-type:=[%d]", msg.MsgType)
	extLen := msg.MsgBodySize
	printFormat("response-size[%d] \n", extLen)
	ext := make([]byte, extLen)
	_, err = m.conn.Read(ext)
	PrintHex("connect-res", ext)
	if err != nil {
		log.Println("recv failed, err:", err)
		panic(err)
	}
	return msg, CreateReader(m.conn, ext, 0)
}

func (m *Singleton) Connect() {
	conn, _ := net.DialTCP("tcp", nil, m.addr)
	m.conn = conn
}
func (m *Singleton) writeTo(data []byte) {
	m.checkConnect()
	PrintHex("prewrite-cmd", data)
	m.conn.Write(data)
}
