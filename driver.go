package gibero

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"net"
	"strings"
	"time"
)

func DsnConvert(dsn string) *TiberoDSN {
	v1prefix := "tibero://"
	v1 := strings.HasPrefix(dsn, v1prefix)
	if !v1 {
		// log.Println("uknown dsn protocol")
		return nil
	}
	str := dsn[len(v1prefix):]
	tokens := strings.Split(str, "@")
	pre := tokens[0]
	sub := tokens[1]
	tokens = strings.Split(pre, ":")
	username := tokens[0]
	password := tokens[1]
	tokens = strings.Split(sub, "/")
	address := tokens[0]
	ext := tokens[1]
	dbname := strings.Split(ext, "?")[0]
	return &TiberoDSN{username: username, password: password, address: address, dbname: dbname, protocal: "tcp"}
}

func (reply *TbMsgExecuteCountReply) LastInsertId() (int64, error) {
	return 0, nil
}

func (reply *TbMsgExecuteCountReply) RowsAffected() (int64, error) {
	return int64(reply.cntLow), nil
}

func (msg *TbMsgExecutePrefetchReply) Columns() []string {
	if msg.colMeta != nil {
		size := len(msg.colMeta)
		rt := make([]string, size)
		for a := 0; a < size; a++ {
			rt[a] = msg.colMeta[a].name
		}
		return rt
	}
	return nil
}
func (msg *TbMsgExecutePrefetchReply) Close() error {
	return nil
}
func (replay *TbMsgExecutePrefetchReply) Next(dest []driver.Value) error {
	rset := replay.nextRow()
	if rset == nil {
		return io.EOF
	}
	for a := 0; a < len(rset.values); a++ {
		dest[a] = rset.values[a]
	}
	return nil
}

func (ps *PrepareStatement) Close() error {
	return nil
}
func (ps *PrepareStatement) NumInput() int {
	// TODO fix
	return -1
}

func setting(ps *PrepareStatement, args []driver.Value) error {
	size := len(args)
	for a := 0; a < size; a += 1 {
		arg := args[a]
		switch v := arg.(type) {
		case int64:
			ps.setInteger(int64(v))
		case float64:
			ps.setFloat64(float64(v))
		case float32:
			ps.setFloat32(float32(v))
		case string:
			ps.setString(string(v))
		case time.Time:
			ps.setTimestamp(time.Time(v))
		// case []byte:
		// 	ps.setTimestamp(time.Time(v))
		default:
			return errors.New("unsupport type")
		}
	}
	return nil
}
func (ps *PrepareStatement) Exec(args []driver.Value) (driver.Result, error) {
	err := setting(ps, args)
	if err != nil {
		return nil, err
	}
	return ps.doExec()
}
func (ps *PrepareStatement) Query(args []driver.Value) (driver.Rows, error) {
	err := setting(ps, args)
	if err != nil {
		return nil, err
	}
	return ps.doQuery()
}
func (connector *TConnector) Commit() error {
	_, err := connector.tibero.commit()
	if err != nil {
		return err
	}
	return nil
}
func (connector *TConnector) Rollback() error {
	_, err := connector.tibero.rollback()
	if err != nil {
		return err
	}
	return nil
}

type TConnector struct {
	*TiberoDriver
	tibero    *Tibero
	autoComit uint32
}

func (connector *TConnector) Prepare(query string) (driver.Stmt, error) {
	// log.Println("create prepare statement")
	ps := connector.tibero.createPrepareStatement(query, connector.autoComit)
	return ps, nil
}

func (connector *TConnector) Close() error {
	return nil
}
func (connector *TConnector) Begin() (driver.Tx, error) {
	// log.Println("start transaction")
	connector.autoComit = 0
	return connector, nil
}
func (connector *TConnector) Connect(context.Context) (driver.Conn, error) {
	// log.Println("do connect--")
	connector.tibero.connect()
	return connector, nil
}
func (connector *TConnector) Driver() driver.Driver {
	return connector.TiberoDriver
}

func CreateTibero(dsn *TiberoDSN) *Tibero {
	addr, _ := net.ResolveTCPAddr("tcp", dsn.address)
	server := &Singleton{TiberoDSN: dsn, addr: addr}
	return &Tibero{client: "go-tibero", DBServer: server, dsn: dsn}
}

type TiberoDriver struct{}

func (driver *TiberoDriver) Open(url string) (driver.Conn, error) {
	printFormat("url [%s]\n", url)
	return nil, nil
}
func (driver *TiberoDriver) OpenConnector(dsn string) (driver.Connector, error) {
	printFormat("dsn [%s]\n", dsn)
	conf := DsnConvert(dsn)
	return &TConnector{TiberoDriver: driver, tibero: CreateTibero(conf), autoComit: 1}, nil
}

func init() {
	tdriver := &TiberoDriver{}
	sql.Register("tibero", tdriver)
}
