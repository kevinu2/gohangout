package output

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ClickHouse/clickhouse-go"
	"github.com/golang/glog"
	"github.com/kevinu2/gohangout/topology"
)

const (
	ClickhouseDefaultBulkActions   = 1000
	ClickhouseDefaultFlushInterval = 30
)

type ClickhouseOutput struct {
	config map[interface{}]interface{}

	bulkActions int
	hosts       []string
	fields      []string
	table       string
	username    string
	password    string

	fieldsLength int
	query        string
	desc         map[string]*rowDesc
	defaultValue map[string]interface{} // columnName -> defaultValue

	bulkChan   chan []map[string]interface{}
	concurrent int

	events      []map[string]interface{}
	executionId uint64

	dbSelector HostSelector

	mux       sync.Mutex
	wg        sync.WaitGroup
	closeChan chan bool
}

type rowDesc struct {
	Name              string `json:"name"`
	Type              string `json:"type"`
	DefaultType       string `json:"default_type"`
	DefaultExpression string `json:"default_expression"`
}

func (c *ClickhouseOutput) setTableDesc() {
	c.desc = make(map[string]*rowDesc)

	query := fmt.Sprintf("desc table %s", c.table)
	glog.V(5).Info(query)

	for i := 0; i < c.dbSelector.Size(); i++ {
		nextDb := c.dbSelector.Next()

		db := nextDb.(*sql.DB)

		rows, err := db.Query(query)
		if err != nil {
			glog.Errorf("query %q error: %s", query, err)
			continue
		}

		columns, err := rows.Columns()
		if err != nil {
			glog.Fatalf("could not get columns from query `%s`: %s", query, err)
		}
		glog.V(10).Infof("desc table columns: %v", columns)

		descMap := make(map[string]string)
		for _, c := range columns {
			descMap[c] = ""
		}

		for rows.Next() {
			values := make([]interface{}, 0)
			for range columns {
				var a string
				values = append(values, &a)
			}

			if err := rows.Scan(values...); err != nil {
				glog.Fatalf("scan rows error: %s", err)
			}

			descMap := make(map[string]string)
			for i, c := range columns {
				value := *values[i].(*string)
				if c == "type" {
					// 特殊处理枚举类型
					if strings.HasPrefix(value, "Enum16") {
						value = "Enum16"
					} else if strings.HasPrefix(value, "Enum8") {
						value = "Enum8"
					}
				}
				descMap[c] = value
			}

			b, err := json.Marshal(descMap)
			if err != nil {
				glog.Fatalf("marshal desc error: %s", err)
			}

			rowDesc := rowDesc{}
			err = json.Unmarshal(b, &rowDesc)
			if err != nil {
				glog.Fatalf("marshal desc error: %s", err)
			}

			glog.V(5).Infof("row desc: %#v", rowDesc)

			c.desc[rowDesc.Name] = &rowDesc
		}
		rows.Close()

		return
	}
}

// TODO only string, number and ip DEFAULT expression is supported for now
func (c *ClickhouseOutput) setColumnDefault() {
	c.setTableDesc()

	c.defaultValue = make(map[string]interface{})

	var defaultValue *string

	for columnName, d := range c.desc {
		switch d.DefaultType {
		case "DEFAULT":
			defaultValue = &(d.DefaultExpression)
		case "MATERIALIZED":
			glog.Fatal("parse default value: MATERIALIZED expression not supported")
		case "ALIAS":
			glog.Fatal("parse default value: ALIAS expression not supported")
		case "":
			defaultValue = nil
		default:
			glog.Fatal("parse default value: only DEFAULT expression supported")
		}

		switch d.Type {
		case "String", "LowCardinality(String)":
			if defaultValue == nil {
				c.defaultValue[columnName] = ""
			} else {
				c.defaultValue[columnName] = *defaultValue
			}
		case "Date", "DateTime":
			c.defaultValue[columnName] = time.Unix(0, 0)
		case "UInt8", "UInt16", "UInt32", "UInt64", "Int8", "Int16", "Int32", "Int64":
			if defaultValue == nil {
				c.defaultValue[columnName] = 0
			} else {
				i, e := strconv.ParseInt(*defaultValue, 10, 64)
				if e == nil {
					c.defaultValue[columnName] = i
				} else {
					glog.Fatalf("parse default value `%v` error: %v", defaultValue, e)
				}
			}
		case "Float32", "Float64":
			if defaultValue == nil {
				c.defaultValue[columnName] = 0
			} else {
				i, e := strconv.ParseFloat(*defaultValue, 64)
				if e == nil {
					c.defaultValue[columnName] = i
				} else {
					glog.Fatalf("parse default value `%v` error: %v", defaultValue, e)
				}
			}
		case "IPv4":
			c.defaultValue[columnName] = "0.0.0.0"
		case "IPv6":
			c.defaultValue[columnName] = "::"
		case "Array(String)", "Array(IPv4)", "Array(IPv6)", "Array(Date)", "Array(DateTime)":
			c.defaultValue[columnName] = clickhouse.Array([]string{})
		case "Array(UInt8)":
			c.defaultValue[columnName] = clickhouse.Array([]uint8{})
		case "Array(UInt16)":
			c.defaultValue[columnName] = clickhouse.Array([]uint16{})
		case "Array(UInt32)":
			c.defaultValue[columnName] = clickhouse.Array([]uint32{})
		case "Array(UInt64)":
			c.defaultValue[columnName] = clickhouse.Array([]uint64{})
		case "Array(Int8)":
			c.defaultValue[columnName] = clickhouse.Array([]int8{})
		case "Array(Int16)":
			c.defaultValue[columnName] = clickhouse.Array([]int16{})
		case "Array(Int32)":
			c.defaultValue[columnName] = clickhouse.Array([]int32{})
		case "Array(Int64)":
			c.defaultValue[columnName] = clickhouse.Array([]int64{})
		case "Array(Float32)":
			c.defaultValue[columnName] = clickhouse.Array([]float32{})
		case "Array(Float64)":
			c.defaultValue[columnName] = clickhouse.Array([]float64{})
		case "Enum16":
			// 需要要求列声明的最小枚举值为 ''
			c.defaultValue[columnName] = ""
		case "Enum8":
			// 需要要求列声明的最小枚举值为 ''
			c.defaultValue[columnName] = ""
		default:
			if strings.HasPrefix(d.Type, "DateTime64") {
				c.defaultValue[columnName] = time.Unix(0, 0)
				continue
			}
			glog.Errorf("column: %s, type: %s. unsupported column type, ignore.", columnName, d.Type)
			continue
		}
	}
}

func (c *ClickhouseOutput) getDatabase() string {
	dbAndTable := strings.Split(c.table, ".")
	dbName := "default"
	if len(dbAndTable) == 2 {
		dbName = dbAndTable[0]
	}
	return dbName
}

func init() {
	Register("Clickhouse", newClickhouseOutput)
}

func newClickhouseOutput(config map[interface{}]interface{}) topology.Output {
	rand.Seed(time.Now().UnixNano())
	p := &ClickhouseOutput{
		config: config,
	}

	if v, ok := config["table"]; ok {
		p.table = v.(string)
	} else {
		glog.Fatalf("table must be set in clickhouse output")
	}

	if v, ok := config["hosts"]; ok {
		for _, h := range v.([]interface{}) {
			p.hosts = append(p.hosts, h.(string))
		}
	} else {
		glog.Fatalf("hosts must be set in clickhouse output")
	}

	if v, ok := config["username"]; ok {
		p.username = v.(string)
	}

	if v, ok := config["password"]; ok {
		p.password = v.(string)
	}

	debug := false
	if v, ok := config["debug"]; ok {
		debug = v.(bool)
	}

	if v, ok := config["fields"]; ok {
		for _, f := range v.([]interface{}) {
			p.fields = append(p.fields, f.(string))
		}
	} else {
		glog.Fatalf("fields must be set in clickhouse output")
	}
	if len(p.fields) <= 0 {
		glog.Fatalf("fields length must be > 0")
	}
	p.fieldsLength = len(p.fields)

	fields := make([]string, p.fieldsLength)
	for i := range fields {
		fields[i] = fmt.Sprintf(`"%s"`, p.fields[i])
	}
	questionMarks := make([]string, p.fieldsLength)
	for i := 0; i < p.fieldsLength; i++ {
		questionMarks[i] = "?"
	}
	p.query = fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", p.table, strings.Join(fields, ","), strings.Join(questionMarks, ","))
	glog.V(5).Infof("query: %s", p.query)

	connMaxLifetime := 0
	if v, ok := config["conn_max_life_time"]; ok {
		connMaxLifetime = v.(int)
	}

	dbs := make([]*sql.DB, 0)

	for _, host := range p.hosts {
		dataSourceName := fmt.Sprintf("%s?database=%s&username=%s&password=%s&debug=%v", host, p.getDatabase(), p.username, p.password, debug)
		if db, err := sql.Open("clickhouse", dataSourceName); err == nil {
			if err := db.Ping(); err != nil {
				if exception, ok := err.(*clickhouse.Exception); ok {
					glog.Errorf("[%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
				} else {
					glog.Errorf("clickhouse ping error: %s", err)
				}
			} else {
				db.SetConnMaxLifetime(time.Second * time.Duration(connMaxLifetime))
				dbs = append(dbs, db)
			}
		} else {
			glog.Errorf("open %s error: %s", host, err)
		}
	}

	glog.V(5).Infof("%d available clickhouse hosts", len(dbs))
	if len(dbs) == 0 {
		glog.Fatal("no available host")
	}

	dbsI := make([]interface{}, len(dbs))
	for i, h := range dbs {
		dbsI[i] = h
	}
	p.dbSelector = NewRRHostSelector(dbsI, 3)

	p.setColumnDefault()

	concurrent := 1
	if v, ok := config["concurrent"]; ok {
		concurrent = v.(int)
	}
	p.concurrent = concurrent
	p.closeChan = make(chan bool, concurrent)

	p.bulkChan = make(chan []map[string]interface{}, concurrent)
	for i := 0; i < concurrent; i++ {
		go func() {
			p.wg.Add(1)
			for {
				select {
				case events := <-p.bulkChan:
					p.innerFlush(events)
				case <-p.closeChan:
					p.wg.Done()
					return
				}
			}
		}()
	}

	if v, ok := config["bulk_actions"]; ok {
		p.bulkActions = v.(int)
	} else {
		p.bulkActions = ClickhouseDefaultBulkActions
	}

	var flushInterval int
	if v, ok := config["flush_interval"]; ok {
		flushInterval = v.(int)
	} else {
		flushInterval = ClickhouseDefaultFlushInterval
	}
	go func() {
		for range time.NewTicker(time.Second * time.Duration(flushInterval)).C {
			p.flush()
		}
	}()

	return p
}

func (c *ClickhouseOutput) innerFlush(events []map[string]interface{}) {
	executionId := atomic.AddUint64(&c.executionId, 1)
	glog.Infof("write %d docs to clickhouse with execution_id %d", len(events), executionId)

	for {
		nextDb := c.dbSelector.Next()

		/*** not ReduceWeight for now , so this should not happen
		if nextDb == nil {
			glog.Info("no available db, wait for 30s")
			time.Sleep(30 * time.Second)
			continue
		}
		****/

		tx, err := nextDb.(*sql.DB).Begin()
		if err != nil {
			glog.Errorf("db begin to create transaction error: %s", err)
			continue
		}

		stmt, err := tx.Prepare(c.query)
		if err != nil {
			glog.Errorf("transaction prepare statement error: %s", err)
			return
		}

		for _, event := range events {
			args := make([]interface{}, c.fieldsLength)
			for i, field := range c.fields {
				if v, ok := event[field]; ok && v != nil {
					args[i] = v
				} else {
					if vv, ok := c.defaultValue[field]; ok {
						args[i] = vv
					} else { // this should not happen
						args[i] = ""
					}
				}
			}
			if _, err := stmt.Exec(args...); err != nil {
				glog.Errorf("exec clickhouse insert %v error: %s", event, err)
				return
			}
		}

		if err := tx.Commit(); err != nil {
			glog.Errorf("exec clickhouse commit error: %s", err)
			return
		}
		glog.Infof("%d docs has been committed to clickhouse", len(events))
		stmt.Close()
		tx.Rollback()
		return
	}
}

func (c *ClickhouseOutput) flush() {
	c.mux.Lock()
	if len(c.events) > 0 {
		events := c.events
		c.events = make([]map[string]interface{}, 0, c.bulkActions)
		c.bulkChan <- events
	}
	c.mux.Unlock()
}

// Emit appends event to c.events, and push to bulkChan if needed
func (c *ClickhouseOutput) Emit(event map[string]interface{}) {
	c.mux.Lock()
	c.events = append(c.events, event)
	if len(c.events) < c.bulkActions {
		c.mux.Unlock()
		return
	}

	events := c.events
	c.events = make([]map[string]interface{}, 0, c.bulkActions)
	c.mux.Unlock()

	c.bulkChan <- events
}

func (c *ClickhouseOutput) awaitClose(timeout time.Duration) {
	exit := make(chan bool)
	defer func() {
		select {
		case <-exit:
			glog.Info("all clickhouse flush job done. return")
			return
		case <-time.After(timeout):
			glog.Info("clickhouse await timeout. return")
			return
		}
	}()

	defer func() {
		go func() {
			c.wg.Wait()
			exit <- true
		}()
	}()

	glog.Info("try to write remaining docs to clickhouse")

	c.mux.Lock()
	if len(c.events) <= 0 {
		glog.Info("no docs remain, return")
		c.mux.Unlock()
	} else {
		events := c.events
		c.events = make([]map[string]interface{}, 0, c.bulkActions)
		c.mux.Unlock()

		glog.Infof("remain %d docs, write them to clickhouse", len(events))
		c.wg.Add(1)
		go func() {
			c.innerFlush(events)
			c.wg.Done()
		}()
	}

	glog.Info("check if there are events blocking in bulk channel")

	for {
		select {
		case events := <-c.bulkChan:
			c.wg.Add(1)
			go func() {
				c.innerFlush(events)
				c.wg.Done()
			}()
		default:
			return
		}
	}
}

// Shutdown would stop receiving message and emitting
func (c *ClickhouseOutput) Shutdown() {
	for i := 0; i < c.concurrent; i++ {
		c.closeChan <- true
	}
	c.awaitClose(30 * time.Second)
}
