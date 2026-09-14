package mysql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"iter"
	"net"
	"net/url"
	"reflect"
	"slices"
	"sort"
	"strings"
	"time"

	mysqlDriver "github.com/go-sql-driver/mysql"
	"github.com/gospider007/gson"
	"github.com/gospider007/gtls"
	"github.com/gospider007/netx"
	"github.com/gospider007/pg"
)

type ClientOption struct {
	DriverName  string //驱动名称
	OpenUrl     string //自定义的uri
	Usr         string //用户名
	Pwd         string //密码
	Host        string
	Port        int
	DbName      string            //数据库
	Protocol    string            //协议
	MaxConns    int               //最大连接数
	MaxLifeTime time.Duration     //最大活跃数
	Params      map[string]string //附加参数
	Socks5Proxy string
}
type Client struct {
	db     *sql.DB
	option ClientOption
}
type Rows struct {
	rows  *sql.Rows
	names []string
	kinds []reflect.Type
	cnl   context.CancelFunc
}
type Result struct {
	result sql.Result
}

// 新插入的列
func (obj *Result) LastInsertId() (int64, error) {
	return obj.result.LastInsertId()
}

// 受影响的行数
func (obj *Result) RowsAffected() (int64, error) {
	return obj.result.RowsAffected()
}

// 是否有下一个数据
func (obj *Rows) Next() bool {
	if obj.rows.Next() {
		return true
	} else {
		obj.Close()
		return false
	}
}

// 遍历
func (obj *Rows) Range() iter.Seq[map[string]any] {
	return func(yield func(map[string]any) bool) {
		defer obj.Close()
		for obj.Next() {
			if !yield(obj.Map()) {
				return
			}
		}
	}
}

type AnyValue interface {
	Value() (driver.Value, error)
}

// 返回游标的数据
func (obj *Rows) Map() map[string]any {
	result := make([]any, len(obj.kinds))
	for k, v := range obj.kinds {
		result[k] = reflect.New(v).Interface()
	}
	err := obj.rows.Scan(result...)
	if err != nil {
		obj.Close()
		return nil
	}
	maprs := map[string]any{}
	for k, v := range obj.names {
		val := reflect.ValueOf(result[k]).Elem().Interface()
		anyVal, ok := val.(AnyValue)
		if ok {
			value, err := anyVal.Value()
			if err != nil {
				obj.Close()
				return maprs
			}
			maprs[v] = value
		} else {
			maprs[v] = val
		}
	}
	return maprs
}

// 关闭游标
func (obj *Rows) Close() error {
	obj.cnl()
	return obj.rows.Close()
}
func NewClient(ctx context.Context, options ...ClientOption) (*Client, error) {
	var option ClientOption
	if len(options) > 0 {
		option = options[0]
	}
	if ctx == nil {
		ctx = context.TODO()
	}
	if option.DriverName == "" {
		option.DriverName = "mysql"
	}
	if option.Socks5Proxy != "" {
		proxy, err := gtls.VerifyProxy(option.Socks5Proxy)
		if err != nil {
			return nil, err
		}
		proxyAddress, err := netx.GetAddressWithUrl(proxy)
		if err != nil {
			return nil, err
		}
		if proxyAddress.Scheme != "socks5" {
			return nil, fmt.Errorf("only support socks5 proxy")
		}
		dialer := &netx.Dialer{}
		mysqlDriver.RegisterDialContext("tcp", func(ctx context.Context, addr string) (net.Conn, error) {
			remoteAdress, err := netx.GetAddressWithAddr(addr)
			if err != nil {
				return nil, err
			}
			return dialer.Socks5TcpProxy(ctx, nil, proxyAddress, remoteAdress)
		})
	}
	if option.MaxConns == 0 {
		option.MaxConns = 65535
	}
	var openAddr string
	if option.OpenUrl != "" {
		openAddr = option.OpenUrl
	} else {
		if option.Usr != "" {
			if option.Pwd != "" {
				openAddr += fmt.Sprintf("%s:%s@", option.Usr, option.Pwd)
			} else {
				openAddr += fmt.Sprintf("%s@", option.Usr)
			}
		}
		if option.Protocol != "" {
			openAddr += option.Protocol
		}
		if option.Host != "" {
			if option.Port == 0 {
				openAddr += fmt.Sprintf("(%s)/", option.Host)
			} else {
				openAddr += fmt.Sprintf("(%s:%d)/", option.Host, option.Port)
			}
		}
		if option.DbName != "" {
			openAddr += option.DbName
		}
		if len(option.Params) == 0 {
			option.Params = map[string]string{"parseTime": "true"}
		} else if _, ok := option.Params["parseTime"]; !ok {
			option.Params["parseTime"] = "true"
		}
		value := url.Values{}
		for k, v := range option.Params {
			value.Add(k, v)
		}
		openAddr += "?" + value.Encode()
	}
	db, err := sql.Open(option.DriverName, openAddr)
	if err != nil {
		return nil, err
	}
	err = db.PingContext(ctx)
	if err != nil {
		return nil, err
	}
	db.SetConnMaxIdleTime(option.MaxLifeTime)
	db.SetConnMaxLifetime(option.MaxLifeTime)
	db.SetMaxOpenConns(option.MaxConns)
	db.SetMaxIdleConns(option.MaxConns)
	c := NewClientWithSqlDB(db)
	c.option = option
	return c, nil
}

func (obj *Client) CreateTable(ctx context.Context, table string, columns ...pg.Column) error {
	if len(columns) == 0 {
		return nil
	}
	sort.Slice(columns, func(i, j int) bool {
		return columns[i].Position < columns[j].Position
	})
	lines := []string{}
	primarys := map[int][]string{}
	uniques := map[int][]string{}
	btrees := map[int][]string{}
	for _, col := range columns {
		// 列名加反引号，避免关键字冲突（rank/order/key 等）
		line := fmt.Sprintf("`%s` %s", col.Name, col.Type)
		if col.NotNull {
			line += " NOT NULL"
		}
		if col.Default != "" && col.Default != "<nil>" {
			line += fmt.Sprintf(" DEFAULT %s", col.Default)
		}
		if col.Desc != "" {
			// MySQL 列注释是内联 COMMENT '...'
			desc := strings.ReplaceAll(col.Desc, "'", "''")
			line += fmt.Sprintf(" COMMENT '%s'", desc)
		}
		lines = append(lines, line)

		quoted := "`" + col.Name + "`"
		if col.Primary {
			primarys[col.IndexGroup] = append(primarys[col.IndexGroup], quoted)
		}
		if col.Unique {
			uniques[col.IndexGroup] = append(uniques[col.IndexGroup], quoted)
		}
		if col.Btree {
			btrees[col.IndexGroup] = append(btrees[col.IndexGroup], quoted)
		}
	}
	if len(primarys) > 1 {
		return fmt.Errorf("the table can only have one primary key")
	}

	// 主键
	for i, primary := range primarys {
		if i == 0 {
			if len(primary) > 1 {
				return fmt.Errorf("the primary key can only have one column")
			}
			lines = append(lines, fmt.Sprintf("PRIMARY KEY (%s)", primary[0]))
		} else {
			lines = append(lines, fmt.Sprintf("PRIMARY KEY (%s)", strings.Join(primary, ", ")))
		}
	}

	// UNIQUE：MySQL 写法 UNIQUE KEY name (col)，放在表内
	for i, unique := range uniques {
		if i == 0 {
			for _, u := range unique {
				lines = append(lines, fmt.Sprintf("UNIQUE KEY `%s` (%s)", strings.Trim(u, "`"), u))
			}
		} else {
			cols := stripBackticks(unique)
			lines = append(lines, fmt.Sprintf("UNIQUE KEY `%s` (%s)", "uniq_"+strings.Join(cols, "_"), strings.Join(unique, ", ")))
		}
	}

	// 普通 B-Tree 索引：MySQL 用 inline KEY 即可，不需要 USING btree
	for i, btree := range btrees {
		if i == 0 {
			for _, b := range btree {
				lines = append(lines, fmt.Sprintf("KEY `idx_%s_%s` (%s)", table, strings.Trim(b, "`"), b))
			}
		} else {
			cols := stripBackticks(btree)
			lines = append(lines, fmt.Sprintf("KEY `idx_%s_%s` (%s)", table, strings.Join(cols, "_"), strings.Join(btree, ", ")))
		}
	}

	sql := fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS `%s` (\n%s\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;",
		table,
		"  "+strings.Join(lines, ",\n  "),
	)
	_, err := obj.Exec(ctx, sql)
	return err
}

// 去掉 “ ` “ 包裹，便于拼索引名
func stripBackticks(quoted []string) []string {
	out := make([]string, len(quoted))
	for i, q := range quoted {
		out[i] = strings.Trim(q, "`")
	}
	return out
}
func (obj *Client) Tables(preCtx context.Context) ([]string, error) {
	dbName := obj.option.DbName
	if dbName == "" {
		return nil, fmt.Errorf("DbName is empty")
	}
	rows, err := obj.Finds(preCtx,
		`SELECT table_name AS table_name
		   FROM information_schema.tables
		  WHERE table_schema = ? AND table_type = 'BASE TABLE'
		  ORDER BY table_name;`, dbName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []string{}
	for row := range rows.Range() {
		if table, _ := row["table_name"].(string); table != "" {
			result = append(result, table)
		}
	}
	return result, nil
}
func (obj *Client) Fields(preCtx context.Context, table string) ([]pg.Column, error) {
	if preCtx == nil {
		preCtx = context.TODO()
	}
	sql := `SELECT
    ORDINAL_POSITION AS position,
    COLUMN_NAME AS name,
    COLUMN_TYPE AS type,
    IF(IS_NULLABLE = 'NO', 'true', 'false') AS not_null,
    COLUMN_DEFAULT AS "default",
    CASE
        WHEN COLUMN_KEY = 'PRI' THEN 'PRIMARY KEY'
        WHEN COLUMN_KEY = 'UNI' THEN 'UNIQUE'
        ELSE NULL
    END AS constrainttype,
    IF(COLUMN_KEY = 'PRI', 'true', 'false') AS "primary",
    IF(COLUMN_KEY = 'UNI', 'true', 'false') AS "unique",
    IF(EXTRA LIKE '%auto_increment%', 'true', 'false') AS auto_increment
FROM information_schema.COLUMNS
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME = ?
ORDER BY ORDINAL_POSITION;`
	row, err := obj.Finds(preCtx, sql, table)
	if err != nil {
		return nil, err
	}
	defer row.Close()
	datas := []pg.Column{}
	for data := range row.Range() {
		var colum pg.Column
		for key, val := range data {
			if ival, ok := val.(string); ok {
				if ival == "true" {
					data[key] = true
				}
				if ival == "false" {
					data[key] = false
				}
				if key == "type" && strings.HasPrefix(ival, "int") {
					data[key] = "INT"
				}
			}
		}
		if _, err = gson.Decode(data, &colum); err != nil {
			return nil, err
		}
		datas = append(datas, colum)
	}
	return datas, nil
}
func (obj *Client) DBName() string {
	return obj.option.DbName
}

func NewClientWithSqlDB(db *sql.DB) *Client {
	return &Client{db: db}
}

func (obj *Client) Insert(ctx context.Context, table string, data ...any) (*Result, error) {
	if ctx == nil {
		ctx = context.TODO()
	}
	names, indexs, values, err := obj.parseInserts(data...)
	if err != nil {
		return nil, err
	}
	return obj.Exec(ctx, fmt.Sprintf("insert into %s %s values %s", table, names, indexs), values...)
}
func (obj *Client) InsertWithValues(ctx context.Context, table string, data ...[]any) (*Result, error) {
	if ctx == nil {
		ctx = context.TODO()
	}
	indexs, values := obj.parseInsertWithValues(data...)
	return obj.Exec(ctx, fmt.Sprintf("insert into %s values %s;", table, indexs), values...)
}
func (obj *Client) parseInsert(data map[string]any, keys []string) (string, []any) {
	values := []any{}
	indexs := make([]string, len(keys))
	for i, k := range keys {
		v := data[k]
		values = append(values, v)
		indexs[i] = "?"
	}
	return fmt.Sprintf("(%s)", strings.Join(indexs, ", ")), values
}
func (obj *Client) parseInsertWithValues(values ...[]any) (string, []any) {
	indexs := make([]string, len(values))
	vvs := []any{}
	for i, vs := range values {
		index := make([]string, len(vs))
		for j, v := range vs {
			index[j] = "?"
			vvs = append(vvs, v)
		}
		indexs[i] = fmt.Sprintf("(%s)", strings.Join(index, ", "))
	}
	return strings.Join(indexs, ", "), vvs
}

func (obj *Client) parseInsertKeys(data ...any) ([]map[string]any, []string, error) {
	values := []map[string]any{}
	keys := []string{}
	for _, d := range data {
		jsonData, err := gson.Decode(d)
		if err != nil {
			return nil, nil, err
		}
		value := map[string]any{}
		for key, val := range jsonData.Map() {
			value[key] = val.Value()
			if !slices.Contains(keys, key) {
				keys = append(keys, key)
			}
		}
		values = append(values, value)
	}

	return values, keys, nil
}
func (obj *Client) parseInserts(data ...any) (string, string, []any, error) {
	datas, names, err := obj.parseInsertKeys(data...)
	if err != nil {
		return "", "", nil, err
	}
	values := []any{}
	indexs := []string{}

	for _, d := range datas {
		index, value := obj.parseInsert(d, names)
		indexs = append(indexs, index)
		values = append(values, value...)
	}
	return fmt.Sprintf("(%s)", strings.Join(names, ", ")), strings.Join(indexs, ", "), values, nil
	// return fmt.Sprintf("(%s)", strings.Join(names, ", ")), fmt.Sprintf("(%s)", strings.Join(indexs, ", ")), values, nil
}

// finds   ?  is args
func (obj *Client) Finds(preCtx context.Context, query string, args ...any) (*Rows, error) {
	if preCtx == nil {
		preCtx = context.TODO()
	}
	ctx, cnl := context.WithCancel(preCtx)
	row, err := obj.db.QueryContext(ctx, query, args...)
	if err != nil {
		cnl()
		return nil, err
	}
	cols, err := row.ColumnTypes()
	if err != nil {
		cnl()
		return nil, err
	}
	names := make([]string, len(cols))
	kinds := make([]reflect.Type, len(cols))
	for coln, col := range cols {
		names[coln] = col.Name()
		kinds[coln] = col.ScanType()
	}
	return &Rows{
		names: names,
		kinds: kinds,
		rows:  row,
		cnl:   cnl,
	}, err
}

// 执行
func (obj *Client) Exec(ctx context.Context, query string, args ...any) (*Result, error) {
	if ctx == nil {
		ctx = context.TODO()
	}
	exeResult, err := obj.db.ExecContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return &Result{result: exeResult}, nil
}

// 执行
func (obj *Client) Update(ctx context.Context, table string, data any, where string, args ...any) (*Result, error) {
	if ctx == nil {
		ctx = context.TODO()
	}
	if where == "" {
		return nil, fmt.Errorf("where is empty")
	}
	jsonData, err := gson.Decode(data)
	if err != nil {
		return nil, err
	}
	names := []string{}
	values := []any{}
	for key, val := range jsonData.Map() {
		names = append(names, fmt.Sprintf("%s=?", key))
		values = append(values, val.Value())
	}
	exeResult, err := obj.db.ExecContext(ctx, fmt.Sprintf("update %s set %s where %s", table, strings.Join(names, ", "), where), append(values, args...)...)
	if err != nil {
		return nil, err
	}
	return &Result{result: exeResult}, nil
}

// 执行
func (obj *Client) Exists(ctx context.Context, table string, where string, args ...any) (bool, error) {
	if ctx == nil {
		ctx = context.TODO()
	}
	if where == "" {
		return false, fmt.Errorf("where is empty")
	}
	exeResult, err := obj.Finds(ctx, fmt.Sprintf("select 1 from %s where %s limit 1", table, where), args...)
	if err != nil {
		return false, err
	}
	var exists bool
	for range exeResult.Range() {
		exists = true
		break
	}
	return exists, nil
}

// 执行
func (obj *Client) UpSert(ctx context.Context, table string, data any, where string, args ...any) (*Result, error) {
	if ctx == nil {
		ctx = context.TODO()
	}
	results, err := obj.Update(ctx, table, data, where, args...)
	if err != nil {
		return nil, err
	}
	i, err := results.RowsAffected()
	if err != nil {
		return nil, err
	}
	if i == 0 {
		exists, err := obj.Exists(ctx, table, where, args...)
		if err != nil {
			return nil, err
		}
		if !exists {
			return obj.Insert(ctx, table, data)
		}
	}
	return results, nil
}

// 关闭客户端
func (obj *Client) Close() error {
	return obj.db.Close()
}
