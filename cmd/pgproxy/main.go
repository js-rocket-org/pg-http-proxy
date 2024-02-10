package main

/*
curl -d "{\"sql\":\"SELECT firstname,lastname,email FROM tbl_user WHERE POSITION('J' IN firstname) > 0 AND POSITION('@' IN email) > 0 LIMIT 10;\", \"parms\": [] }" \
  "http://localhost:15432/query"

curl -d '{"sql":"SELECT firstname,lastname,email FROM tbl_user WHERE POSITION($1 IN firstname) > 0 AND POSITION($2 IN email) > 0 LIMIT 10;", "parms": ["J","example"] }' \
  -H "x-api-key: 70E46E04-76B0-49A0-8106-46ABF552F16E" -H "Accept: plain/text" "http://localhost:15432/query"
*/
import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const DEFAULT_DB_PORT = 5432
const DEFAULT_PROXY_PORT = 15432
const DEFAULT_ROW_SEPERATOR = 30
const DEFAULT_COL_SEPERATOR = 31

// RequestBody is a struct to represent the JSON request body
type RequestBody struct {
	Sql   string   `json:"sql"`
	Parms []string `json:"parms"`
}

// ResponseBody is a struct to represent the JSON response body
type ResponseBody struct {
	Message string `json:"message"`
}

type DBConnection struct {
	db      *pgxpool.Pool
	context context.Context
}

type DatabaseOptions struct {
	dbHost  string
	dbPort  int
	dbName  string
	dbUser  string
	dbPass  string
	sslMode string
}

type CommandLineOptions struct {
	dbOptions DatabaseOptions
	port      int
	apiKey    string
}

type Globals struct {
	dbc    *DBConnection
	apiKey string
}

var GLOBAL = Globals{
	dbc:    nil,
	apiKey: "",
}

func getCommandLine() CommandLineOptions {
	dbOptions := DatabaseOptions{dbHost: "", dbPort: 5432, dbName: "", dbUser: "", dbPass: "", sslMode: ""}
	cmdOptions := CommandLineOptions{dbOptions: dbOptions}
	dbOpt := &cmdOptions.dbOptions

	flag.StringVar(&dbOpt.dbHost, "dbhost", "", "PostgreSQL host")
	flag.IntVar(&dbOpt.dbPort, "dbport", DEFAULT_DB_PORT, "PostgreSQL Port")
	flag.StringVar(&dbOpt.dbName, "dbname", "", "Database name")
	flag.StringVar(&dbOpt.dbUser, "dbuser", "", "User name")
	flag.StringVar(&dbOpt.dbPass, "dbpass", "", "Password")
	flag.StringVar(&dbOpt.sslMode, "dbsslmode", "disable", "SSL mode")

	flag.IntVar(&cmdOptions.port, "port", DEFAULT_PROXY_PORT, "Proxy lister port")
	flag.StringVar(&cmdOptions.apiKey, "apikey", "", "API key to protect access")

	flag.Parse()

	if dbOpt.dbHost == "" {
		log.Fatal("Error -dbhost parameter must be provided")
	}

	if dbOpt.dbName == "" {
		log.Fatal("Error -dbname parameter must be provided")
	}

	if dbOpt.dbUser == "" {
		log.Fatal("Error -dbuser parameter must be provided")
	}

	if dbOpt.dbPass == "" {
		log.Fatal("Error -dbpass parameter must be provided")
	}

	if cmdOptions.apiKey == "" {
		log.Fatal("Error -apikey parameter must be provided")
	}

	return cmdOptions

}

func dbConnect(dbOpts DatabaseOptions) (*DBConnection, error) {
	dsn := fmt.Sprintf("postgresql://%s:%s@%s:%d/%s?sslmode=disable",
		dbOpts.dbUser, dbOpts.dbPass, dbOpts.dbHost, dbOpts.dbPort, dbOpts.dbName)

	ctx := context.Background()

	poolConfig, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}

	// poolConfig.MaxConnLifetime = 30 * time.Second
	// poolConfig.MaxConnIdleTime = 5 * time.Second
	// poolConfig.BeforeConnect = func(context.Context, *pgx.ConnConfig) error {
	// 	fmt.Println("Before connect")
	// 	return nil
	// }
	// poolConfig.BeforeClose = func(*pgx.Conn) {
	// 	fmt.Println("Closing a connection")
	// }

	db, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, err
	}

	err = db.Ping(ctx)
	if err != nil {
		log.Fatal("Error pinging database:", err)
		os.Exit(1)
	}
	fmt.Println("Connected to the database!")

	dbc := DBConnection{db: db, context: context.Background()}

	return &dbc, nil
}

func bytesToUUID(value interface{}) string {
	uuidbytes := value.([16]byte)
	return fmt.Sprintf("%x-%x-%x-%x-%x", uuidbytes[0:4], uuidbytes[4:6], uuidbytes[6:8], uuidbytes[8:10], uuidbytes[10:])
}

func getRowData(rows pgx.Rows, columns []pgconn.FieldDescription) ([]interface{}, error) {
	values, err := rows.Values()
	if err != nil {
		return nil, err
	}

	var rowData []interface{}
	valuesLen := len(values)
	for i := 0; i < valuesLen; i++ {
		value := values[i]
		colType := columns[i].DataTypeOID
		if colType == 2950 {
			rowData = append(rowData, bytesToUUID(value))
		} else {
			rowData = append(rowData, value)
		}
	}

	return rowData, nil
}

func dbQuery(dbc *DBConnection, sql string, parms []string) ([]interface{}, error) {

	var params []interface{}
	parmsLen := len(parms)
	for i := 0; i < parmsLen; i++ {
		element := parms[i]
		params = append(params, element)
	}

	rows, err := dbc.db.Query(dbc.context, sql, params...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []interface{}

	// Get column names and make it the first row in the data
	columns := rows.FieldDescriptions()
	columnsLen := len(columns)
	var columnNames []interface{}
	for i := 0; i < columnsLen; i++ {
		columnNames = append(columnNames, columns[i].Name)
	}
	results = append(results, columnNames)

	// Get data rows
	rowCount := 0
	for rows.Next() {
		rowData, err := getRowData(rows, columns)
		if err != nil {
			return nil, err
		}
		results = append(results, rowData)
		rowCount++
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return results, nil
}

func writeCompactJSONOutput(w http.ResponseWriter, rows []interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	var result []interface{}

	// Use first row to get the number of columns
	firstRow := rows[0].([]interface{})
	colLen := len(firstRow)

	rowsLen := len(rows)
	for i := 0; i < rowsLen; i++ {
		row := rows[i].([]interface{})
		var newRow []interface{}
		for c := 0; c < colLen; c++ {
			value := row[c]
			newRow = append(newRow, value)
		}
		result = append(result, newRow)
	}

	err := json.NewEncoder(w).Encode(result)
	if err != nil {
		http.Error(w, "Error encoding JSON response", http.StatusInternalServerError)
	}
}

func writeJSONOutput(w http.ResponseWriter, rows []interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	var result []map[string]interface{}

	// Use first row to get the number of columns
	firstRow := rows[0].([]interface{})
	colLen := len(firstRow)

	rowsLen := len(rows)
	for i := 1; i < rowsLen; i++ {
		row := rows[i].([]interface{})
		newRow := make(map[string]interface{})
		for c := 0; c < colLen; c++ {
			colName := firstRow[c].(string)
			value := row[c]
			newRow[colName] = value
		}
		result = append(result, newRow)
	}

	err := json.NewEncoder(w).Encode(result)
	if err != nil {
		http.Error(w, "Error encoding JSON response", http.StatusInternalServerError)
	}
}

func writeListOutput(w http.ResponseWriter, rows []interface{}, rowSep byte, colSep byte) {
	w.Header().Set("Content-Type", "plain/text")
	w.WriteHeader(http.StatusOK)

	var rowString strings.Builder

	// Use first row to get the number of columns
	firstRow := rows[0].([]interface{})
	colLen := len(firstRow)

	// Remaining row is data
	rowsLen := len(rows)
	for i := 0; i < rowsLen; i++ {
		row := rows[i].([]interface{})
		rowString.Reset()
		for col := 0; col < colLen; col++ {
			value := row[col]
			if col > 0 {
				rowString.WriteByte(colSep)
			}
			rowString.WriteString(fmt.Sprintf("%v", value))
		}
		rowString.WriteByte(rowSep)
		w.Write([]byte(rowString.String()))
	}
}

func getListSeparators(rowInput string, colInput string) (byte, byte) {
	rowChar, rowErr := strconv.Atoi(rowInput)
	if rowErr != nil || !(rowChar >= 0 && rowChar <= 255) {
		rowChar = DEFAULT_ROW_SEPERATOR
	}
	colChar, colErr := strconv.Atoi(colInput)
	if colErr != nil || !(colChar >= 0 && colChar <= 255) {
		colChar = DEFAULT_COL_SEPERATOR
	}

	return byte(rowChar), byte(colChar)
}

func proxyHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Error method"+r.Method, http.StatusNotFound)
		return
	}

	if r.Header.Get("x-api-key") != GLOBAL.apiKey {
		w.WriteHeader(http.StatusForbidden)
		return
	}

	// Decode JSON request body
	var requestData RequestBody
	err := json.NewDecoder(r.Body).Decode(&requestData)
	if err != nil {
		http.Error(w, "Invalid JSON request", http.StatusBadRequest)
		return
	}

	// Process the request and generate response
	result, err := dbQuery(GLOBAL.dbc, requestData.Sql, requestData.Parms)
	if err != nil {
		w.Header().Set("Content-Type", "plain/text")
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, err.Error())
		return
	}

	if result == nil || len(result) <= 1 {
		w.Header().Set("Content-Type", "plain/text")
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, "")
		return
	}

	if r.Header.Get("Accept") == "application/json" {
		if r.Header.Get("x-compact-json") == "true" {
			writeCompactJSONOutput(w, result)
		} else {
			writeJSONOutput(w, result)
		}
	} else {
		rowChar, colChar := getListSeparators(r.Header.Get("x-rowsep"), r.Header.Get("x-colsep"))
		writeListOutput(w, result, rowChar, colChar)
	}
}

func main() {
	cmdOptions := getCommandLine()

	dbc, err := dbConnect(cmdOptions.dbOptions)
	if err != nil {
		log.Fatalln(err)
	}

	// set global variables
	GLOBAL.dbc = dbc
	GLOBAL.apiKey = cmdOptions.apiKey

	http.HandleFunc("/query", proxyHandler)

	fmt.Printf("PGProxy running on port %d\n", cmdOptions.port)

	err = http.ListenAndServe(":"+strconv.Itoa(cmdOptions.port), nil)
	if err != nil {
		fmt.Println("Error starting server:", err)
	}
}
