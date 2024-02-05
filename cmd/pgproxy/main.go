package main

/*
curl -d "{\"sql\":\"SELECT firstname,lastname,email FROM tbl_user WHERE POSITION('J' IN firstname) > 0 AND POSITION('@' IN email) > 0 LIMIT 10;\", \"parms\": [] }" \
  "http://localhost:15432/query"

curl -d '{"sql":"SELECT firstname,lastname,email FROM tbl_user WHERE POSITION($1 IN firstname) > 0 AND POSITION($2 IN email) > 0 LIMIT 10;", "parms": ["J","example"] }' \
  -H "x-api-key: 70E46E04-76B0-49A0-8106-46ABF552F16E" "http://localhost:15432/query"
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

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

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
	dbPort  string
	dbName  string
	dbUser  string
	dbPass  string
	sslMode string
}

type CommandLineOptions struct {
	dbOptions DatabaseOptions
	port      string
	apiKey    string
}

var globalDBC *DBConnection
var API_KEY string

const SEPERATOR = "\t"

func getCommandLine() CommandLineOptions {
	dbOptions := DatabaseOptions{dbHost: "", dbPort: "", dbName: "", dbUser: "", dbPass: "", sslMode: ""}
	cmdOptions := CommandLineOptions{dbOptions: dbOptions}
	dbOpt := &cmdOptions.dbOptions

	flag.StringVar(&dbOpt.dbHost, "dbhost", "", "PostgreSQL host")
	flag.StringVar(&dbOpt.dbPort, "dbport", "5432", "PostgreSQL Port")
	flag.StringVar(&dbOpt.dbName, "dbname", "", "Database name")
	flag.StringVar(&dbOpt.dbUser, "dbuser", "", "User name")
	flag.StringVar(&dbOpt.dbPass, "dbpass", "", "Password")
	flag.StringVar(&dbOpt.sslMode, "dbsslmode", "disable", "SSL mode")
	var portNumber int
	flag.IntVar(&portNumber, "port", 15432, "Proxy lister port")
	cmdOptions.port = strconv.Itoa(portNumber)
	flag.StringVar(&cmdOptions.apiKey, "apikey", "", "Proxy lister port")
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
	dsn := fmt.Sprintf("postgresql://%s:%s@%s:%s/%s?sslmode=disable",
		dbOpts.dbUser, dbOpts.dbPass, dbOpts.dbHost, dbOpts.dbPort, dbOpts.dbName)

	ctx := context.Background()

	db, err := pgxpool.New(ctx, dsn)
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

func getRowData(rows pgx.Rows, columns []pgconn.FieldDescription) (map[string]interface{}, error) {
	values, err := rows.Values()
	if err != nil {
		return nil, err
	}

	rowData := make(map[string]interface{})
	for i, value := range values {
		colName := string(columns[i].Name)
		colType := columns[i].DataTypeOID
		if colType == 2950 {
			rowData[colName] = bytesToUUID(value)
		} else {
			rowData[colName] = value
		}
	}

	return rowData, nil
}

func dbQuery(dbc *DBConnection, sql string, parms []string) ([]map[string]interface{}, error) {

	var params []interface{}
	for _, element := range parms {
		params = append(params, element)
	}

	rows, err := dbc.db.Query(dbc.context, sql, params...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []map[string]interface{}

	columns := rows.FieldDescriptions()

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

	// Convert results slice to JSON
	// jsonData, err := json.Marshal(results)
	// if err != nil {
	// 	return "Error marshaling JSON", err
	// }

	return results, nil
}

func proxyHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Error method"+r.Method, http.StatusNotFound)
		return
	}

	if r.Header.Get("x-api-key") != API_KEY {
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
	result, err := dbQuery(globalDBC, requestData.Sql, requestData.Parms)
	if err != nil {
		w.Header().Set("Content-Type", "plain/text")
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, err.Error())
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	//fmt.Fprint(w, result)

	if result == nil {
		fmt.Fprint(w, "")
		return
	}

	// Encode JSON response
	err = json.NewEncoder(w).Encode(result)
	if err != nil {
		http.Error(w, "Error encoding JSON response", http.StatusInternalServerError)
		return
	}
}

func main() {
	cmdOptions := getCommandLine()

	API_KEY = cmdOptions.apiKey

	dbc, err := dbConnect(cmdOptions.dbOptions)
	if err != nil {
		log.Fatalln(err)
	}

	globalDBC = dbc

	// Define a handler function
	http.HandleFunc("/query", proxyHandler)

	fmt.Printf("PGProxy running on port %s\n", cmdOptions.port)

	err = http.ListenAndServe(":"+cmdOptions.port, nil)
	if err != nil {
		fmt.Println("Error starting server:", err)
	}
}
