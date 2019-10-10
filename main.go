package main

import (
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	. "strings"

	"github.com/gorilla/mux"
	_ "github.com/lib/pq"
)

type Company struct {
	Id      string `json:"Id,omitempty"`
	Name    string `json:"Name,omitempty"`
	Zip     string `json:"Zip,omitempty"`
	Website string `json:"Website,omitempty"`
}

var body struct {
	Path string `json:"path"`
}

var match struct {
	Name string `json:"Name"`
	Zip  string `json:"Zip"`
}

var companies []Company
var db *sql.DB
var err error

func main() {

	dbConfig := fmt.Sprintf("host=localhost port=5432 user=postgres password=1234 dbname=yawoen " +
		"sslmode=disable")

	db, err = sql.Open("postgres", dbConfig)
	if err != nil {
		panic(err)
	}

	//	fmt.Println(companies)
	router := mux.NewRouter()
	router.HandleFunc("/loadData/", LoadDataDB).Methods("POST")
	router.HandleFunc("/mergeData/", MergeDataDB).Methods("POST")
	router.HandleFunc("/matchData/", MatchDataDB).Methods("POST")
	router.HandleFunc("/company/", GetCompanies).Methods("GET")
	router.HandleFunc("/company/{id}", GetCompany).Methods("GET")
	log.Fatal(http.ListenAndServe(":8000", router))

}

func prepareDB() {
	deleteTable := `DROP TABLE IF EXISTS company;`

	_, err = db.Exec(deleteTable)
	if err != nil {
		panic(err)
	}

	createTable := "CREATE TABLE IF NOT EXISTS company (" +
		"Id serial primary key," +
		" name varchar," +
		"  zip varchar" +
		" );"

	_, err = db.Exec(createTable)
	if err != nil {
		panic(err)
	}
}

func LoadDataDB(w http.ResponseWriter, r *http.Request) {
	prepareDB()
	//Insert the rows, omitting the first header row from the CSV.
	stmt, err := db.Prepare(`INSERT INTO company (Id, name, zip) VALUES ($1, $2, $3);`)
	if err != nil {
		log.Fatal(err)
	}

	_ = json.NewDecoder(r.Body).Decode(&body)
	csvFile, _ := os.Open(string(body.Path))
	reader := csv.NewReader(csvFile)
	reader.Comma = ';'
	id := 0
	for {
		line, error := reader.Read()
		if error == io.EOF {
			break
		} else if error != nil {
			log.Fatal(error)
		} else if id == 0 {
			id++
			continue
		}

		//	companies = append(companies, Company{Id: strconv.Itoa(id), Name: line[0], Zip: line[1]})
		line[0] = ToUpper(line[0])
		_, err = stmt.Exec(id, line[0], line[1])
		if err != nil {
			log.Fatal(err)
		}
		id++
	}
	w.Header().Set("Content-Type", "application/json")

	w.WriteHeader(http.StatusCreated)

	json.NewEncoder(w).Encode("Created. CSV PATH: " + body.Path)

}

func MergeDataDB(w http.ResponseWriter, r *http.Request) {
	//prepareDB()
	//Insert the rows, omitting the first header row from the CSV.

	altertable := "ALTER TABLE company ADD COLUMN website VARCHAR DEFAULT '';"

	_, err = db.Exec(altertable)
	if err != nil {
		panic(err)
	}

	stmt, err := db.Prepare(`UPDATE company set website=($1) where Name = ($2) and Zip =($3);`)
	if err != nil {
		log.Fatal(err)
	}

	_ = json.NewDecoder(r.Body).Decode(&body)
	csvFile, _ := os.Open(string(body.Path))
	reader := csv.NewReader(csvFile)
	reader.Comma = ';'
	id := 0
	for {
		line, error := reader.Read()
		if error == io.EOF {
			break
		} else if error != nil {
			log.Fatal(error)
		} else if id == 0 {
			id++
			continue
		}
		//append website
		//	companies = append(companies, Company{Id: strconv.Itoa(id), Name: line[0], Zip: line[1], website: line[2]})
		_, err = stmt.Exec(line[2], ToUpper(line[0]), line[1])
		if err != nil {
			log.Fatal(err)
		}
		id++
	}
	w.Header().Set("Content-Type", "application/json")

	w.WriteHeader(http.StatusCreated)

	json.NewEncoder(w).Encode("Merged. CSV PATH: " + body.Path)

}

func GetCompany(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)

	var numberColumns int

	row := db.QueryRow("select count(*) from information_schema.columns where table_name='company';")

	row.Scan(&numberColumns)

	rows, err := db.Query(`SELECT * FROM company where id= ($1)`, params["id"])
	if err != nil {
		log.Fatal(err)
	}

	var u Company
	defer rows.Close()
	for rows.Next() {

		if numberColumns == 3 {
			err := rows.Scan(&u.Id, &u.Name, &u.Zip)
			if err != nil {
				log.Fatal(err)
			}
		}
		if numberColumns == 4 {
			err := rows.Scan(&u.Id, &u.Name, &u.Zip, &u.Website)
			if err != nil {
				log.Fatal(err)
			}
		}
	}
	err = rows.Err()
	if err != nil {
		log.Fatal(err)
	}
	w.Header().Set("Content-Type", "application/json")

	json.NewEncoder(w).Encode(u)

}

func GetCompanies(w http.ResponseWriter, r *http.Request) {

	var numberColumns int

	row := db.QueryRow("select count(*) from information_schema.columns where table_name='company';")

	row.Scan(&numberColumns)

	rows, err := db.Query(`SELECT * FROM company`)

	if err != nil {
		log.Fatal(err)
	}
	var u Company
	defer rows.Close()
	for rows.Next() {
		if numberColumns == 3 {
			err := rows.Scan(&u.Id, &u.Name, &u.Zip)
			if err != nil {
				log.Fatal(err)
			}
		}
		if numberColumns == 4 {
			err := rows.Scan(&u.Id, &u.Name, &u.Zip, &u.Website)
			if err != nil {
				log.Fatal(err)
			}
		}

		companies = append(companies, Company{Id: u.Id, Name: u.Name, Zip: u.Zip, Website: string(u.Website)})
	}
	err = rows.Err()
	if err != nil {
		log.Fatal(err)
	}

	w.Header().Set("Content-Type", "application/json")

	json.NewEncoder(w).Encode(companies)
}

func MatchDataDB(w http.ResponseWriter, r *http.Request) {
	var numberColumns int

	row := db.QueryRow("select count(*) from information_schema.columns where table_name='company';")

	row.Scan(&numberColumns)

	_ = json.NewDecoder(r.Body).Decode(&match)
	name := string(match.Name)
	zip := string(match.Zip)

	rows, err := db.Query(`SELECT * FROM company where name like ($1) and zip like ($2)`, "%"+name+"%", "%"+zip+"%")
	if err != nil {
		log.Fatal(err)
	}
	var u Company
	defer rows.Close()
	for rows.Next() {

		if numberColumns == 3 {
			err := rows.Scan(&u.Id, &u.Name, &u.Zip)
			if err != nil {
				log.Fatal(err)
			}
		}
		if numberColumns == 4 {
			err := rows.Scan(&u.Id, &u.Name, &u.Zip, &u.Website)
			if err != nil {
				log.Fatal(err)
			}
		}
	}
	err = rows.Err()
	if err != nil {
		log.Fatal(err)
	}
	w.Header().Set("Content-Type", "application/json")

	json.NewEncoder(w).Encode(u)

}
