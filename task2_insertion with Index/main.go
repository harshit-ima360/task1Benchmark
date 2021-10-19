package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/jszwec/csvutil"
	_ "github.com/lib/pq"
)

//GlobalVariables for all dbs
var (
	GenData []model
	conn    *pgx.Conn
)

//Credentials for the database
var (
	user   string = "postgres"
	pword  string = "postgre"
	host   string = "localhost"
	port   int    = 5432
	dbname string = "benchDB"
)

/* //doing an initial setup for our database
func init() {
	//db, _ = sql.Open("postgres", fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", host, port, user, pword, dbname))
	//db.SetMaxOpenConns(1)
	err := db.Ping()
	if err != nil {
		panic(err)
	}
} */

//Our model struct for storing the fields required
type model struct {
	ID            int    `gorm:"column:id" db:"id"`
	Name          string `gorm:"column:name" db:"name"`
	CarMaker      string `gorm:"column:car_maker" db:"car_maker"`
	Gender        string `gorm:"column:gender" db:"gender"`
	SSN           string `gorm:"column:ssn" db:"ssn"`
	Email         string `gorm:"column:email" db:"email"`
	Address       string `gorm:"column:address" db:"address"`
	Phone         string `gorm:"column:phone" db:"phone"`
	Phone2        string `gorm:"column:phone2" db:"phone2"`
	CreditCardNum string `gorm:"column:credit_card" db:"credit_card"`
	JobTitle      string `gorm:"column:job_title" db:"job_title"`
	Level         string `gorm:"column:level" db:"level"`
	Company       string `gorm:"column:company" db:"company"`
	FatherName    string `gorm:"column:father_n" db:"father_n"`
	MotherName    string `gorm:"column:mother_n" db:"mother_n"`
	Street        string `gorm:"column:street" db:"street"`
	StreetName    string `gorm:"column:street_n" db:"street_n"`
	City          string `gorm:"column:city" db:"city"`
	State         string `gorm:"column:state" db:"state"`
	Country       string `gorm:"column:country" db:"country"`
	Zip           string `gorm:"column:zip" db:"zip"`
}

func ReadData(filename string) (retData []model) {
	f, err := os.Open(filename)

	if err != nil {
		log.Fatal("error readng file", err)
	}
	tempF, _ := ioutil.ReadAll(f)

	if err := csvutil.Unmarshal(tempF, &retData); err != nil {
		log.Fatal("Error unmarshalling")
	}
	//fmt.Println("Data unmarshaled")
	return
}

func ClearIndices(col string) {
	_, err := conn.Exec(context.Background(), fmt.Sprintf("DROP INDEX %sindex;", col))
	if err != nil {
		log.Fatal("Unable to remove indexings - ", col, " ", err)
	}
	fmt.Printf("\nCleaned the %s indexings", col)
}

func CreateIndexing(col string) {
	_, err := conn.Exec(context.Background(), fmt.Sprintf("CREATE INDEX %sindex ON test(%s);", col, col))
	if err != nil {
		log.Fatal("Unable to Create indexings - ", col, " ", err)
	}
	fmt.Printf("\nCreated the %s indexings on test", col)
}

// Fucntions for Pgx Library
func InsertionPgx(dat model) {
	if _, err := conn.Exec(context.Background(), fmt.Sprintf("INSERT INTO test(name, car_maker, gender, ssn, email, address, phone, phone2, credit_card, job_title,level, company, father_n, mother_n, street, street_n, city,state, country, zip) values('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s') ON CONFLICT DO NOTHING;", dat.Name, dat.CarMaker, dat.Gender, dat.SSN, dat.Email, dat.Address, dat.Phone, dat.Phone2, dat.CreditCardNum, dat.JobTitle, dat.Level, dat.Company, dat.FatherName, dat.MotherName, dat.Street, dat.StreetName, dat.City, dat.State, dat.Country, dat.Zip)); err != nil {
		fmt.Println("Unable to insert Data", err)
		return
	}

}

func main() {

	//defer db.Close()
	//Using the PGX ibrary
	GenData = ReadData("Sample-data.csv")
	var err error
	conn, err = pgx.Connect(context.Background(), "postgres://postgres:postgre@localhost:5432/benchDB")
	if err != nil {
		log.Fatal("Connection not established - ", err)
	}
	defer conn.Close(context.Background())
	tests_Name := []string{"No Indexings", "1 Indexing", "10 Indexings"}
	InsertionNums := []int{100, 500, 1000, 10000}
	for _, t := range tests_Name {
		fmt.Println("\n\nTesting with ", t)
		if t == "1 Indexing" {
			CreateIndexing("id")
			defer ClearIndices("id")

		}

		if t == "10 Indexings" {
			colList := []string{"name", "ssn", "email", "phone", "company", "state", "city", "credit_card", "phone2"}
			t2 := time.Now()
			for _, c := range colList {
				CreateIndexing(c)
				defer ClearIndices(c)
			}
			fmt.Println("Time taken for indexing is ", time.Since(t2))
		}
		for _, Inum := range InsertionNums {
			t1 := time.Now()
			TotalInserts := 1
			for i := 1; i < Inum; i++ {

				randId := rand.Intn(10000) + 1
				InsertionPgx(GenData[randId])
				TotalInserts += 1

			}
			fmt.Printf("\nTried %v Done %v Insertions in %s", Inum, TotalInserts, time.Since(t1))
		}
	}
	// fmt.Println(GenData[100])
	// InsertionPgx(GenData[100])

}
