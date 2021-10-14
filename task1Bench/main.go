package main

//Libraries
import (
	"context"
	"database/sql"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"sync"
	"time"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/jackc/pgx/v4"
	"github.com/jinzhu/gorm"
	//_ "github.com/jinzhu/gorm/dialects/postgres"
	"github.com/jszwec/csvutil"
	//_ "github.com/lib/pq"
)

//GlobalVariables for all dbs
var (
	db      *sql.DB
	GenData []model
	wg      sync.WaitGroup
	//ctx context.Background()
)

//Credentials for the database
var (
	user   string = "postgres"
	pword  string = "postgre"
	host   string = "localhost"
	port   int    = 5432
	dbname string = "benchDB"
)

//doing an initial setup for our database
func init() {
	db, _ = sql.Open("postgres", fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", host, port, user, pword, dbname))
	//db.SetMaxOpenConns(1)
	err := db.Ping()
	if err != nil {
		panic(err)
	}
}

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

//Sample Data Generartion Function
func DataGenerator(numDat int) {

	tempDat := []model{}

	for i := 0; i < numDat; i++ {
		tempDat = append(tempDat, model{
			i + 1,
			gofakeit.Name(),
			gofakeit.CarMaker(),
			gofakeit.Gender(),
			gofakeit.SSN(),
			gofakeit.Email(),
			gofakeit.StreetNumber(),
			gofakeit.Phone(),
			gofakeit.Phone(),
			gofakeit.AppName(),
			gofakeit.JobTitle(),
			gofakeit.JobLevel(),
			gofakeit.Company(),
			gofakeit.Name(),
			gofakeit.Name(),
			gofakeit.Street(),
			gofakeit.StreetName(),
			gofakeit.City(),
			gofakeit.State(),
			gofakeit.Country(),
			gofakeit.Zip(),
		})
	}
	//Saving the data to a csv file
	b, err := csvutil.Marshal(tempDat)
	if err != nil {
		fmt.Println("error:", err)
	}
	err = ioutil.WriteFile("Sample-data.csv", b, 0644)
	if err != nil {
		log.Fatalln("Error during writing file", err)
	}
	//Assigning the data to our global data variable
	GenData = tempDat
}

func ReadData(filename string) (retData []model) {
	f, err := os.Open("Sample-data.csv")

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

//Few defaults for gorm table selection
func (model) TableName() string {
	return "test"
}

//Setup we need before running and connecting to database
func Setup() {
	//Create Table
	createQ := `
	   	   	CREATE TABLE IF NOT EXISTS test(
					id serial PRIMARY KEY,
					name varchar(50) NOT NULL,
					car_maker varchar(50) NOT NULL,
					gender varchar(10) NOT NULL,
					ssn varchar(20) NOT NULL,
	   	   			email varchar(150) NOT NULL,
					address varchar(200) NOT NULL,
					phone varchar(20) NOT NULL,
					phone2 varchar(20) NOT NULL,
					credit_card varchar(60) NOT NULL,
					job_title varchar(20) NOT NULL,
					level varchar(30) NOT NULL,
					company varchar(100) NOT NULL,
					father_n varchar(50) NOT NULL,
					mother_n varchar(50) NOT NULL,
					street varchar(60) NOT NULL,
					street_n varchar(100) NOT NULL,
					city varchar(100) NOT NULL,
					state varchar(100) NOT NULL,
					country varchar(100) NOT NULL,
					zip varchar(10) NOT NULL				
	   	   	);`

	_, err := db.Exec(createQ)

	if err != nil {
		panic(err)
	}
	//Generating sample Data
	if _, err := os.Open("Sample-data.csv"); err == nil {
		//fmt.Printf("File exists\n")
		GenData = ReadData("Sample-data.csv")

	} else {
		//fmt.Printf("File does not exist\n")
		DataGenerator(100000)
	}

}

//Cleaning up the database after the operations
func Cleanup() {
	_, err := db.Exec(`DROP TABLE test`)
	if err != nil {
		panic(err)
	}
}

// Functions for GORM library
func InsertionGORM(db *gorm.DB, dat model) {
	//Inserting Data
	db.Create(&dat)
}

func FetchGORM(db *gorm.DB, id int) model {
	dat := model{}
	db.First(&dat, id)
	return dat
}

//-

// Functions for Native SQL Library
func InsertionNative(db *sql.DB, dat model) {
	_, err := db.Exec(fmt.Sprintf("INSERT INTO test values('%d', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s') ON CONFLICT DO NOTHING;", dat.ID, dat.Name, dat.CarMaker, dat.Gender, dat.SSN, dat.Email, dat.Address, dat.Phone, dat.Phone2, dat.CreditCardNum, dat.JobTitle, dat.Level, dat.Company, dat.FatherName, dat.MotherName, dat.Street, dat.StreetName, dat.City, dat.State, dat.Country, dat.Zip))
	if err != nil {
		log.Fatalln("error inserting data", err)
	}

}

func FetchNative(db *sql.DB, id int) model {
	dat := model{}
	err := db.QueryRow("SELECT * FROM test WHERE id = $1", id).Scan(&dat.ID, &dat.Name, &dat.CarMaker, &dat.Gender, &dat.SSN, &dat.Email, &dat.Address, &dat.Phone, &dat.Phone2, &dat.CreditCardNum, &dat.JobTitle, &dat.Level, &dat.Company, &dat.FatherName, &dat.MotherName, &dat.Street, &dat.StreetName, &dat.City, &dat.State, &dat.Country, &dat.Zip)
	if err != nil {
		log.Fatal("Failed to execute query: ", err)
	}
	return dat
}

//-

// Fucntions for Pgx Library
func InsertionPgx(conn *pgx.Conn, dat model) {
	if _, err := conn.Exec(context.Background(), fmt.Sprintf("INSERT INTO test values('%d', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s') ON CONFLICT DO NOTHING;", dat.ID, dat.Name, dat.CarMaker, dat.Gender, dat.SSN, dat.Email, dat.Address, dat.Phone, dat.Phone2, dat.CreditCardNum, dat.JobTitle, dat.Level, dat.Company, dat.FatherName, dat.MotherName, dat.Street, dat.StreetName, dat.City, dat.State, dat.Country, dat.Zip)); err != nil {
		fmt.Println("Unable to insert Data", err)
		return
	}

}

func FetchPgx(conn *pgx.Conn, id int) model {
	tempdat := model{}

	conn.QueryRow(context.Background(), "SELECT * FROM test WHERE id=$1;", id).Scan(&tempdat)
	return tempdat
}

//-

// Functions for PgxPool
// Under development

//Concurrent Functions

func ConInsertionNative(db *sql.DB, dat model, wg sync.WaitGroup) {

	_, err := db.Exec(fmt.Sprintf("INSERT INTO test values('%d', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s') ON CONFLICT DO NOTHING;", dat.ID, dat.Name, dat.CarMaker, dat.Gender, dat.SSN, dat.Email, dat.Address, dat.Phone, dat.Phone2, dat.CreditCardNum, dat.JobTitle, dat.Level, dat.Company, dat.FatherName, dat.MotherName, dat.Street, dat.StreetName, dat.City, dat.State, dat.Country, dat.Zip))
	if err != nil {
		log.Fatalln("error inserting data", err)
	}
	wg.Done()

}

func main() {
	Setup()
	//fmt.Println(GenData[8])
	defer Cleanup()
	defer db.Close()
	//g, _ := gorm.Open("postgres", db)
	//defer g.Close()
	//defer db.Close()
	//g.AutoMigrate(&model)
	//InsertionGORM(g, GenData[2])

	//conn, _ := pgx.Connect(context.Background(), "postgres://postgres:postgres@localhost:5432/benchDB")
	//defer conn.Close(context.Background())

	//conn, _ := pgxpool.Connect(context.Background(), "postgres://postgres:postgres@localhost:5432/benchDB")
	//defer conn.Close()
	t1 := time.Now()
	//conn.Config().MaxConns = 100
	for i := 0; i < 10000; {
		for k := 0; k < 80; k++ {

			i++
			wg.Add(1)
			//InsertionNative(db, GenData[i])
			//InsertionGORM(g, GenData[i])
			//InsertionPgx(conn, GenData[i])

			//go InsertionPgxPool(conn, GenData[i])
			go InsertionNative(db, GenData[i])
		}
		wg.Wait()
		//go InsertionPgxPool(conn, GenData[i])

	}

	fmt.Println("Time Taken for insertion :- ", time.Since(t1))
	//fmt.Println(FetchNative(db, 4))
	//fmt.Println(FetchGORM(g, 10))
	//fmt.Println(FetchPgx(conn, 10))
	//fmt.Println(FetchPgxPool(conn, 10))

}
