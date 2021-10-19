package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/postgres"

	_ "github.com/lib/pq"
)

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

func init() {

	db, _ = sql.Open("postgres", fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", host, port, user, pword, dbname))
	//db.SetMaxOpenConns(1)
	err := db.Ping()
	if err != nil {
		panic(err)
	}

}

// For Gorm
func (model) TableName() string {
	return "test"
}

func FetchNative(id int) (model, error) {
	dat := model{}
	err := db.QueryRow("SELECT * FROM test WHERE id = $1", id).Scan(&dat.ID, &dat.Name, &dat.CarMaker, &dat.Gender, &dat.SSN, &dat.Email, &dat.Address, &dat.Phone, &dat.Phone2, &dat.CreditCardNum, &dat.JobTitle, &dat.Level, &dat.Company, &dat.FatherName, &dat.MotherName, &dat.Street, &dat.StreetName, &dat.City, &dat.State, &dat.Country, &dat.Zip)
	if err != nil {
		log.Fatal("Failed to execute query: ", err)
		return dat, nil
	}
	return dat, nil
}

func FetchNativeByColumn(state string) int {
	var count int
	err := db.QueryRow("SELECT COUNT(*) FROM test WHERE state = $1", state).Scan(&count)
	if err != nil {
		log.Fatal(err)
	}

	return count
}

func FetchGORM(db *gorm.DB, id int) model {
	dat := model{}
	db.First(&dat, id)

	return dat
}

func FetchGORMByColumn(db *gorm.DB, state string) int {
	var TempModel model
	//TempModel.State = state
	//var count int
	//db.Where("state = ?", state).Find(&models)
	//db.Table("test").Select("COUNT(*)").Where("state = ?", state).Scan(&models)
	//_ = db.Raw("SELECT COUNT(*) FROM test WHERE state = ?", state).Scan(&count)
	result := db.Find(&TempModel, "state = ?", state)
	//fmt.Println(TempModel)
	return int(result.RowsAffected)
}

func FetchPgx(conn *pgx.Conn, id int) model {
	dat := model{}

	conn.QueryRow(context.Background(), "SELECT * FROM test WHERE id=$1;", id).Scan(&dat.ID, &dat.Name, &dat.CarMaker, &dat.Gender, &dat.SSN, &dat.Email, &dat.Address, &dat.Phone, &dat.Phone2, &dat.CreditCardNum, &dat.JobTitle, &dat.Level, &dat.Company, &dat.FatherName, &dat.MotherName, &dat.Street, &dat.StreetName, &dat.City, &dat.State, &dat.Country, &dat.Zip)
	return dat
}

func FetchPgxByColumn(conn *pgx.Conn, state string) int {
	//dat := model{}
	var count int
	err := conn.QueryRow(context.Background(), "SELECT COUNT(*) FROM test WHERE state=$1;", state).Scan(&count)
	if err != nil {
		log.Fatal(err)
	}

	return count
}

func main() {

	defer db.Close()
	States := []string{"Kansas", "Rhode Island", "Texas", "Alaska", "North Dakota", "Iowa", "Massachusetts", "Pennsylvania", "New Jersey", "East Damore"}

	Iters_list := []int{
		100,
		500,
		1000,
	}
	test_iter_num := 3
	FetchByID := false
	//"Native SQL", "GORM", "PGX"
	package_list := []string{"GORM"}

	//Setup for GORM
	g, err := gorm.Open("postgres", db)
	if err != nil {
		log.Panic("not able to open connection ", err)
	}
	defer g.Close()

	//Setup for PGX
	conn, err := pgx.Connect(context.Background(), "postgres://postgres:postgre@localhost:5432/benchDB")

	if err != nil {
		log.Panic("not able to open connection ", err)
	}
	defer conn.Close(context.Background())

	for _, p_name := range package_list {
		fmt.Println("\nResults for ", p_name)

		// Loop for test iterations
		for test_iter := 1; test_iter < test_iter_num+1; test_iter++ {
			fmt.Println("\nTest number :- ", test_iter)

			for _, j := range Iters_list {
				t1 := time.Now()
				var Entry_num int
				for i := 0; i < j; i++ {
					if FetchByID {
						randomId := rand.Intn(100000) + 1
						if p_name == "Native SQL" {
							FetchNative(randomId)
						} else if p_name == "GORM" {
							FetchGORM(g, randomId)
						} else {
							FetchPgx(conn, randomId)
						}
						Entry_num += 1

					} else {
						if p_name == "Native SQL" {
							Entry_num += FetchNativeByColumn(States[rand.Intn(9)+1])

						} else if p_name == "GORM" {
							Entry_num += FetchGORMByColumn(g, States[rand.Intn(9)+1])
						} else {
							Entry_num += FetchPgxByColumn(conn, States[rand.Intn(9)+1])
						}
					}

				}
				fmt.Println("The time taken to fetch ", j, " in ", time.Since(t1), "and fetched around ", Entry_num, " responses")

			}

		}

	}

	//fmt.Println(FetchNativeByColumn("Boston"))

	//fmt.Println(FetchGORMByColumn(g, States[rand.Intn(9)+1]))
	//fmt.Println(FetchPgxByColumn(conn, States[rand.Intn(9)+1]))
}
