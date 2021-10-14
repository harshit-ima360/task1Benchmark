package main

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v4/pgxpool"
)

func InsertionPgxPool(conn *pgxpool.Pool, dat model) {
	//defer wg.Done()
	if _, err := conn.Exec(context.Background(), fmt.Sprintf("INSERT INTO test values('%d', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s') ON CONFLICT DO NOTHING;", dat.ID, dat.Name, dat.CarMaker, dat.Gender, dat.SSN, dat.Email, dat.Address, dat.Phone, dat.Phone2, dat.CreditCardNum, dat.JobTitle, dat.Level, dat.Company, dat.FatherName, dat.MotherName, dat.Street, dat.StreetName, dat.City, dat.State, dat.Country, dat.Zip)); err != nil {
		fmt.Println("Unable to insert Data", err)
		return
	}

}

func FetchPgxPool(conn *pgxpool.Pool, id int) model {
	tempdat := model{}

	conn.QueryRow(context.Background(), "select * from test WHERE id=$1;", id).Scan(&tempdat)
	return tempdat
}
