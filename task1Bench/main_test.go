package main

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/jackc/pgx/v4"
	"github.com/jinzhu/gorm"
)

func Benchmark(b *testing.B) {
	Setup()
	defer Cleanup()
	var NumDat uint64
	g, _ := gorm.Open("postgres", db)

	limits := []int{
		5,
		50,
		500,
		1000,
	}

	fmt.Println("Insertion Tests")
	for _, lim := range limits {
		lim := lim

		// Benchmark NativeSql
		b.Run(fmt.Sprintf("native SQL limit:%d", lim), func(b *testing.B) {
			for i := 0; i < b.N; {
				for k := 0; k < lim; k++ {
					InsertionNative(db, GenData[NumDat])
					NumDat += 1
				}
			}
		})

		// Benchmark GORM
		b.Run(fmt.Sprintf("GORM SQL limit:%d", lim), func(b *testing.B) {
			g, _ := gorm.Open("postgres", db)

			for i := 0; i < b.N; i++ {
				for k := 0; k < lim; k++ {
					InsertionGORM(g, GenData[NumDat])
					NumDat += 1
				}
			}
		})

		// Benchmark PGX
		b.Run(fmt.Sprintf("PGX SQL limit:%d", lim), func(b *testing.B) {
			conn, _ := pgx.Connect(context.Background(), "postgres://postgres:postgres@localhost:5432/benchDB")

			for i := 0; i < b.N; i++ {
				for k := 0; k < lim; k++ {
					InsertionPgx(conn, GenData[NumDat])
					NumDat += 1
				}
			}
		})

		fmt.Println("========================================================================")
	}

	//Fetch Benchmarks

	for _, lim := range limits {
		lim := lim

		// Benchmark NativeSql
		b.Run(fmt.Sprintf("native SQL limit:%d", lim), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				for k := 1; k <= lim; k++ {
					FetchNative(db, k)

				}
			}
		})

		// Benchmark GORM
		b.Run(fmt.Sprintf("GORM SQL limit:%d", lim), func(b *testing.B) {

			for i := 0; i < b.N; i++ {
				for k := 1; k <= lim; k++ {
					FetchGORM(g, k)

				}
			}
		})

		// Benchmark PGX
		b.Run(fmt.Sprintf("PGX SQL limit:%d", lim), func(b *testing.B) {
			conn, _ := pgx.Connect(context.Background(), "postgres://postgres:postgres@localhost:5432/benchDB")

			for i := 0; i < b.N; i++ {
				for k := 1; k <= lim; k++ {
					FetchPgx(conn, k)

				}
			}
		})

		fmt.Println("========================================================================")

	}
}

func Benchmark_NativeConcurrent(b *testing.B) {
	Setup()
	//defer Cleanup()
	var wg sync.WaitGroup
	var NumData uint64 = 1
	for i := 0; i < b.N; {

		for k := 0; k < 10; k++ {
			i++
			wg.Add(1)
			go ConInsertionNative(db, GenData[NumData], wg)
			NumData += 1

		}
		wg.Wait()

	}
}
