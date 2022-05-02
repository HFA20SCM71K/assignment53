package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	//	"io/ioutil"
	//	"net/http"
	//	"strconv"
	"time"

	"database/sql"
	//	"encoding/json"

	//	"github.com/kelvins/geocoder"
	_ "github.com/lib/pq"
)

type TaxiTripsJsonRecords []struct {
	Trip_id              string `json:"trip_id"`
	Trip_start_timestamp string `json:"trip_start_timestamp"`
	Trip_end_timestamp   string `json:"trip_end_timestamp"`
	//	Pickup_centroid_latitude   string `json:"pickup_centroid_latitude"`
	//	Pickup_centroid_longitude  string `json:"pickup_centroid_longitude"`
	//	Dropoff_centroid_latitude  string `json:"dropoff_centroid_latitude"`
	//	Dropoff_centroid_longitude string `json:"dropoff_centroid_longitude"`
}

type BuildingPermitsJsonRecords []struct {
	Permit_id           string `json:"id"`
	Permit_number       string `json:"permit_"`
	Permit_type         string `json:"permit_type"`
	Issue_date          string `json:"issue_date"`
	Work_Description    string `json:"work_description"`
	Subtotal_paid       string `json:"subtotal_paid"`
	Building_fee_unpaid string `json:"building_fee_unpaid"`
	Zoning_fee_unpaid   string `json:"zoning_fee_unpaid"`
	Reported_cost       string `json:"reported_cost"`
}

type CovidCasesJsonRecords []struct {
	Zipcode             string `json:"zip_code"`
	Week_number         string `json:"week_number"`
	Week_Start          string `json:"week_start"`
	Week_end            string `json:"week_end"`
	Cases_weekly        string `json:"cases_weekly"`
	CaseRate_Cumulative string `json:"case_rate_cumulative"`
	Tests_weekly        string `json:"tests_weekly"`
	TestRate_Cumulative string `json:"test_rate_cumulative"`
	Percent_positive    string `json:"percent_tested_positive_weekly"`
	Deaths_weekly       string `json:"deaths_weekly"`
	Population          string `json:"population"`
}

type UnemploymentJsonRecords []struct {
	Community_area     string `json:"community_area"`
	Community_areaname string `json:"community_area_name"`
	Unemployment       string `json:"unemployment"`
	Firearm_related    string `json:"firearm_related"`
	Below_povertylevel string `json:"below_poverty_level"`
}

func main() {
	// Establish connection to Postgres Database
	//db_connection := "user=postgres dbname=chicagoTest password=root host=localhost sslmode=disable"
	db_connection := "user=postgres dbname=chicago_business_intelligence password=root host=/cloudsql/chicagobusiness-intelligence:us-central1:mypostgres sslmode=disable port = 5432"

	db, err := sql.Open("postgres", db_connection)
	if err != nil {
		panic(err)
	}

	// Test the database connection
	err = db.Ping()
	if err != nil {
		fmt.Println("Couldn't Connect to database")
		panic(err)
	}

	days := 0

	for{

		time.Sleep(24 * time.Hour)
		days++
		GetTaxiTrips(db)
		GetCOVIDStats(db)

		if days == 7 {
			GetBuildingPermits(db)
			GetUnemploymentStats(db)
			days = 1
		}
	}
	

}

func GetUnemploymentStats(db *sql.DB) {
	fmt.Println("GetUnemploymentStats: Collecting Unemployment Data")

	drop_phrase := `drop table if exists unemployment_stats`
	_, err := db.Exec(drop_phrase)
	if err != nil {
		panic(err)
	}

	create_phrase := `CREATE TABLE IF NOT EXISTS "unemployment_stats" (
		"community_area"		INTEGER,
		"community_ara_name"	VARCHAR(255),
		"unemployment"			NUMERIC,
		"firearm_related" 		NUMERIC,
		"below_poverty_level"	NUMERIC
		);`

	_, _err := db.Exec(create_phrase)
	if err != nil {
		panic(_err)
	}

	var url = "https://data.cityofchicago.org/resource/iqnk-2tcu.json?$limit=500"

	res, err := http.Get(url)
	if err != nil {
		panic(err)
	}

	body, _ := ioutil.ReadAll(res.Body)
	var unemploymentData UnemploymentJsonRecords
	json.Unmarshal(body, &unemploymentData)

	for i := 0; i < len(unemploymentData); i++ {

		//any record with missing fields, we will omit it from the result
		//	trip_id := taxi_trips_list
		community_area := unemploymentData[i].Community_area
		if community_area == "" {
			continue
		}

		community_area_name := unemploymentData[i].Community_areaname
		if community_area_name == "" {
			continue
		}

		unemployment := unemploymentData[i].Unemployment
		if unemployment == "" {
			continue
		}

		firearm_related := unemploymentData[i].Firearm_related

		below_povertylevel := unemploymentData[i].Below_povertylevel

		sql := `INSERT INTO unemployment_stats values ($1, $2, $3, $4, $5)`

		_, err = db.Exec(sql, community_area, community_area_name, unemployment, firearm_related, below_povertylevel)

		if err != nil {
			panic(err)
		}

	}

	fmt.Println("onichan! Help! I am scared xD")

}

func GetTaxiTrips(db *sql.DB) {

	fmt.Println("GetTaxiTrips: Collecting Taxi Trips Data")

	drop_phrase := `drop table if exists taxi_trips`
	_, err := db.Exec(drop_phrase)
	if err != nil {
		panic(err)
	}

	create_phrase := `CREATE TABLE IF NOT EXISTS "taxi_trips" (
		"row_id"	SERIAL,
		"trip_id"	VARCHAR(255) UNIQUE,
		"trip_start_timestamp"	TIMESTAMP WITH TIME ZONE,
		"trip_end_timestamp" TIMESTAMP WITH TIME ZONE 
		);`

	_, _err := db.Exec(create_phrase)
	if err != nil {
		panic(_err)
	}

	var url = "https://data.cityofchicago.org/resource/wrvz-psew.json?$limit=500"

	res, err := http.Get(url)
	if err != nil {
		panic(err)
	}

	body, _ := ioutil.ReadAll(res.Body)
	var taxi_trips_list TaxiTripsJsonRecords
	json.Unmarshal(body, &taxi_trips_list)

	for i := 0; i < len(taxi_trips_list); i++ {

		//any record with missing fields, we will omit it from the result
		//	trip_id := taxi_trips_list
		trip_id := taxi_trips_list[i].Trip_id
		if trip_id == "" {
			continue
		}

		trip_start_timestamp := taxi_trips_list[i].Trip_start_timestamp
		if len(trip_start_timestamp) < 23 {
			continue
		}

		trip_end_timestamp := taxi_trips_list[i].Trip_end_timestamp
		if len(trip_end_timestamp) < 23 {
			continue
		}

		sql := `INSERT INTO taxi_trips ("trip_id", "trip_start_timestamp", "trip_end_timestamp") values ($1, $2, $3) `

		_, err = db.Exec(sql, trip_id, trip_start_timestamp, trip_end_timestamp)

		if err != nil {
			panic(err)
		}

	}

	fmt.Println("onichan! Help! I am scared xD")
}

func GetBuildingPermits(db *sql.DB) {

	fmt.Println("GetBuildingPermits: Collecting Building Permit Data")

	drop_table := `drop table if exists building_permits`
	_, err := db.Exec(drop_table)
	if err != nil {
		panic(err)
	}

	create_table := `CREATE TABLE IF NOT EXISTS "building_permits" (
					"id"	SERIAL,
					"PERMIT_NO"   VARCHAR(255),
					"PERMIT_TYPE" VARCHAR(255),
					"ISSUE_DATE"  TIMESTAMP WITH TIME ZONE,
					"WORK_DESCRIPTION" TEXT,
					"SUBTOTAL_PAID" NUMERIC,
					"BUILDING_FEE_UNPAID" NUMERIC,
					"ZONING_FEE_UNPAID" NUMERIC,
					"REPORTED_COST" NUMERIC,
					PRIMARY KEY("id") 
					);`

	_, _err := db.Exec(create_table)
	if _err != nil {
		panic(_err)
	}

	var url = "https://data.cityofchicago.org/resource/ydr8-5enu.json?$limit=500"

	res, err := http.Get(url)
	if err != nil {
		panic(err)
	}

	body, _ := ioutil.ReadAll(res.Body)
	var building_permits_list BuildingPermitsJsonRecords
	json.Unmarshal(body, &building_permits_list)

	for i := 0; i < len(building_permits_list); i++ {

		my_permit_id := building_permits_list[i].Permit_id
		if my_permit_id == "" {
			continue
		}

		my_permit_no := building_permits_list[i].Permit_number
		if my_permit_no == "" {
			continue
		}

		my_permit_type := building_permits_list[i].Permit_type
		if my_permit_type == "" {
			continue
		}

		my_issue_date := building_permits_list[i].Issue_date
		if my_issue_date == "" {
			continue
		}

		my_work_description := building_permits_list[i].Work_Description
		if my_work_description == "" {
			continue
		}

		my_subtotal_paid := building_permits_list[i].Subtotal_paid
		//cant do defensive programming on floats

		my_building_fee_unpaid := building_permits_list[i].Building_fee_unpaid

		my_zoning_fee_unpaid := building_permits_list[i].Zoning_fee_unpaid

		my_reported_cost := building_permits_list[i].Reported_cost

		sql := `INSERT INTO building_permits ("id", "PERMIT_NO", "PERMIT_TYPE", "ISSUE_DATE", "WORK_DESCRIPTION", "SUBTOTAL_PAID", "BUILDING_FEE_UNPAID", "ZONING_FEE_UNPAID", "REPORTED_COST")
				values ($1, $2, $3, $4, $5, $6, $7, $8, $9)`

		_, err = db.Exec(sql, my_permit_id, my_permit_no, my_permit_type, my_issue_date, my_work_description, my_subtotal_paid, my_building_fee_unpaid, my_zoning_fee_unpaid, my_reported_cost)

		if err != nil {
			panic(err)
		}

	}
	fmt.Println("onichan! Help! I am scared xD")
}

func GetCOVIDStats(db *sql.DB) {

	fmt.Println("GetCOVIDStats: Collecting COVID Stats Data")

	drop_table := `drop table if exists covid_stats`
	_, err := db.Exec(drop_table)
	if err != nil {
		panic(err)
	}

	// create_table := `CREATE TABLE IF NOT EXISTS "covid_stats" (
	// 				"zipcode"		VARCHAR(255),
	// 				"week_number" 	integer,
	// 				"week_start"	TIMESTAMP WITH TIME ZONE,
	// 				"week_end"		TIMESTAMP WITH TIME ZONE,
	// 				"cases_weekly"	integer,
	// 				"case_rate_cumulative"	NUMERIC,
	// 				"tests_weekly"	integer,
	// 				"test_rate_cumulative"	NUMERIC,
	// 				"percent_positive"	NUMERIC,
	// 				"deaths_weekly"	integer,
	// 				"population"	integer
	// 				);`

	create_table := `CREATE TABLE IF NOT EXISTS "covid_stats" (
		"zipcode"		VARCHAR(255),
		"week_number" 	VARCHAR(255),
		"week_start"	VARCHAR(255),
		"week_end"		VARCHAR(255),
		"cases_weekly"	VARCHAR(255),
		"case_rate_cumulative"	VARCHAR(255),
		"tests_weekly"	VARCHAR(255),
		"test_rate_cumulative"	VARCHAR(255),
		"percent_positive"	VARCHAR(255),
		"deaths_weekly"	VARCHAR(255),
		"population"	VARCHAR(255)
		);`

	_, _err := db.Exec(create_table)
	if _err != nil {
		panic(_err)
	}

	var url = "https://data.cityofchicago.org/resource/yhhz-zm2v.json?$limit=500"

	res, err := http.Get(url)
	if err != nil {
		panic(err)
	}

	body, _ := ioutil.ReadAll(res.Body)
	var covidcases CovidCasesJsonRecords
	json.Unmarshal(body, &covidcases)

	for i := 0; i < len(covidcases); i++ {
		my_zipcode := covidcases[i].Zipcode
		//if my_zipcode == "" {continue}
		my_weeknumber := covidcases[i].Week_number
		my_weekstart := covidcases[i].Week_Start
		my_weekend := covidcases[i].Week_end
		my_caseweekly := covidcases[i].Cases_weekly
		my_cumcases := covidcases[i].CaseRate_Cumulative
		my_testweekly := covidcases[i].Tests_weekly
		my_cumtestrate := covidcases[i].TestRate_Cumulative
		my_percentpos := covidcases[i].Percent_positive
		my_deaths := covidcases[i].Deaths_weekly
		my_population := covidcases[i].Population

		sql := `INSERT INTO covid_stats values($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`

		_, err = db.Exec(
			sql,
			my_zipcode,
			my_weeknumber,
			my_weekstart,
			my_weekend,
			my_caseweekly,
			my_cumcases,
			my_testweekly,
			my_cumtestrate,
			my_percentpos,
			my_deaths,
			my_population)

		if err != nil {
			panic(err)
		}

	}

	fmt.Println("onichan! help! i am scared!")

}
