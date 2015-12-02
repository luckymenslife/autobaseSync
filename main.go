package main

import (
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	"time"
	"encoding/json"
	"net/http"
	"bytes"
	"io/ioutil"
)

type AutobaseOrg struct{
	Gid int64
	Name string
	Address string
	Stamp string
	Phone string
	Sync bool
}

func main(){
	dbhost := "gbu.asuds77.ru"
	dbport := "5432"
	dbname := "inf_asuds_copy"
	dbuser := "ilgiz"
	dbpass := "ctrhtnysq!rjl"

	dbconn := fmt.Sprintf("host=%s port=%s dbname=%s user=%s password=%s sslmode=disable",dbhost,dbport,dbname,dbuser,dbpass)
	//fmt.Println(dbconn)
	db,err := sql.Open("postgres",dbconn)
	defer db.Close()

	fmt.Println("# Querying")
	rows, err := db.Query("SELECT id,object_type, change_type, change_date, data FROM changes")
	checkErr(err)

	for rows.Next() {
		var id int
		var objectType string
		var changeType string
		var changeDate time.Time
		var data string
		err = rows.Scan(&id, &objectType, &changeType, &changeDate,&data)
		checkErr(err)
		fmt.Printf("%d",id, objectType, changeType, changeDate, data)
		var abOrg AutobaseOrg
		err := json.Unmarshal([]byte(data),&abOrg)
		checkErr(err)
		fmt.Println(data)
		fmt.Println(abOrg.Gid)
		fmt.Println(abOrg.Name)
		fmt.Println(abOrg.Address)
		fmt.Println(abOrg.Stamp)
		fmt.Println(abOrg.Phone)
		fmt.Println(abOrg.Sync)
	}

	automapURL := "http://mt.asuds77.ru/token/auth"
	automapUser := "ilgiz"
	automapPassword := "anakonda"
	jsonStr:=[]byte(fmt.Sprintf("{\"login\":\"%s\",\"password\":\"%s\"}",automapUser,automapPassword))
	req, err := http.NewRequest("POST", automapURL, bytes.NewBuffer(jsonStr))
	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	fmt.Println("response Status:", resp.Status)
	fmt.Println("response Headers:", resp.Header)
	body, _ := ioutil.ReadAll(resp.Body)
	fmt.Println("response Body:", string(body))
}

func checkErr(err error){
	if err!=nil{
		panic(err)
	}
}