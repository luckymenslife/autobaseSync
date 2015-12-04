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
	"errors"
	"strconv"
)
type AutomapInstance struct{
	ac AutomapConf
	at AutomapToken
}
type AutobaseOrg struct{
	Gid int
	Name string
	Address string
	Stamp string
	Phone string
	Sync bool
	External_id int
}

type AutomapConf struct {
	url string
	login string
	password string
}

type AutomapAuth struct{
	Login string
	Password string
}

type AutomapToken struct{
	Token string
	RefreshToken string
	Ttl string
	updateTime time.Time
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
		abOrg.External_id = -1
		err := json.Unmarshal([]byte(data),&abOrg)
		checkErr(err)
		fmt.Println(data)
		fmt.Println(abOrg.Gid)
		fmt.Println(abOrg.Name)
		fmt.Println(abOrg.Address)
		fmt.Println(abOrg.Stamp)
		fmt.Println(abOrg.Phone)
		fmt.Println(abOrg.Sync)
		fmt.Println(abOrg.External_id)
	}

	automapInst := AutomapInstance{
			AutomapConf{"http://mt.asuds77.ru/","ilgiz","anakonda"},
			AutomapToken{"","","",time.Now()}}

	token,err := automapInst.getToken(false)
	fmt.Println(token)


}

func checkErr(err error){
	if err!=nil{
		panic(err)
	}
}
/*
Выполнения POST-запроса. req - URI запроса, reqObj - структура (объект), передаваемая серверу при запросе.
respObj - структура ответа сервера. useToken - флаг, указывающий, прикреплять ли к URI параметр с токеном.
reqObj трансформируется в JSON и отсылается на веб.
Ответ парсится из JSONа и запихивается в respObj
 */
func (ai AutomapInstance) doPost(req string,reqObj interface{},respObj interface{},useToken bool,params string) (int,error){
	reqObjJson, err := json.Marshal(reqObj)
	checkErr(err)
	fullURL := ai.ac.url+req
	if useToken{
		fullURL += "?token="+ai.at.Token
		if len(params)>0 {
			fullURL += "&"+params
		}
	}else {
		if len(params)>0 {
			fullURL += "?"+params
		}
	}
	//request, err := http.NewRequest("POST", fullURL, bytes.NewBuffer(reqObjJson))
	//request.Header.Set("Content-Type", "application/json")
	client := &http.Client{}
	fmt.Println(fullURL)
	fmt.Println(string(reqObjJson))
	response, err := client.Post(fullURL,"application/json",bytes.NewBuffer(reqObjJson))
	checkErr(err)
	defer response.Body.Close()
	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return 0,err
	}
	if response.StatusCode != 200 {
		return response.StatusCode,errors.New(string(body))
	}else {
		err = json.Unmarshal(body,respObj)
		checkErr(err)
		return 200,nil
	}
}

/*
Выполнения GET-запроса. req - URI запроса, respObj - структура ответа сервера.
useToken - флаг, указывающий, прикреплять ли к URI параметр с токеном.
params - строка параметров для запроса (Ex. firstname=Ilgiz&lastname=Sadykov)
Ответ парсится из JSONа и запихивается в respObj
 */
func (ai AutomapInstance) doGet(req string,params string,respObj interface{},useToken bool) (int,error){
	fullURL := ai.ac.url+req
	if useToken{
		fullURL += "?token="+ai.at.Token
		if len(params)>0 {
			fullURL += "&"+params
		}
	}else {
		if len(params)>0 {
			fullURL += "?"+params
		}
	}
	client := &http.Client{}
	response, err := client.Get(fullURL)
	checkErr(err)
	defer response.Body.Close()
	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return 0,err
	}
	if response.StatusCode != 200 {
		return response.StatusCode,errors.New(string(body))
	}else {
		err = json.Unmarshal(body,respObj)
		checkErr(err)
		return 200,nil
	}
}


/*
Возвращение токена аутентификации.
Если указан флаг force, то будет заново произведена аутентификация с помощью
логина и пароля.
В случае, если токен пустой, то это аналогично использованию флага force.
Если срок действия токена вышел, то происходит процедура обновления токена.
 */
func (ai AutomapInstance) getToken(force bool) (string,error){
	if force {
		aAuth := AutomapAuth{ai.ac.login,ai.ac.password}
		var respToken AutomapToken
		statusCode,err := ai.doPost("/token/auth",aAuth,&respToken,false,"")
		fmt.Println(statusCode)
		checkErr(err)
		if statusCode == 200 {
			ai.at = respToken
			ai.at.updateTime = time.Now()
			fmt.Println("Authorized.")
			return respToken.Token,nil
		}
		return "",err
	}else {
		curTime := time.Now()
		ttl, _ := strconv.Atoi(ai.at.Ttl)
		if curTime.Before(ai.at.updateTime.Add(time.Duration(ttl) * time.Second))||len(ai.at.Token)<2 {
			if len(ai.at.Token)>1 {
				var respToken AutomapToken
				statusCode, err := ai.doGet("/token/refresh", "refreshToken=" + ai.at.RefreshToken,&respToken, false)
				if statusCode == 200 {
					ai.at = respToken
					ai.at.updateTime = time.Now()
					fmt.Println("Token refreshed.")
					return respToken.Token, nil
				}
				if statusCode != 404 {
					return "", err
				}
			}
			return ai.getToken(true)
		}else {
			return ai.at.Token, nil
		}
	}
}