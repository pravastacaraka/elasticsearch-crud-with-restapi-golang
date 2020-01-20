package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	"github.com/olivere/elastic"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
)

type User struct {
	FirstName string `json:"first_name"`
	LastName  string `json:"last_name"`
	BornDate  string `json:"ttl"`
}

const mapping = `
{
	"settings": {
		"number_of_shards": 3,
		"number_of_replicas": 1
	},
	"mappings": {
		"properties": {
			"first_name": {
				"type": "text",
				"fields": {
					"keyword": {
						"type": "keyword"
					}
				}
			},
			"last_name": {
				"type": "text",
				"fields": {
					"keyword": {
						"type": "keyword"
					}
				}
			},
			"ttl": {
				"type": "date"
			}
		}
	}
}`

var indexName string = "elastic_go"
var idIteration int

func restAPI() {
	r := mux.NewRouter()
	s := r.PathPrefix("/products").Subrouter()

	s.HandleFunc("/", getAllPosts).Methods("GET")
	s.HandleFunc("/", createPost).Methods("POST")
	s.HandleFunc("/_bulk", bulkPost).Methods("POST")
	s.HandleFunc("/_search", searchPost).Methods("GET")
	s.HandleFunc("/{id}", getPost).Methods("GET")
	s.HandleFunc("/{id}", updatePost).Methods("PUT")
	s.HandleFunc("/{id}", deletePost).Methods("DELETE")

	fmt.Println("API started...")
	_ = http.ListenAndServe(":8000", s)
}

func initElastic() *elastic.Client {
	client, err := elastic.NewClient(elastic.SetURL("http://localhost:9201"))
	if err != nil {
		panic(err)
	}

	return client
}

func getAllElasticData() map[string]interface{} {
	client := initElastic()
	searchResult := client.Scroll().
		Index(indexName).
		Size(1)

	var users []User
	var idUsers []string
	var userData User

	for {
		res, err := searchResult.Do(context.Background())
		if err == io.EOF {
			break
		}
		for _, hit := range res.Hits.Hits {
			_ = json.Unmarshal(hit.Source, &userData)
			users = append(users, userData)
			idUsers = append(idUsers, hit.Id)
		}
	}

	response := make(map[string]interface{})

	for i, data := range users {
		index := fmt.Sprint(idUsers[i])
		response[index] = data
	}

	return response
}

func createElasticData(data User, id string) map[string]interface{} {
	client := initElastic()
	putDoc, err := client.Index().
						Index(indexName).
						Id(id).
						BodyJson(data).
						Do(context.Background())

	response := make(map[string]interface{})

	if err != nil {
		response["status"] = false
		response["message"] = "failed"
	} else {
		response["status"] = true
		response["message"] = "success"
		response["id"] = putDoc.Id
	}

	return response
}

func bulkElasticData(data []User) map[string]interface{} {
	client := initElastic()
	bulkRequest := client.Bulk()

	var id string
	response := make(map[string]interface{})

	for _, doc := range data {
		idIteration++
		id = strconv.Itoa(idIteration)
		indexReq := elastic.NewBulkIndexRequest().Index(indexName).
												Id(id).
												Doc(doc)
		bulkRequest.Add(indexReq)
	}
	bulkResponse, err := bulkRequest.Do(context.Background())

	if err != nil {
		response["status"] = false
		response["message"] = "failed"
	} else {
		response["status"] = true
		response["message"] = "success"
		response["data"] = bulkResponse.Items
	}

	return response
}

func getElasticData(id string) map[string]interface{} {
	client := initElastic()
	getDoc, err := client.Get().
						Index(indexName).
						Id(id).
						Do(context.Background())

	response := make(map[string]interface{})

	if err != nil {
		response["status"] = false
		response["message"] = "data not found"
	} else {
		// Deserialize get.Source into User from json data
		var userData User
		_ = json.Unmarshal(getDoc.Source, &userData)

		response["status"] = true
		response["message"] = "success"
		response["id"] = getDoc.Id
		response["first_name"] = userData.FirstName
		response["last_name"] = userData.LastName
		response["ttl"] = userData.BornDate
	}

	return response
}

func updateElasticData(data User, id string) map[string]interface{} {
	client := initElastic()
	updateDoc, err := client.Update().
							Index(indexName).
							Id(id).
							Doc(data).
							Do(context.Background())

	response := make(map[string]interface{})

	if err != nil {
		response["status"] = false
		response["message"] = "failed"
	} else {
		response["status"] = true
		response["message"] = "updated"
		response["id"] = updateDoc.Id
	}

	return response
}

func deleteElasticData(id string) map[string]interface{} {
	client := initElastic()
	deleteDoc, err := client.Delete().
							Index(indexName).
							Id(id).
							Do(context.Background())

	response := make(map[string]interface{})

	if err != nil {
		response["status"] = false
		response["message"] = "failed"
	} else {
		response["status"] = true
		response["message"] = "deleted"
		response["id"] = deleteDoc.Id
	}

	return response
}

func searchElasticData(key string, value string) map[string]interface{} {
	client := initElastic()
	termQuery := elastic.NewTermQuery(key, value)
	searchResult := client.Scroll().
							Index(indexName).
							Query(termQuery).
							Size(1).
							Pretty(true)

	var getData []map[string]interface{}

	for {
		res, err := searchResult.Do(context.Background())
		if err == io.EOF {
			break
		}
		for _, hit := range res.Hits.Hits {
			var data map[string]interface{}
			_= json.Unmarshal(hit.Source, &data)
			data["_id"] = hit.Id
			getData = append(getData, data)
		}
	}

	response := make(map[string]interface{})
	response["data"] = getData
	return response
}

func getAllPosts(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(getAllElasticData())
}

func createPost(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	var post User
	var id string
	idIteration++
	_ = json.NewDecoder(r.Body).Decode(&post)
	id = strconv.Itoa(idIteration)
	_ = json.NewEncoder(w).Encode(createElasticData(post, id))
}

func bulkPost(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	var post User
	posts := []User{}

	dec := json.NewDecoder(r.Body)
	for  {
		err := dec.Decode(&post)
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}

		posts = append(posts, post)
	}

	_ = json.NewEncoder(w).Encode(bulkElasticData(posts))
}

func getPost(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	params := mux.Vars(r) // Get url parameter
	_ = json.NewEncoder(w).Encode(getElasticData(params["id"]))
}

func updatePost(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	params := mux.Vars(r)
	id := params["id"]
	getData := getElasticData(id)

	var post User
	_ = json.NewDecoder(r.Body).Decode(&post)

	if post.FirstName == "" {
		post.FirstName = fmt.Sprintf("%v", getData["first_name"])
	}
	if post.LastName == "" {
		post.LastName = fmt.Sprintf("%v", getData["last_name"])
	}
	if post.BornDate == "" {
		post.BornDate = fmt.Sprintf("%v", getData["ttl"])
	}

	_ = json.NewEncoder(w).Encode(updateElasticData(post, id))
}

func deletePost(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	params := mux.Vars(r)
	id := params["id"]

	_ = json.NewEncoder(w).Encode(deleteElasticData(id))
}

func searchPost(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	var(
		key string
		value string
	)

	v := r.URL.Query()
	query := v.Get("q")
	data := strings.Split(query, ":")

	key = data[0]
	value = data[1]

	//if firstName != "" {
	//	key = "first_name"
	//	value = firstName
	//}
	//if lastName != "" {
	//	key = "last_name"
	//	value = lastName
	//}
	//if ttl != "" {
	//	key = "ttl"
	//	value = ttl
	//}

	//fmt.Println(firstName + " | " + lastName + " | " + ttl)
	_ = json.NewEncoder(w).Encode(searchElasticData(key, value))
}

func main() {
	client := initElastic()

	// Check index is already exist or not
	exists, err:= client.IndexExists(indexName).Do(context.Background())
	if err != nil {
		panic(err)
	}
	if exists {
		fmt.Println("Index already exist!")
	} else {
		createIndex, err := client.CreateIndex(indexName).BodyString(mapping).Do(context.Background())
		if err != nil {
			panic(err)
		}
		if createIndex.Acknowledged {
			fmt.Println("Index created!")
		}
	}

	// API Connection
	restAPI()
}