package main

import (
	"fmt"
	"log"

	"github.com/go-pg/pg/v10"
	"github.com/go-pg/pg/v10/orm"
)

var db *pg.DB

// model for mapping table
type Mapping struct {
	EId uint
	MId uint
}

func init() {
	//connection to database
	db = pg.Connect(&pg.Options{

		User:     "app_user",
		Password: "app_password",
		Database: "app_database",
		Addr:     "localhost:5432",
	})
	if db == nil {
		log.Fatalln("Could not connect to the database ")
	}
	log.Println("Connection to DB successful")

	//Create tables
	err := createTables()
	if err != nil {
		log.Fatal(err)
	}
}
func createTables() error {
	err := db.Model((*Mapping)(nil)).CreateTable(&orm.CreateTableOptions{
		IfNotExists: true,
	})
	if err != nil {
		return err
	}
	log.Println("Tables succesffuly created ")
	return nil
}
func fetchDetails() ([]Mapping,error) {
	var data []Mapping
	if selErr := db.Model(&data).Select(); selErr != nil {
		return nil, selErr
	}
	return data, nil
}

func isCyclic(eid uint, visited map[uint]bool ,pathVisited map[uint]bool, adjList map[uint][]uint ) bool {
	visited[eid] = true
	pathVisited[eid] = true

	//traverse for adjacent nodes
	for _,mId := range adjList[eid] {
		
		if !visited[mId] {

			if isCyclic(mId,visited,pathVisited,adjList) {
				return true
			}

		}else if pathVisited[mId] {
			return true
		}
	}

	pathVisited[eid] = false
	return false
}

func upsertRecord(eid,mid uint) error {
	
	record := Mapping{
		EId: eid,
		MId: mid,
	}
	exists, err := db.Model(&record).Where("e_id=?",eid).Exists()
    if err != nil {
        return err
    }
	// If the record exists, update it. Otherwise, insert it.
    if exists {
        if _, err = db.Model(&Mapping{}).Set("m_id=?", mid).Where("e_id=?", eid).Update(); err != nil {
			return err
		}
		fmt.Println("Record updated successfully!")
		
    } else {
        if _, err = db.Model(&record).Insert(); err != nil{
			return err
		}
		fmt.Println("Record inserted successfully!")
    }
    return nil
}

func main() {
	fmt.Println("In main func")
	data, err := fetchDetails()
	if err != nil {
		log.Fatal(err)
	}

	//create the graph from this data
	adjList := make(map[uint][]uint)
	for _, item := range data {
		adjList[item.EId] = append(adjList[item.EId], item.MId)
	}

	fmt.Println("Graph is ", adjList)

	var eid, mid uint
	fmt.Println("Enter the EId to update/insert")
	fmt.Scanln(&eid)
	fmt.Println("Enter the MId to update/insert")
	fmt.Scanln(&mid)

	//add it to adjList
	adjList[eid] = append(adjList[eid], mid)

	visited := make(map[uint]bool)
	pathVisited := make(map[uint]bool)

	//check for cycle
	if isCyclic(eid,visited,pathVisited, adjList) {
		fmt.Println("Ids cannot be updated as it will lead to a cycle")
	}else{
		//perform updation
		if err := upsertRecord(eid,mid); err != nil{
			fmt.Println("Failed to update/insert record in database, error: ",err)
		}
	}
}



