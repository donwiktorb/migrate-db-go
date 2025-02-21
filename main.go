package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

type OldUser struct {
	Identifier  string  `json:"identifier"`
	Money       int     `json:"money"`
	Name        *string `json:"name"`
	Skin        *string `json:"skin"`
	Loadout     string  `json:"loadout"`
	Position    *string `json:"position"`
	Bank        int     `json:"bank"`
	Inventory   string  `json:"inventory"`
	Firstname   string  `json:"firstname"`
	Lastname    string  `json:"lastname"`
	Dateofbirth string  `json:"dateofbirth"`
	Sex         string  `json:"sex"`
	Height      *int    `json:"height"`
}

type LoadoutItemData struct {
	Ammo int `json:"ammo"`
}

type InventoryItem struct {
	Name  string           `json:"name"`
	Count int              `json:"count"`
	Data  *LoadoutItemData `json:"data"`
}

type UserInventory struct {
	Items    map[string]int  `json:"items"`
	AltItems []InventoryItem `json:"alt_items"`
}

type LoadoutItem struct {
	Name  string          `json:"name"`
	Count int             `json:"count"`
	Data  LoadoutItemData `json:"data"`
}

type LoadoutItemOld struct {
	Name string `json:"name"`
	Ammo int    `json:"ammo"`
}

type UserLoadout struct {
	Items []LoadoutItemOld `json:"items"`
}

type SkinData map[string]interface{}

type PropertyDress struct {
	Label *string   `json:"label"`
	Skin  *SkinData `json:"skin"`
}

type PropertyData struct {
	Dressing []PropertyDress `json:"dressing"`
	Weapons  []InventoryItem `json:"weapons"`
	Items    []InventoryItem `json:"items"`
}

type VehicleData map[string]interface{}

func getUsers(db *sql.DB) *sql.Rows {
	results, err := db.Query("SELECT identifier, money, name, skin, loadout, position, bank, inventory, firstname, lastname, dateofbirth, sex, height FROM users")
	if err != nil {
		panic(err.Error()) // proper error handling instead of panic in your app
	}

	return results
}

func processInventory(jsonInventory string) []InventoryItem {
	var jsonArray []InventoryItem
	var userInventory UserInventory

	// Try unmarshaling as map first
	err := json.Unmarshal([]byte(jsonInventory), &userInventory.Items)
	if err == nil && len(userInventory.Items) > 0 {
		// The inventory is in map format
		for itemName, itemCount := range userInventory.Items {
			jsonArray = append(jsonArray, InventoryItem{Name: itemName, Count: itemCount})
			// fmt.Printf("Item: %s, Count: %d\n", itemName, itemCount)
		}

		return jsonArray
	}

	userInventory.Items = nil

	err = json.Unmarshal([]byte(jsonInventory), &userInventory.AltItems)
	if err == nil && len(userInventory.AltItems) > 0 {
		// for _, altItem := range userInventory.AltItems {
		// 	jsonArray = append(jsonArray, InventoryItem{Name: altItem.Name, Count: altItem.Count})
		// }

		// return jsonArray
		return userInventory.AltItems
	}

	log.Printf("Error parsing inventory")

	return jsonArray
}

func processLoadout(jsonArray []InventoryItem, loadout string) []InventoryItem {
	var userLoadout UserLoadout

	err := json.Unmarshal([]byte(loadout), &userLoadout.Items)

	if err == nil && len(userLoadout.Items) > 0 {
		for _, altItem := range userLoadout.Items {
			jsonArray = append(jsonArray, InventoryItem{Name: altItem.Name, Count: 1, Data: &LoadoutItemData{Ammo: altItem.Ammo}})
		}

		return jsonArray
	}

	log.Printf("Error parsing loadout")
	return jsonArray
}

func handleBlackMoney(db *sql.DB, identifier string) int {
	var money int

	err := db.QueryRow("SELECT money FROM addon_account_data where owner = ?", identifier).Scan(&money)
	if err != nil {
		return 0
	}

	return money
}

func handleVeh(db_old *sql.DB, stmt *sql.Stmt, identifier string, owner int64) {
	results, err := db_old.Query("SELECT vehicle FROM owned_vehicles WHERE owner = ?", identifier)
	if err != nil {
		panic(err.Error()) // proper error handling instead of panic in your app
	}

	defer results.Close()

	for results.Next() {
		var vehicle string

		err = results.Scan(&vehicle)
		if err != nil {
			log.Printf("Err: %s", err.Error())
		}

		_, err = stmt.Exec(owner, vehicle)
		if err != nil {
			log.Printf("Err: %s", err.Error())
		}
	}
}

func handleProp(db_old *sql.DB, stmt *sql.Stmt, identifier string, owner int64) {
	var property_data PropertyData
	var data string

	err := db_old.QueryRow("SELECT data FROM datastore_data where owner = ?", identifier).Scan(&data)
	if err != nil {
	}

	err = json.Unmarshal([]byte(data), &property_data)
	if err != nil {
	}

	jsonClothes, err := json.Marshal(property_data.Dressing)
	if err != nil {
	}

	results, err := db_old.Query("SELECT name, count FROM addon_inventory_items WHERE owner = ?", identifier)
	if err != nil {
		panic(err.Error()) // proper error handling instead of panic in your app
	}

	defer results.Close()

	for results.Next() {
		var name string
		var count int
		err = results.Scan(&name, &count)
		if err != nil {
			log.Printf("Err: %s", err.Error())
		}

		property_data.Weapons = append(property_data.Weapons, InventoryItem{Name: name, Count: count})
	}

	jsonItems, err := json.Marshal(property_data.Weapons)
	if err != nil {
		log.Printf("Error json %s", err.Error())
	}

	if string(jsonItems) != "null" || string(jsonClothes) != "null" {
		// _, err = db_new.Exec("INSERT INTO owned_properties (owner, items, clothes) VALUES (?, ?, ?)", owner, string(jsonItems), string(jsonClothes))
		_, err := stmt.Exec(owner, string(jsonItems), string(jsonClothes))
		// if there is an error inserting, handle it
		if err != nil {
			panic(err.Error())
		}
		// return string(jsonClothes), string(jsonItems)
	}
}

type CharData struct {
	Firstname   string `json:"firstname"`
	Lastname    string `json:"lastname"`
	Dateofbirth string `json:"dateofbirth"`
	Sex         string `json:"sex"`
	Height      *int   `json:"height"`
}

func createCharData(old_user OldUser) CharData {
	return CharData{old_user.Firstname, old_user.Lastname, old_user.Dateofbirth, old_user.Sex, old_user.Height}
}

func insertCharacter(stmt *sql.Stmt, data string, skin *string, inventory string, position *string) int64 {
	insert, err := stmt.Exec(data, skin, inventory, position)
	// insert, err := db.Exec("INSERT INTO characters (data, skin, inventory, position) VALUES (?, ?, ?, ?)", data, skin, inventory, position)
	// if there is an error inserting, handle it
	if err != nil {
		panic(err.Error())
	}

	id, err := insert.LastInsertId()
	if err != nil {
		panic(err.Error())
	}

	return id
}

type Char struct {
	Id        int64  `json:"id"`
	Firstname string `json:"firstname"`
	Lastname  string `json:"lastname"`
}

type UserData struct {
	Chars []Char `json:"chars"`
	Char  int64  `json:"char"`
}

func createUserData(charId int64, firstname string, lastname string) UserData {
	return UserData{Chars: []Char{{charId, firstname, lastname}}, Char: charId}
}

func insertUser(stmt *sql.Stmt, identifier string, name *string, data string) {
	_, err := stmt.Exec(identifier, name, data)
	// _, err := db.Exec("INSERT INTO users (identifier, name, data) VALUES (?, ?, ?)", identifier, name, data)
	// if there is an error inserting, handle it
	if err != nil {
		panic(err.Error())
	}
}

func insertBank(stmt *sql.Stmt, charId int64, bank int) {
	_, err := stmt.Exec(charId, bank)
	// _, err := db.Exec("INSERT INTO bank_accounts (bankId, balance) VALUES (?, ?)", charId, bank)
	// if there is an error inserting, handle it
	if err != nil {
		panic(err.Error())
	}
}

func processUser(old_user OldUser, db_old *sql.DB, db_new *sql.DB, stmt *sql.Stmt, char_stmt *sql.Stmt, stmt2 *sql.Stmt, stmt4 *sql.Stmt, stmt5 *sql.Stmt, wg *sync.WaitGroup) {
	// tx, err := db_new.Begin()

	// if err != nil {
	// 	log.Printf("T err %s", err.Error())
	// 	defer wg.Done()
	// 	return
	// }

	inv := processInventory(old_user.Inventory)

	loadout := processLoadout(inv, old_user.Loadout)

	black_money := handleBlackMoney(db_old, old_user.Identifier)

	if black_money > 0 {
		loadout = append(loadout, InventoryItem{Name: "black_money", Count: black_money})
		log.Printf("%s %s %s %d", old_user.Identifier, old_user.Inventory, old_user.Loadout, black_money)
	}

	var inventory []byte

	if loadout != nil {
		// log.Printf("No loadout")

		// log.Printf("%s %s %s %d", old_user.Identifier, old_user.Inventory, old_user.Loadout, black_money)

		jsonInventory, err := json.Marshal(loadout)
		if err != nil {
			log.Printf("Error marshall inventory")
		}

		// log.Printf("%s", jsonInventory)

		inventory = jsonInventory
	}

	charData := createCharData(old_user)

	jsonCharData, err := json.Marshal(charData)
	if err != nil {
		// log.Printf("Error char data")
		panic("Error char data")
	}

	charId := insertCharacter(char_stmt, string(jsonCharData), old_user.Skin, string(inventory), old_user.Position)

	userData := createUserData(charId, old_user.Firstname, old_user.Lastname)

	jsonUserData, err := json.Marshal(userData)
	if err != nil {
		// log.Printf("Error char data")
		panic("Error char data")
	}

	insertUser(stmt, old_user.Identifier, old_user.Name, string(jsonUserData))

	insertBank(stmt2, charId, old_user.Bank)

	handleProp(db_old, stmt4, old_user.Identifier, charId)

	handleVeh(db_old, stmt5, old_user.Identifier, charId)

	// err = tx.Commit()

	// if err != nil {
	// 	log.Printf("T err %s", err.Error())
	// }

	defer wg.Done()
}

func initDb() {
	db_old, err := sql.Open("mysql", "username:password@tcp(IP:PORT)/DB_OLD")
	db_new, err2 := sql.Open("mysql", "username:password@tcp(IP:PORT)/DB_NEW")

	// if there is an error opening the connection, handle it
	if err != nil {
		panic(err.Error())
	}

	if err2 != nil {
		panic(err2.Error())
	}

	defer db_old.Close()
	defer db_new.Close()

	db_old.SetConnMaxLifetime(time.Minute * 5)
	db_new.SetConnMaxLifetime(time.Minute * 5)

	db_old.SetMaxOpenConns(50)
	db_new.SetMaxOpenConns(50)

	db_old.SetMaxIdleConns(5)
	db_new.SetMaxIdleConns(5)

	users := getUsers(db_old)

	var rowCount int

	stmt, err := db_new.Prepare("INSERT INTO users (identifier, name, data) VALUES (?, ?, ?)")
	if err != nil {
		log.Printf("Error %s", err.Error())
	}

	char_stmt, err := db_new.Prepare("INSERT INTO characters (data, skin, inventory, position) VALUES (?, ?, ?, ?)")
	if err != nil {
		log.Printf("Error %s", err.Error())
	}

	stmt2, err := db_new.Prepare("INSERT INTO bank_accounts (bankId, balance) VALUES (?, ?)")
	if err != nil {
		log.Printf("Error %s", err.Error())
	}

	stmt4, err := db_new.Prepare("INSERT INTO owned_properties (owner, items, clothes) VALUES (?, ?, ?)")
	if err != nil {
		log.Printf("Error %s", err.Error())
	}

	stmt5, err := db_new.Prepare("INSERT INTO owned_vehicles (owner, vehicle) VALUES (?, ?)")
	if err != nil {
		log.Printf("Error %s", err.Error())
	}

	var wg sync.WaitGroup

	var threads int

	for users.Next() {
		threads++
		rowCount++
		log.Printf("%d / 70000", rowCount)
		var old_user OldUser

		err = users.Scan(&old_user.Identifier, &old_user.Money, &old_user.Name, &old_user.Skin, &old_user.Loadout, &old_user.Position, &old_user.Bank, &old_user.Inventory, &old_user.Firstname, &old_user.Lastname, &old_user.Dateofbirth, &old_user.Sex, &old_user.Height)
		if err != nil {
			log.Printf("Error %s", err.Error())
		}

		wg.Add(1)

		go processUser(old_user, db_old, db_new, stmt, char_stmt, stmt2, stmt4, stmt5, &wg)

		if threads >= 500 {
			threads = 0
			wg.Wait()
		}

		// inv := processInventory(old_user.Inventory)

		// loadout := processLoadout(inv, old_user.Loadout)

		// black_money := handleBlackMoney(db_old, old_user.Identifier)

		// if black_money > 0 {
		// 	loadout = append(loadout, InventoryItem{Name: "black_money", Count: black_money})
		// 	log.Printf("%s %s %s %d", old_user.Identifier, old_user.Inventory, old_user.Loadout, black_money)
		// }

		// var inventory []byte

		// if loadout != nil {
		// 	// log.Printf("No loadout")

		// 	// log.Printf("%s %s %s %d", old_user.Identifier, old_user.Inventory, old_user.Loadout, black_money)

		// 	jsonInventory, err := json.Marshal(loadout)

		// 	if err != nil {
		// 		log.Printf("Error marshall inventory")
		// 	}

		// 	// log.Printf("%s", jsonInventory)

		// 	inventory = jsonInventory
		// }

		// charData := createCharData(old_user)

		// jsonCharData, err := json.Marshal(charData)

		// if err != nil {
		// 	// log.Printf("Error char data")
		// 	panic("Error char data")
		// }

		// charId := insertCharacter(char_stmt, string(jsonCharData), old_user.Skin, string(inventory), old_user.Position)

		// userData := createUserData(charId, old_user.Firstname, old_user.Lastname)

		// jsonUserData, err := json.Marshal(userData)

		// if err != nil {
		// 	// log.Printf("Error char data")
		// 	panic("Error char data")
		// }

		// // log.Printf("%s", string(jsonUserData))

		// go insertUser(stmt, old_user.Identifier, old_user.Name, string(jsonUserData))

		// go insertBank(stmt2, charId, old_user.Bank)

		// go handleProp(db_old, stmt4, old_user.Identifier, charId)

		// go handleVeh(db_old, stmt5, old_user.Identifier, charId)

		// // if old_user.Name == nil {
		// // 	log.Printf(old_user.Identifier)
		// // }

	}

	// defer the close till after the main function has finished
	// executing
	defer stmt.Close()
	defer char_stmt.Close()
	defer stmt2.Close()
	defer stmt4.Close()
	defer stmt5.Close()
	defer db_old.Close()
	defer db_new.Close()
}

func main() {
	fmt.Println("Hello, World!")
	initDb()
}
