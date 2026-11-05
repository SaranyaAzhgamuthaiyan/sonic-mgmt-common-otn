////////////////////////////////////////////////////////////////////////////////
//                                                                            //
//  Copyright 2023 Dell, Inc.                                                 //
//                                                                            //
//  Licensed under the Apache License, Version 2.0 (the "License");           //
//  you may not use this file except in compliance with the License.          //
//  You may obtain a copy of the License at                                   //
//                                                                            //
//     http://www.apache.org/licenses/LICENSE-2.0                             //
//                                                                            //
//  Unless required by applicable law or agreed to in writing, software       //
//  distributed under the License is distributed on an "AS IS" BASIS,         //
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  //
//  See the License for the specific language governing permissions and       //
//  limitations under the License.                                            //
//                                                                            //
////////////////////////////////////////////////////////////////////////////////

package transformer_test

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"

	"github.com/Azure/sonic-mgmt-common/internal/mdb"
	"github.com/Azure/sonic-mgmt-common/translib/db"
	"github.com/Azure/sonic-mgmt-common/translib/ocbinds"
	"github.com/go-redis/redis/v7"
	"github.com/openconfig/ygot/ytypes"

	"testing"
)

const hostDBName = "host"

var dbConfig struct {
	Instances map[string]map[string]interface{} `json:"INSTANCES"`
	Databases map[string]map[string]interface{} `json:"DATABASES"`
}
var rclient *redis.Client
var rclientNs map[string]*redis.Client
var filehandle *os.File
var ygSchema *ytypes.Schema
var rclientDBNum map[db.DBNum]*redis.Client
var rclientDBByNs map[string]map[db.DBNum]*redis.Client
var mdbNames []string

func getDBOptions(dbNo db.DBNum, isWriteDisabled bool) db.Options {
	var opt db.Options

	switch dbNo {
	case db.ApplDB, db.CountersDB, db.AsicDB, db.FlexCounterDB:
		opt = getDBOptionsWithSeparator(dbNo, "", ":", ":", isWriteDisabled)
		break
	case db.ConfigDB, db.StateDB:
		opt = getDBOptionsWithSeparator(dbNo, "", "|", "|", isWriteDisabled)
		break
	}

	return opt
}

func initMdbNames() {
	asicConf := mdb.DefaultAsicConfPath
	if path, ok := os.LookupEnv("ASIC_CONFIG_PATH"); ok {
		asicConf = path
	}
	numAsic := mdb.ReadNumAsic(asicConf)
	mdbNames = []string{hostDBName}
	if numAsic > 1 {
		for i := 0; i < numAsic; i++ {
			mdbNames = append(mdbNames, "asic"+strconv.Itoa(i))
		}
	}
}

func getMdbNames() []string {
	return mdbNames
}

func redisClientForMdb(mdbName string, dbNum db.DBNum) *redis.Client {
	opt := getDBOptions(dbNum, false)
	opt.MDBName = mdbName
	if mdbName != hostDBName {
		opt.ForceNewRedisConnection = true
	}
	return db.RedisClientForOptions(opt)
}

func getDBOptionsWithSeparator(dbNo db.DBNum, initIndicator string, tableSeparator string, keySeparator string, isWriteDisabled bool) db.Options {
	return (db.Options{
		DBNo:               dbNo,
		InitIndicator:      initIndicator,
		TableNameSeparator: tableSeparator,
		KeySeparator:       keySeparator,
		IsWriteDisabled:    isWriteDisabled,
	})
}

func TestMain(t *testing.M) {
	fmt.Println("----- Setting up transformer tests -----")
	if err := setup(); err != nil {
		fmt.Fprintf(os.Stderr, "Error setting up transformer testing state: %v.\n", err)
		os.Exit(1)
	}
	defer teardown()
	os.Exit(t.Run())
}

func initDbConfig() error {
	dbConfigFile := "/run/redis/sonic-db/database_config.json"
	if path, ok := os.LookupEnv("DB_CONFIG_PATH"); ok {
		dbConfigFile = path
	}

	fmt.Println("dbConfigFile =", dbConfigFile)
	dbConfigJson, err := ioutil.ReadFile(dbConfigFile)
	if err == nil {
		err = json.Unmarshal(dbConfigJson, &dbConfig)
	}

	return err
}

func clearDb() {

	dbNumTblList := make(map[db.DBNum][]string)
	dbNumTblList[db.ConfigDB] = []string{
		"TEST_SENSOR_GROUP",
		"TEST_SENSOR_A_TABLE",
		"TEST_SENSOR_B_TABLE",
		"TEST_SET_TABLE",
	}
	dbNumTblList[db.CountersDB] = []string{
		"TEST_SENSOR_MODE_TABLE",
	}

	for dbNum, tblList := range dbNumTblList {
		for _, tbl := range tblList {
			tblKeys, keysErr := rclientDBNum[dbNum].Keys(tbl + "|*").Result()
			if keysErr != nil {
				fmt.Printf("Couldn't fetch keys for table %v", tbl)
				continue
			}
			for _, key := range tblKeys {
				e := rclientDBNum[dbNum].Del(key).Err()
				if e != nil {
					fmt.Printf("Couldn't delete key %v", key)
				}
			}
		}
	}
}

/* Prepares the database clients in Redis Server. */
func prepareDb() bool {
	initMdbNames()

	testDBNums := []db.DBNum{db.CountersDB, db.ConfigDB, db.ApplDB, db.StateDB}
	rclientDBByNs = make(map[string]map[db.DBNum]*redis.Client)
	rclientNs = make(map[string]*redis.Client)

	for _, ns := range mdbNames {
		rclientDBByNs[ns] = make(map[db.DBNum]*redis.Client)
		for _, dbNum := range testDBNums {
			client := redisClientForMdb(ns, dbNum)
			if client == nil {
				fmt.Printf("error in redisClientForMdb(%q, %v)", ns, dbNum)
				return false
			}
			if _, err := client.Ping().Result(); err != nil {
				fmt.Printf("failed to connect to redis ns=%q db=%v: %v", ns, dbNum, err)
				return false
			}
			rclientDBByNs[ns][dbNum] = client
		}
		rclientNs[ns] = rclientDBByNs[ns][db.ConfigDB]
	}

	rclientDBNum = rclientDBByNs[hostDBName]
	rclient = rclientNs[hostDBName]
	return true
}

// setups state each of the tests uses
func setup() error {
	fmt.Println("----- Performing setup -----")
	var err error
	if ygSchema, err = ocbinds.GetSchema(); err != nil {
		panic("Error in getting the schema: " + err.Error())
		return err
	}

	if err := initDbConfig(); err != nil {
		return err
	}

	/* Prepare the Redis database clients. */
	if !prepareDb() {
		return fmt.Errorf("Failure in setting up Redis DB client.")
	}

	//Clear all tables which are used for testing
	clearDb()

	return nil
}

func teardown() error {
	fmt.Println("----- Performing teardown -----")
	clearDb()
	for ns, byNum := range rclientDBByNs {
		for dbNum, client := range byNum {
			if client != nil {
				db.CloseRedisClient(client)
			}
			rclientDBByNs[ns][dbNum] = nil
		}
	}

	return nil
}

func loadDBWithClient(client *redis.Client, dbNum db.DBNum, mpi map[string]interface{}) {
	if client == nil {
		fmt.Printf("loadDB: nil redis client for db %v", dbNum)
		return
	}
	opts := getDBOptions(dbNum, false)
	for key, fv := range mpi {
		switch fv.(type) {
		case map[string]interface{}:
			for subKey, subValue := range fv.(map[string]interface{}) {
				newKey := key + opts.KeySeparator + subKey
				_, err := client.HMSet(newKey, subValue.(map[string]interface{})).Result()

				if err != nil {
					fmt.Printf("Invalid data for db: %v : %v %v", newKey, subValue, err)
				}

			}
		default:
			fmt.Printf("Invalid data for db: %v : %v", key, fv)
		}
	}
}

func loadDB(a interface{}, b interface{}, c ...map[string]interface{}) {
	switch v := a.(type) {
	case string:
		mdbName := v
		dbNum := b.(db.DBNum)
		loadDBWithClient(rclientDBByNs[mdbName][dbNum], dbNum, c[0])
	case db.DBNum:
		loadDBWithClient(rclientDBNum[v], v, b.(map[string]interface{}))
	default:
		fmt.Printf("loadDB: unsupported first argument type %T", a)
	}
}

func unloadDBWithClient(client *redis.Client, dbNum db.DBNum, mpi map[string]interface{}) {
	if client == nil {
		fmt.Printf("unloadDB: nil redis client for db %v", dbNum)
		return
	}
	opts := getDBOptions(dbNum, false)
	for key, fv := range mpi {
		switch fv.(type) {
		case map[string]interface{}:
			for subKey, subValue := range fv.(map[string]interface{}) {
				newKey := key + opts.KeySeparator + subKey
				_, err := client.Del(newKey).Result()

				if err != nil {
					fmt.Printf("Invalid data for db: %v : %v %v", newKey, subValue, err)
				}

			}
		default:
			fmt.Printf("Invalid data for db: %v : %v", key, fv)
		}
	}

}

func unloadDB(a interface{}, b interface{}, c ...map[string]interface{}) {
	switch v := a.(type) {
	case string:
		mdbName := v
		dbNum := b.(db.DBNum)
		unloadDBWithClient(rclientDBByNs[mdbName][dbNum], dbNum, c[0])
	case db.DBNum:
		unloadDBWithClient(rclientDBNum[v], v, b.(map[string]interface{}))
	default:
		fmt.Printf("unloadDB: unsupported first argument type %T", a)
	}
}

func getDbClient(dbNum int) *redis.Client {
	return redisClientForMdb(hostDBName, db.DBNum(dbNum))
}
