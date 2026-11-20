////////////////////////////////////////////////////////////////////////////////
//                                                                            //
//  Copyright 2019 Broadcom. The term Broadcom refers to Broadcom Inc. and/or //
//  its subsidiaries.                                                         //
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

/*
Package translib implements APIs like Create, Get, Subscribe etc.

to be consumed by the north bound management server implementations

This package takes care of translating the incoming requests to

Redis ABNF format and persisting them in the Redis DB.

It can also translate the ABNF format to YANG specific JSON IETF format

This package can also talk to non-DB clients.
*/

package translib

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"

	"github.com/Azure/sonic-mgmt-common/translib/db"
	"github.com/Azure/sonic-mgmt-common/translib/tlerr"
	"github.com/Workiva/go-datastructures/queue"
	log "github.com/golang/glog"
	"github.com/openconfig/ygot/ygot"
)

// Write lock for all write operations to be synchronized
var writeMutex = &sync.Mutex{}
var ENABLE_TRANSLIB_ROLLBACK = "false"

type ErrSource int

const (
	ProtoErr ErrSource = iota
	AppErr
)

type TranslibFmtType int

const (
	TRANSLIB_FMT_IETF_JSON TranslibFmtType = iota
	TRANSLIB_FMT_YGOT
)

type UserRoles struct {
	Name  string
	Roles []string
}

type SetRequest struct {
	Path             string
	Payload          []byte
	User             UserRoles
	AuthEnabled      bool
	ClientVersion    Version
	DeleteEmptyEntry bool
}

type SetResponse struct {
	ErrSrc ErrSource
	Err    error
}

type QueryParameters struct {
	Depth   uint     // range 1 to 65535, default is <U+0093>0<U+0094> i.e. all
	Content string   // all, config, non-config(REST)/state(GNMI), operational(GNMI only)
	Fields  []string // list of fields from NBI
}

type GetRequest struct {
	Path          string
	FmtType       TranslibFmtType
	User          UserRoles
	AuthEnabled   bool
	ClientVersion Version
	QueryParams   QueryParameters
	Ctxt          context.Context
}

type GetResponse struct {
	Payload   []byte
	ValueTree ygot.ValidatedGoStruct
	ErrSrc    ErrSource
}

type ActionRequest struct {
	Path          string
	Payload       []byte
	User          UserRoles
	AuthEnabled   bool
	ClientVersion Version
}

type ActionResponse struct {
	Payload []byte
	ErrSrc  ErrSource
}

// BulkRequestEntry - Entry for BulkRequest
type BulkRequestEntry struct {
	Entry                 SetRequest
	Operation             int
	ResourceCheckOnDelete bool
}

// BulkRequest - Will be used by Northbounds to send Bulk Request.
type BulkRequest struct {
	Request       []BulkRequestEntry
	User          UserRoles
	AuthEnabled   bool
	ClientVersion Version
}

// BulkResponseEntry - Entry for BulkResponse
type BulkResponseEntry struct {
	Entry     SetResponse
	Operation int
}

// BulkResponse - Will be used by Northbounds to receive Bulk Response.
type BulkResponse struct {
	Response []BulkResponseEntry
}

type ModelData struct {
	Name string
	Org  string
	Ver  string
}

// initializes logging and app modules
func init() {
	log.Flush()
}

func commitTransactions(redisMdbInstances map[string]*db.DB, nsMaps []NamespacePayload) error {

	for ns, redisDbInstance := range redisMdbInstances {

		if err := redisDbInstance.CommitTx(); err != nil {
			return err
		}

		// Find the NamespaceMap with Namespace == ns and mark committed
		for i := range nsMaps {
			if nsMaps[i].Namespace == ns {
				nsMaps[i].Commited = true
			}
		}

	}

	return nil
}

// closeMdbInstances releases the DB handles opened for a write request.
func closeMdbInstances(redisMdbInstances map[string]*db.DB) {
	for _, d := range redisMdbInstances {
		d.DeleteDB()
	}
}

// commitWithRollback commits all namespaces. If the commit fails, the namespaces already
// committed are rolled back through rollbackFn (when rollback is enabled).
func commitWithRollback(redisMdbInstances map[string]*db.DB, nsPayloads []NamespacePayload,
	rollbackFn func(ns string, d *db.DB) error) error {
	cerr := commitTransactions(redisMdbInstances, nsPayloads)
	if cerr != nil && ENABLE_TRANSLIB_ROLLBACK == "true" {
		for ns, redisDbInstance := range redisMdbInstances {
			for _, payload := range nsPayloads {
				if payload.Namespace == ns && payload.Commited {
					log.Infof("Rollback key:%v, Namespace: %s", payload.Key, payload.Namespace)
					if err := rollbackFn(ns, redisDbInstance); err != nil {
						log.Infof("Rollback operation failed!")
					}
					break
				}
			}
		}
	}
	if ENABLE_TRANSLIB_ROLLBACK == "true" {
		if err := processPostPhase(); err != nil {
			log.Infof("Cleanup of backup entries operation failed!")
		}
	}
	return cerr
}

// expandAllNamespaces expands the "*" namespace into one entry per namespace, keeping the key.
func expandAllNamespaces(nsPayloads []NamespacePayload) []NamespacePayload {
	if len(nsPayloads) != 1 || nsPayloads[0].Namespace != "*" {
		return nsPayloads
	}
	var expanded []NamespacePayload
	for _, ns := range db.GetMultiDbNames() {
		expanded = append(expanded, NamespacePayload{Namespace: ns, Key: nsPayloads[0].Key})
	}
	return expanded
}

// abortAll aborts all namespaces and returns the abort error if it fails, else err.
func abortAll(redisMdbInstances map[string]*db.DB, err error) error {
	if abortErr := abortTransactions(redisMdbInstances); abortErr != nil {
		return abortErr
	}
	return err
}

func abortTransactions(redisMdbInstances map[string]*db.DB) error {

	for _, redisDbInstance := range redisMdbInstances {

		if err := redisDbInstance.AbortTx(); err != nil {
			log.Infof("Abort transaction failed for %s: %v", redisDbInstance, err)
			return err
		}
	}
	return nil
}

func initializeMDBInstance(mdbName string, redisMdbInstances map[string]*db.DB) (*db.DB, error) {
	var d *db.DB
	// Check if the MDB instance already exists in the map
	if existingInstance, exists := redisMdbInstances[mdbName]; !exists {
		var err error
		d, err = db.NewDB(getDBOptions(db.ConfigDB, SetMDBName(mdbName)))
		if err != nil {
			return nil, err
		}

		err = d.StartTx(nil, nil)
		if err != nil {
			return nil, err
		}

		redisMdbInstances[mdbName] = d
	} else {
		d = existingInstance
	}
	return d, nil
}

func processPostPhase() error {
	log.Infof("Cleanup: deleting all backup files from /tmp")

	// Match all files with pattern: /tmp/common_backup_*.json
	backupFiles, err := filepath.Glob("/var/tmp/common_backup_*.json")
	if err != nil {
		log.Infof("Failed to list backup files: %v", err)
		return err
	}

	if len(backupFiles) == 0 {
		log.Infof("No backup files found to delete.")
		return nil
	}

	for _, file := range backupFiles {
		err := os.Remove(file)
		if err != nil {
			log.Infof("Failed to delete backup file: %s, error: %v", file, err)
			return err
		}
		log.Infof("Deleted backup file: %s", file)
	}
	return nil
}

type nsWriteContext struct {
	nsPayload    NamespacePayload
	payloadBytes []byte
	d            *db.DB
}

// Create - Creates entries in the redis DB pertaining to the path and payload
func Create(req SetRequest) (SetResponse, error) {
	var keys []db.WatchKeys
	var resp SetResponse
	var d *db.DB

	redisMdbInstances := make(map[string]*db.DB)

	path := req.Path
	payload := req.Payload
	if !isAuthorizedForSet(req) {
		return resp, tlerr.AuthorizationError{
			Format: "User is unauthorized for Create Operation",
			Path:   path,
		}
	}

	log.Info("Create request received with path =", path)
	log.Info("Create request received with payload =", string(payload))

	app, appInfo, err := getAppModule(path, req.ClientVersion)

	if err != nil {
		resp.ErrSrc = ProtoErr
		return resp, err
	}

	err = appInitialize(app, appInfo, path, &payload, nil, CREATE)

	if err != nil {
		resp.ErrSrc = AppErr
		return resp, err
	}

	namespacePayloads, err := getAppNamespace(*app, path)

	if err != nil {
		log.Infof("Error from getNamespace %v", err)
		resp.ErrSrc = AppErr
		return resp, tlerr.InvalidArgsError{Format: err.Error()}
	}

	writeMutex.Lock()
	defer writeMutex.Unlock()
	defer closeMdbInstances(redisMdbInstances)

	log.Infof("Create: starting PHASE 1 - CVL validation for all namespaces, total namespaces=%d", len(namespacePayloads))

	var writeContexts []nsWriteContext

	for _, nsPayload := range namespacePayloads {
		nameSpace := nsPayload.Namespace
		payloads := nsPayload.Payloads
		if len(payloads) == 0 {
			log.Infof("Payloads for namespace '%s' are empty, using top-level payload", nameSpace)
			var topLevelPayload map[string]interface{}
			err := json.Unmarshal(payload, &topLevelPayload)
			if err != nil {
				resp.ErrSrc = AppErr
				return resp, abortAll(redisMdbInstances, err)
			}
			payloads = append(payloads, topLevelPayload)
		}
		log.Infof("Create: PHASE1 - CVL Validation for namespace=%s", nameSpace)
		log.Infof("Namespace: %s", nameSpace)

		for _, pl := range payloads {
			payloadBytes, err := json.Marshal(pl)
			if err != nil {
				resp.ErrSrc = AppErr
				return resp, abortAll(redisMdbInstances, err)
			}

			err = appInitialize(app, appInfo, path, &payloadBytes, nil, CREATE)
			if err != nil {
				resp.ErrSrc = AppErr
				return resp, abortAll(redisMdbInstances, err)
			}

			d, err = initializeMDBInstance(nameSpace, redisMdbInstances)
			if err != nil {
				resp.ErrSrc = ProtoErr
				return resp, abortAll(redisMdbInstances, err)
			}

			keys, err = (*app).translateCreate(d)
			if err != nil {
				resp.ErrSrc = AppErr
				return resp, abortAll(redisMdbInstances, err)
			}

			err = d.AppendWatchTx(keys, appInfo.tablesToWatch)
			if err != nil {
				resp.ErrSrc = AppErr
				return resp, abortAll(redisMdbInstances, err)
			}

			// CVL only
			err = appCVLValidate(*app, d)
			if err != nil {
				log.Errorf("Create: PHASE 1 - CVL failed for namespace=%s, aborting all , error=%v", nameSpace, err)
				resp.ErrSrc = AppErr
				return resp, abortAll(redisMdbInstances, err)
			}
			log.Infof("Create: PHASE 1- CVL passed for namespace=%s, queuing for Phase2", nameSpace)

			writeContexts = append(writeContexts, nsWriteContext{
				nsPayload:    nsPayload,
				payloadBytes: payloadBytes,
				d:            d,
			})
		}
	}
	log.Infof("Create: PHASE 1 complete -CVL Passed for all namespaces, total=%d, proceeding to PHASE2", len(writeContexts))
	log.Infof("Create: Starting PHASE 2 - actual write for all namespaces")
	for _, ctx := range writeContexts {
		ctx.d.SetSkipCVL(true)
		log.Infof("Create: PHASE2 - writing namespace=%s", ctx.nsPayload.Namespace)
		err = appInitialize(app, appInfo, path, &ctx.payloadBytes, nil, CREATE)
		if err != nil {
			resp.ErrSrc = AppErr
			return resp, abortAll(redisMdbInstances, err)
		}

		// Re-run translate to repopulate cmnAppTableMap for processCreate
		_, err = (*app).translateCreate(ctx.d)
		if err != nil {
			resp.ErrSrc = AppErr
			return resp, abortAll(redisMdbInstances, err)
		}

		if ENABLE_TRANSLIB_ROLLBACK == "true" {
			ctx.d.Opts.DisableCVLCheck = true
			err = appPreparePhase(*app, ctx.d, ctx.nsPayload.Key)
			ctx.d.Opts.DisableCVLCheck = false
			if err != nil {
				resp.ErrSrc = AppErr
				return resp, abortAll(redisMdbInstances, err)
			}
		}
		resp, err = (*app).processCreate(ctx.d)
		if err != nil {
			resp.ErrSrc = AppErr
			return resp, abortAll(redisMdbInstances, err)
		}

	}
	log.Infof("Create: PHASE 2 - all namespaces written successfully, proceeding to commit")
	return resp, commitWithRollback(redisMdbInstances, namespacePayloads,
		func(_ string, d *db.DB) error { return appRollback(*app, d) })

}

// Update - Updates entries in the redis DB pertaining to the path and payload
func Update(req SetRequest) (SetResponse, error) {
	var keys []db.WatchKeys
	var resp SetResponse
	var d *db.DB

	redisMdbInstances := make(map[string]*db.DB)

	path := req.Path
	payload := req.Payload
	if !isAuthorizedForSet(req) {
		return resp, tlerr.AuthorizationError{
			Format: "User is unauthorized for Update Operation",
			Path:   path,
		}
	}

	log.Info("Update request received with path =", path)
	log.Info("Update request received with payload =", string(payload))

	app, appInfo, err := getAppModule(path, req.ClientVersion)

	if err != nil {
		resp.ErrSrc = ProtoErr
		return resp, err
	}

	err = appInitialize(app, appInfo, path, &payload, nil, UPDATE)

	if err != nil {
		resp.ErrSrc = AppErr
		return resp, err
	}

	namespacePayloads, err := getAppNamespace(*app, path)
	if err != nil {
		log.Infof("Error from getNamespace %v", err)
		resp.ErrSrc = AppErr
		return resp, tlerr.InvalidArgsError{Format: err.Error()}
	}

	writeMutex.Lock()
	defer writeMutex.Unlock()
	defer closeMdbInstances(redisMdbInstances)

	var writeContexts []nsWriteContext

	log.Infof("Update: Starting PHASE 1 - CVL validation for all namespaces, total=%d", len(namespacePayloads))

	for _, nsPayload := range namespacePayloads {
		nameSpace := nsPayload.Namespace
		payloads := nsPayload.Payloads
		if len(payloads) == 0 {
			log.Infof("Payloads for namespace '%s' are empty, using top-level payload", nameSpace)
			var topLevelPayload map[string]interface{}
			err := json.Unmarshal(payload, &topLevelPayload)
			if err != nil {
				resp.ErrSrc = AppErr
				log.Errorf("Failed to unmarshal top-level payload: %v", err)
				return resp, abortAll(redisMdbInstances, err)
			}
			payloads = append(payloads, topLevelPayload)
		}

		log.Infof("Update: PHASE 1 - CVL validation for namespace=%s", nameSpace)

		for _, pl := range payloads {
			payloadBytes, err := json.Marshal(pl)
			if err != nil {
				resp.ErrSrc = AppErr
				return resp, abortAll(redisMdbInstances, err)
			}

			err = appInitialize(app, appInfo, path, &payloadBytes, nil, UPDATE)
			if err != nil {
				resp.ErrSrc = AppErr
				return resp, abortAll(redisMdbInstances, err)
			}

			d, err = initializeMDBInstance(nameSpace, redisMdbInstances)
			if err != nil {
				resp.ErrSrc = ProtoErr
				return resp, abortAll(redisMdbInstances, err)
			}

			keys, err = (*app).translateUpdate(d)
			if err != nil {
				resp.ErrSrc = AppErr
				return resp, abortAll(redisMdbInstances, err)
			}

			err = d.AppendWatchTx(keys, appInfo.tablesToWatch)
			if err != nil {
				resp.ErrSrc = AppErr
				return resp, abortAll(redisMdbInstances, err)
			}

			err = appCVLValidate(*app, d)
			if err != nil {
				log.Errorf("Update: PHASE 1 - CVL FAILED for namespace=%s, aborting all, error=%v", nameSpace, err)
				resp.ErrSrc = AppErr
				return resp, abortAll(redisMdbInstances, err)
			}
			log.Infof("Update: PHASE 1 - CVL passed for namespace=%s, queuing for Phase 2", nameSpace)

			writeContexts = append(writeContexts, nsWriteContext{
				nsPayload:    nsPayload,
				payloadBytes: payloadBytes,
				d:            d,
			})
		}
	}

	log.Infof("Update: PHASE 1 complete - CVL passed for all namespaces, total=%d, proceeding to PHASE 2", len(writeContexts))

	log.Infof("Update: Starting PHASE 2 - actual Redis writes")

	for _, ctx := range writeContexts {
		ctx.d.SetSkipCVL(true)
		log.Infof("Update: PHASE 2 - writing namespace=%s", ctx.nsPayload.Namespace)

		err = appInitialize(app, appInfo, path, &ctx.payloadBytes, nil, UPDATE)
		if err != nil {
			resp.ErrSrc = AppErr
			return resp, abortAll(redisMdbInstances, err)
		}

		_, err = (*app).translateUpdate(ctx.d)
		if err != nil {
			resp.ErrSrc = AppErr
			return resp, abortAll(redisMdbInstances, err)
		}

		if ENABLE_TRANSLIB_ROLLBACK == "true" {
			ctx.d.Opts.DisableCVLCheck = true
			err = appPreparePhase(*app, ctx.d, ctx.nsPayload.Key)
			ctx.d.Opts.DisableCVLCheck = false
			if err != nil {
				resp.ErrSrc = AppErr
				return resp, abortAll(redisMdbInstances, err)
			}
		}
		resp, err = (*app).processUpdate(ctx.d)
		if err != nil {
			resp.ErrSrc = AppErr
			return resp, abortAll(redisMdbInstances, err)
		}

	}

	log.Infof("Update: PHASE 2 complete - all namespaces written successfully, proceeding to commit")

	return resp, commitWithRollback(redisMdbInstances, namespacePayloads,
		func(_ string, d *db.DB) error { return appRollback(*app, d) })
}

// Replace - Replaces entries in the redis DB pertaining to the path and payload
func Replace(req SetRequest) (SetResponse, error) {
	var err error
	var keys []db.WatchKeys
	var resp SetResponse
	var d *db.DB

	redisMdbInstances := make(map[string]*db.DB)

	path := req.Path
	payload := req.Payload
	if !isAuthorizedForSet(req) {
		return resp, tlerr.AuthorizationError{
			Format: "User is unauthorized for Replace Operation",
			Path:   path,
		}
	}

	log.Info("Replace request received with path =", path)
	log.Info("Replace request received with payload =", string(payload))

	app, appInfo, err := getAppModule(path, req.ClientVersion)

	if err != nil {
		resp.ErrSrc = ProtoErr
		return resp, err
	}

	err = appInitialize(app, appInfo, path, &payload, nil, REPLACE)
	if err != nil {
		resp.ErrSrc = AppErr
		return resp, err
	}

	namespacePayloads, err := getAppNamespace(*app, path)
	if err != nil {
		log.Infof("Error from getNamespace %v", err)
		resp.ErrSrc = AppErr
		return resp, tlerr.InvalidArgsError{Format: err.Error()}
	}

	if len(namespacePayloads) == 1 && namespacePayloads[0].Namespace == "*" {
		resp.ErrSrc = AppErr
		return resp, tlerr.InvalidArgsError{Format: "Replace payload is empty", Path: path}
	}

	writeMutex.Lock()
	defer writeMutex.Unlock()
	defer closeMdbInstances(redisMdbInstances)

	var writeContexts []nsWriteContext

	log.Infof("Replace: Starting PHASE 1 - CVL validation for all namespaces, total=%d", len(namespacePayloads))

	for _, nsPayload := range namespacePayloads {
		nameSpace := nsPayload.Namespace
		payloads := nsPayload.Payloads
		if len(payloads) == 0 {
			log.Infof("Payloads for namespace '%s' are empty, using top-level payload", nameSpace)
			var topLevelPayload map[string]interface{}
			err := json.Unmarshal(payload, &topLevelPayload)
			if err != nil {
				resp.ErrSrc = AppErr
				log.Errorf("Failed to unmarshal top-level payload: %v", err)
				return resp, abortAll(redisMdbInstances, err)
			}
			payloads = append(payloads, topLevelPayload)
		}

		log.Infof("Replace: PHASE 1 - CVL validation for namespace=%s", nameSpace)

		for _, pl := range payloads {
			payloadBytes, err := json.Marshal(pl)
			if err != nil {
				resp.ErrSrc = AppErr
				return resp, abortAll(redisMdbInstances, err)
			}

			err = appInitialize(app, appInfo, path, &payloadBytes, nil, REPLACE)
			if err != nil {
				resp.ErrSrc = AppErr
				return resp, abortAll(redisMdbInstances, err)
			}

			d, err = initializeMDBInstance(nameSpace, redisMdbInstances)
			if err != nil {
				resp.ErrSrc = ProtoErr
				return resp, abortAll(redisMdbInstances, err)
			}

			keys, err = (*app).translateReplace(d)
			if err != nil {
				resp.ErrSrc = AppErr
				return resp, abortAll(redisMdbInstances, err)
			}

			err = d.AppendWatchTx(keys, appInfo.tablesToWatch)
			if err != nil {
				resp.ErrSrc = AppErr
				return resp, abortAll(redisMdbInstances, err)
			}

			err = appCVLValidate(*app, d)
			if err != nil {
				log.Errorf("Replace: PHASE 1 - CVL FAILED for namespace=%s, aborting all, error=%v", nameSpace, err)
				resp.ErrSrc = AppErr
				return resp, abortAll(redisMdbInstances, err)
			}
			log.Infof("Replace: PHASE 1 - CVL passed for namespace=%s, queuing for Phase 2", nameSpace)

			writeContexts = append(writeContexts, nsWriteContext{
				nsPayload:    nsPayload,
				payloadBytes: payloadBytes,
				d:            d,
			})
		}
	}

	log.Infof("Replace: PHASE 1 complete - CVL passed for all namespaces, total=%d, proceeding to PHASE 2", len(writeContexts))
	log.Infof("Replace: Starting PHASE 2 - actual Redis writes")

	for _, ctx := range writeContexts {
		ctx.d.SetSkipCVL(true)
		log.Infof("Replace: PHASE 2 - writing namespace=%s", ctx.nsPayload.Namespace)

		err = appInitialize(app, appInfo, path, &ctx.payloadBytes, nil, REPLACE)
		if err != nil {
			resp.ErrSrc = AppErr
			return resp, abortAll(redisMdbInstances, err)
		}

		_, err = (*app).translateReplace(ctx.d)
		if err != nil {
			resp.ErrSrc = AppErr
			return resp, abortAll(redisMdbInstances, err)
		}

		if ENABLE_TRANSLIB_ROLLBACK == "true" {
			ctx.d.Opts.DisableCVLCheck = true
			err = appPreparePhase(*app, ctx.d, ctx.nsPayload.Key)
			ctx.d.Opts.DisableCVLCheck = false
			if err != nil {
				resp.ErrSrc = AppErr
				return resp, abortAll(redisMdbInstances, err)
			}
		}

		resp, err = (*app).processReplace(ctx.d)
		if err != nil {
			resp.ErrSrc = AppErr
			return resp, abortAll(redisMdbInstances, err)
		}

	}

	log.Infof("Replace: PHASE 2 complete - all namespaces written successfully, proceeding to commit")

	return resp, commitWithRollback(redisMdbInstances, namespacePayloads,
		func(_ string, d *db.DB) error { return appRollback(*app, d) })
}

// Delete - Deletes entries in the redis DB pertaining to the path
func Delete(req SetRequest) (SetResponse, error) {
	var err error
	var resp SetResponse
	var keys []db.WatchKeys
	var d *db.DB

	redisMdbInstances := make(map[string]*db.DB)

	path := req.Path
	if !isAuthorizedForSet(req) {
		return resp, tlerr.AuthorizationError{
			Format: "User is unauthorized for Delete Operation",
			Path:   path,
		}
	}

	log.Info("Delete request received with path =", path)

	app, appInfo, err := getAppModule(path, req.ClientVersion)

	if err != nil {
		resp.ErrSrc = ProtoErr
		return resp, err
	}

	opts := appOptions{deleteEmptyEntry: req.DeleteEmptyEntry}
	err = appInitialize(app, appInfo, path, nil, &opts, DELETE)

	if err != nil {
		resp.ErrSrc = AppErr
		return resp, err
	}

	namespacePayloads, err := getAppNamespace(*app, path)
	if err != nil {
		log.Infof("Error from getNamespace %v", err)
		resp.ErrSrc = AppErr
		return resp, tlerr.InvalidArgsError{Format: err.Error()}
	}

	// Expand "*" (no key in the path) to all namespaces
	namespacePayloads = expandAllNamespaces(namespacePayloads)

	writeMutex.Lock()
	defer writeMutex.Unlock()
	defer closeMdbInstances(redisMdbInstances)

	var writeContexts []nsWriteContext

	log.Infof("Delete: Starting PHASE 1 - CVL validation for all namespaces, total=%d", len(namespacePayloads))

	for _, nsPayload := range namespacePayloads {
		opts := appOptions{deleteEmptyEntry: req.DeleteEmptyEntry}
		err = appInitialize(app, appInfo, path, nil, &opts, DELETE)
		if err != nil {
			resp.ErrSrc = AppErr
			return resp, err
		}

		nameSpace := nsPayload.Namespace
		log.Infof("Delete: PHASE 1 - CVL validation for namespace=%s", nameSpace)

		d, err = initializeMDBInstance(nameSpace, redisMdbInstances)
		if err != nil {
			resp.ErrSrc = ProtoErr
			return resp, abortAll(redisMdbInstances, err)
		}

		keys, err = (*app).translateDelete(d)
		if err != nil {
			resp.ErrSrc = AppErr
			return resp, abortAll(redisMdbInstances, err)
		}

		err = d.AppendWatchTx(keys, appInfo.tablesToWatch)
		if err != nil {
			resp.ErrSrc = AppErr
			return resp, abortAll(redisMdbInstances, err)
		}

		d.Opts.DisableCVLCheck = true
		err = appCVLValidate(*app, d)
		d.Opts.DisableCVLCheck = false
		if err != nil {
			log.Errorf("Delete: PHASE 1 - CVL FAILED for namespace=%s, aborting all, error=%v", nameSpace, err)
			resp.ErrSrc = AppErr
			return resp, abortAll(redisMdbInstances, err)
		}
		log.Infof("Delete: PHASE 1 - CVL passed for namespace=%s, queuing for Phase 2", nameSpace)

		writeContexts = append(writeContexts, nsWriteContext{
			nsPayload: nsPayload,
			d:         d,
		})
	}

	log.Infof("Delete: PHASE 1 complete - CVL passed for all namespaces, total=%d, proceeding to PHASE 2", len(writeContexts))

	log.Infof("Delete: Starting PHASE 2 - actual Redis writes")

	for _, ctx := range writeContexts {
		ctx.d.SetSkipCVL(true)
		log.Infof("Delete: PHASE 2 - writing namespace=%s", ctx.nsPayload.Namespace)

		opts := appOptions{deleteEmptyEntry: req.DeleteEmptyEntry}
		err = appInitialize(app, appInfo, path, nil, &opts, DELETE)
		if err != nil {
			resp.ErrSrc = AppErr
			return resp, err
		}

		_, err = (*app).translateDelete(ctx.d)
		if err != nil {
			resp.ErrSrc = AppErr
			return resp, abortAll(redisMdbInstances, err)
		}

		if ENABLE_TRANSLIB_ROLLBACK == "true" {
			log.Infof("Delete: PHASE 2 - Calling processPreparePhase for namespace=%s", ctx.nsPayload.Namespace)
			ctx.d.Opts.DisableCVLCheck = true
			err = appPreparePhase(*app, ctx.d, ctx.nsPayload.Key)
			ctx.d.Opts.DisableCVLCheck = false
			if err != nil {
				resp.ErrSrc = AppErr
				return resp, abortAll(redisMdbInstances, err)
			}

		}
		resp, err = (*app).processDelete(ctx.d)
		if err != nil {
			resp.ErrSrc = AppErr
			return resp, abortAll(redisMdbInstances, err)
		}

	}

	log.Infof("Delete: PHASE 2 complete - all namespaces written successfully, proceeding to commit")

	return resp, commitWithRollback(redisMdbInstances, namespacePayloads,
		func(_ string, d *db.DB) error { return appRollback(*app, d) })
}

// Get - Gets data from the redis DB and converts it to northbound format
func Get(req GetRequest) ([]GetResponse, error) {
	var payload []byte
	var allResp []GetResponse
	var resp GetResponse
	var mdb map[string][db.MaxDB]*db.DB
	allErrors := true
	var errorSrc ErrSource
	var nameSpaces []string

	path := req.Path
	if !isAuthorizedForGet(req) {
		return allResp, tlerr.AuthorizationError{
			Format: "User is unauthorized for Get Operation",
			Path:   path,
		}
	}

	log.Info("Received Get request for path = ", path)

	app, appInfo, err := getAppModule(path, req.ClientVersion)

	if err != nil {
		return []GetResponse{{Payload: payload, ErrSrc: ProtoErr}}, err
	}

	opts := appOptions{depth: req.QueryParams.Depth, content: req.QueryParams.Content, fields: req.QueryParams.Fields, ctxt: req.Ctxt}
	err = appInitialize(app, appInfo, path, nil, &opts, GET)

	if err != nil {
		return []GetResponse{{Payload: payload, ErrSrc: AppErr}}, err
	}

	namespacePayloads, err := getAppNamespace(*app, path)

	if err != nil {
		log.Infof("Error from getNamespace %v", err)
		return []GetResponse{{Payload: payload, ErrSrc: AppErr}}, err
	}

	// Fetching the DBNames to iterate if getNamespace returned *
	// if keys is not present in xpath of GetRequest.
	if len(namespacePayloads) == 1 && namespacePayloads[0].Namespace == "*" {
		nameSpaces = db.GetMultiDbNames()
	} else {
		nameSpaces = append(nameSpaces, namespacePayloads[0].Namespace) //Specfic key Get operation.
	}

	mdb, err = getAllMdbs(withWriteDisable)

	if err != nil {
		return []GetResponse{{Payload: payload, ErrSrc: ProtoErr}}, err
	}

	defer closeAllMdbs(mdb)

	for _, nameSpace := range nameSpaces {

		opts := appOptions{depth: req.QueryParams.Depth, content: req.QueryParams.Content, fields: req.QueryParams.Fields, ctxt: req.Ctxt}
		err = appInitialize(app, appInfo, path, nil, &opts, GET)

		if err != nil {
			return []GetResponse{{Payload: payload, ErrSrc: AppErr}}, err
		}

		err = (*app).translateGet(mdb[nameSpace])

		if err != nil {
			return []GetResponse{{Payload: payload, ErrSrc: AppErr}}, err
		}

		log.Infof("Process Get for nameSpace:%v ", nameSpace)

		resp, err = (*app).processGet(mdb[nameSpace], req.FmtType)

		if len(resp.Payload) > 0 && err == nil {
			// Unmarshal the payload to check if it's an empty object
			var jsonObj map[string]interface{}
			err := json.Unmarshal(resp.Payload, &jsonObj)
			if err != nil {
				log.Errorf("Error unmarshalling response payload: %v", err)
				return allResp, err
			}

			// Check if the payload is an empty JSON object
			if len(jsonObj) > 0 {
				// Only append if the response is not an empty JSON object
				allResp = append(allResp, GetResponse{
					Payload:   resp.Payload,
					ValueTree: resp.ValueTree,
					ErrSrc:    resp.ErrSrc,
				})
				log.Infof("\n ProcessGet Response appending: %v", resp.Payload)
				log.Infof("\n ProcessGet Response after appending: %v", allResp)
				allErrors = false
			} else {
				log.Infof("Empty JSON object received, not appending.")
			}

		} else {
			if err != nil {
				if err.Error() != "Resource not found" {
					return allResp, err
				}
				if errorSrc == 0 {
					errorSrc = resp.ErrSrc
				}
			}
		}
	}
	// If all processGet calls resulted in Resource not found,
	// update ErrSrc for each response
	if allErrors {
		for i := range allResp {
			allResp[i].ErrSrc = errorSrc
		}
	} else {
		// Since processGet didnt fail for all redis namesSpace
		err = nil
	}

	return allResp, err
}

func Action(req ActionRequest) ([]ActionResponse, error) {
	var payload []byte
	var allResp []ActionResponse
	var resp ActionResponse
	var nameSpaces []string

	path := req.Path

	if !isAuthorizedForAction(req) {
		return allResp, tlerr.AuthorizationError{
			Format: "User is unauthorized for Action Operation",
			Path:   path,
		}
	}

	log.Info("Received Action request for path = ", path)

	app, appInfo, err := getAppModule(path, req.ClientVersion)

	if err != nil {
		return []ActionResponse{{Payload: payload, ErrSrc: ProtoErr}}, err
	}

	aInfo := *appInfo

	aInfo.isNative = true

	err = appInitialize(app, &aInfo, path, &req.Payload, nil, GET)

	if err != nil {
		return []ActionResponse{{Payload: payload, ErrSrc: AppErr}}, err
	}

	namespacePayloads, err := getAppNamespace(*app, path)

	if err != nil {
		log.Infof("Error from getNamespace %v", err)
		return []ActionResponse{{Payload: payload, ErrSrc: AppErr}}, err
	}

	// Fetching the DBNames to iterate if getNamespace returned *
	// if keys is not present in xpath of GetRequest.
	if len(namespacePayloads) == 1 && namespacePayloads[0].Namespace == "*" {
		nameSpaces = db.GetMultiDbNames()
	} else {
		for _, nsPayload := range namespacePayloads {
			nameSpaces = append(nameSpaces, nsPayload.Namespace)
		}
	}

	writeMutex.Lock()
	defer writeMutex.Unlock()

	mdb, err := getAllMdbs()

	if err != nil {
		return []ActionResponse{{Payload: payload, ErrSrc: ProtoErr}}, err
	}

	defer closeAllMdbs(mdb)

	for _, nameSpace := range nameSpaces {

		err = (*app).translateAction(mdb[nameSpace])

		if err != nil {
			return []ActionResponse{{Payload: payload, ErrSrc: AppErr}}, err
		}

		resp, err = (*app).processAction(mdb[nameSpace])

		if len(resp.Payload) > 0 && err == nil {
			allResp = append(allResp, ActionResponse{
				Payload: resp.Payload,
				ErrSrc:  resp.ErrSrc,
			})
		}
	}

	return allResp, err
}

// Bulk - BULK Request API for northbounds
// Processes the request in received order
// Transaction based
func Bulk(req BulkRequest) (BulkResponse, error) {
	type bulkRequestContext struct {
		app          *appInterface
		appInfo      *appInfo
		operation    int
		reqIndex     int
		nsPayload    NamespacePayload
		payloadBytes []byte
		d            *db.DB
	}

	var err error
	var keys []db.WatchKeys
	var errSrc ErrSource
	var appResp SetResponse
	var namespacePayloads []NamespacePayload

	resp := BulkResponse{}
	redisMdbInstances := make(map[string]*db.DB)
	var writeContexts []bulkRequestContext

	if !isAuthorizedForBulk(req) {
		return resp, tlerr.AuthorizationError{
			Format: "User is unauthorized for Action Operation",
		}
	}

	writeMutex.Lock()
	defer writeMutex.Unlock()
	defer closeMdbInstances(redisMdbInstances)

	resp.Response = make([]BulkResponseEntry, len(req.Request))

	// bulkFail records the failure for request idx and aborts all namespaces.
	bulkFail := func(idx int, err error) error {
		log.Infof("BulkError: %+v", err)
		appResp.Err = err
		appResp.ErrSrc = errSrc
		resp.Response[idx].Entry = appResp

		if abortErr := abortTransactions(redisMdbInstances); abortErr != nil {
			appResp.Err = abortErr
			resp.Response[idx].Entry = appResp
			return abortErr
		}
		return err
	}

	log.Infof("Bulk: Starting PHASE 1 - CVL validation for all namespaces, total=%d", len(req.Request))

	for i := range req.Request {
		path := req.Request[i].Entry.Path
		operation := req.Request[i].Operation
		payload := req.Request[i].Entry.Payload
		resp.Response[i].Operation = operation

		log.Infof("Bulk Request operation: %v received with path = %v", operation, path)

		app, appInfo, err := getAppModule(path, req.Request[i].Entry.ClientVersion)
		if err != nil {
			errSrc = ProtoErr
			goto BulkError
		}
		if operation == DELETE {
			opts := appOptions{deleteEmptyEntry: req.Request[i].Entry.DeleteEmptyEntry}
			err = appInitialize(app, appInfo, path, nil, &opts, operation)
		} else {
			err = appInitialize(app, appInfo, path, &payload, nil, operation)
		}

		if err != nil {
			errSrc = AppErr
			goto BulkError
		}

		namespacePayloads, err = getAppNamespace(*app, path)
		if err != nil {
			errSrc = AppErr
			goto BulkError
		}

		if operation == DELETE {
			namespacePayloads = expandAllNamespaces(namespacePayloads)
		}

		if operation == REPLACE && len(namespacePayloads) == 1 && namespacePayloads[0].Namespace == "*" {
			err = tlerr.InvalidArgsError{Format: "Replace payload is empty", Path: path}
			errSrc = AppErr
			goto BulkError
		}

		for _, nsPayload := range namespacePayloads {
			nameSpace := nsPayload.Namespace
			payloads := nsPayload.Payloads

			var payloadBytes []byte

			var d *db.DB
			d, err = initializeMDBInstance(nameSpace, redisMdbInstances)
			if err != nil {
				errSrc = ProtoErr
				goto BulkError
			}

			if operation == DELETE {
				keys, err = (*app).translateDelete(d)

				if err != nil {
					errSrc = AppErr
					goto BulkError
				}

				err = d.AppendWatchTx(keys, appInfo.tablesToWatch)
				if err != nil {
					errSrc = AppErr
					goto BulkError
				}
				log.Infof("Bulk: PHASE 1 - AppendWatchTx done for namespace=%s", nameSpace)

				d.Opts.DisableCVLCheck = true
				err = appCVLValidate(*app, d)
				d.Opts.DisableCVLCheck = false
				if err != nil {
					log.Errorf("Bulk: PHASE 1 - CVL FAILED for namespace=%s, aborting all, error=%v", nameSpace, err)
					errSrc = AppErr
					goto BulkError
				}
				log.Infof("Bulk: PHASE 1 - CVL passed for namespace=%s, queuing for Phase 2", nameSpace)

				writeContexts = append(writeContexts, bulkRequestContext{
					app:          app,
					appInfo:      appInfo,
					operation:    operation,
					reqIndex:     i,
					nsPayload:    nsPayload,
					payloadBytes: payloadBytes,
					d:            d,
				})
			} else {
				if len(payloads) == 0 {
					log.Infof("Payloads for namespace '%s' are empty, using top-level payload", nameSpace)

					var topLevelPayload map[string]interface{}
					err = json.Unmarshal(payload, &topLevelPayload)
					if err != nil {
						log.Errorf("Failed to unmarshal top-level payload: %v", err)
						errSrc = ProtoErr
						goto BulkError
					}

					payloads = append(payloads, topLevelPayload)
				}

				for _, p := range payloads {
					operation := operation
					payloadBytes, err = json.Marshal(p)
					if err != nil {
						errSrc = AppErr
						goto BulkError
					}
					err = appInitialize(app, appInfo, path, &payloadBytes, nil, operation)
					if err != nil {
						errSrc = AppErr
						goto BulkError
					}

					switch operation {
					case CREATE:
						keys, err = (*app).translateCreate(d)
					case UPDATE:
						keys, err = (*app).translateUpdate(d)
						if err != nil && isBulkNotFoundError(err) {
							log.V(2).Infof("Since UPDATE Failed, Changing operation type to REPLACE")
							operation = REPLACE
							err = appInitialize(app, appInfo, path, &payloadBytes, nil, operation)
							if err != nil {
								errSrc = AppErr
								goto BulkError
							}
							keys, err = (*app).translateReplace(d)
						}
					case REPLACE:
						keys, err = (*app).translateReplace(d)
					default:
						err = tlerr.NotSupported("Unknown operation '%v'", operation)
					}

					if err != nil {
						errSrc = AppErr
						goto BulkError
					}

					err = d.AppendWatchTx(keys, appInfo.tablesToWatch)
					if err != nil {
						errSrc = AppErr
						goto BulkError
					}

					err = appCVLValidate(*app, d)
					if err != nil {
						log.Errorf("Bulk: PHASE 1 - CVL FAILED for namespace=%s, aborting all, error=%v", nameSpace, err)
						errSrc = AppErr
						goto BulkError
					}
					log.Infof("Bulk: PHASE 1 - CVL passed for namespace=%s, queuing for Phase 2", nameSpace)

					writeContexts = append(writeContexts, bulkRequestContext{
						app:          app,
						appInfo:      appInfo,
						operation:    operation,
						reqIndex:     i,
						nsPayload:    nsPayload,
						payloadBytes: payloadBytes,
						d:            d,
					})
				}
			}
		}

		continue

	BulkError:
		return resp, bulkFail(i, err)
	}

	log.Infof("Bulk: PHASE 1 complete - CVL passed for all namespaces, total=%d, proceeding to PHASE 2", len(writeContexts))

	log.Infof("Bulk: Starting PHASE 2 - actual Redis writes")

	for _, ctx := range writeContexts {
		ctx.d.SetSkipCVL(true)
		log.Infof("Bulk: PHASE 2 - writing namespace=%s", ctx.nsPayload.Namespace)

		path := req.Request[ctx.reqIndex].Entry.Path

		// Re-init and re-translate for this specific context's payload —
		// app is shared across all payload items within the same request
		// index, so its internal state must be repopulated per-context
		// before writing, otherwise only the last Phase-1 iteration's
		// state would get written repeatedly.
		if ctx.operation == DELETE {
			opts := appOptions{deleteEmptyEntry: req.Request[ctx.reqIndex].Entry.DeleteEmptyEntry}
			err = appInitialize(ctx.app, ctx.appInfo, path, nil, &opts, DELETE)
		} else {
			err = appInitialize(ctx.app, ctx.appInfo, path, &ctx.payloadBytes, nil, ctx.operation)
		}
		if err != nil {
			errSrc = AppErr
			goto BulkError2
		}

		switch ctx.operation {
		case DELETE:
			_, err = (*ctx.app).translateDelete(ctx.d)
		case CREATE:
			_, err = (*ctx.app).translateCreate(ctx.d)
		case UPDATE:
			_, err = (*ctx.app).translateUpdate(ctx.d)
		case REPLACE:
			_, err = (*ctx.app).translateReplace(ctx.d)
		}
		if err != nil {
			errSrc = AppErr
			goto BulkError2
		}

		if ENABLE_TRANSLIB_ROLLBACK == "true" {
			ctx.d.Opts.DisableCVLCheck = true
			err = appPreparePhase(*ctx.app, ctx.d, ctx.nsPayload.Key)
			ctx.d.Opts.DisableCVLCheck = false
			if err != nil {
				errSrc = AppErr
				goto BulkError2
			}
		}

		switch ctx.operation {
		case DELETE:
			appResp, err = (*ctx.app).processDelete(ctx.d)
		case CREATE:
			appResp, err = (*ctx.app).processCreate(ctx.d)
		case UPDATE:
			appResp, err = (*ctx.app).processUpdate(ctx.d)
		case REPLACE:
			appResp, err = (*ctx.app).processReplace(ctx.d)
		}

		if err != nil {
			errSrc = AppErr
			goto BulkError2
		}

		resp.Response[ctx.reqIndex].Entry = appResp
		continue

	BulkError2:
		return resp, bulkFail(ctx.reqIndex, err)
	}

	log.Infof("Bulk: PHASE 2 complete - all namespaces written successfully, proceeding to commit")

	var allNamespacePayloads []NamespacePayload
	for _, ctx := range writeContexts {
		exists := false
		for _, payload := range allNamespacePayloads {
			if payload.Namespace == ctx.nsPayload.Namespace {
				exists = true
				break
			}
		}
		if !exists {
			allNamespacePayloads = append(allNamespacePayloads, ctx.nsPayload)
		}
	}

	return resp, commitWithRollback(redisMdbInstances, allNamespacePayloads,
		func(ns string, d *db.DB) error {
			for _, ctx := range writeContexts {
				if ctx.nsPayload.Namespace == ns {
					return appRollback(*ctx.app, d)
				}
			}
			return nil
		})
}

// GetModels - Gets all the models supported by Translib
func GetModels() ([]ModelData, error) {
	var err error

	return getModels(), err
}

// Creates connection will all the redis DBs. To be used for get request
func getAllMdbs(opts ...func(*db.Options)) (map[string][db.MaxDB]*db.DB, error) {
	var dbs [db.MaxDB]*db.DB
	var err error
	mdbNames := db.GetMultiDbNames()
	if len(mdbNames) == 0 {
		return nil, errors.New("get all db names failed")
	}

	mdb := make(map[string][db.MaxDB]*db.DB)
	for _, nameSpace := range mdbNames {

		for dbNum := db.DBNum(0); dbNum < db.MaxDB; dbNum++ {
			if len(dbNum.Name()) == 0 {
				continue
			}
			// Pass the nameSpace to SetMDBName and other opts to getDBOptions
			dbs[dbNum], err = db.NewDB(getDBOptions(dbNum, append(opts, SetMDBName(nameSpace))...))
			if err != nil {
				closeAllDbs(dbs[:])
				break
			}
		}
		mdb[nameSpace] = dbs
	}
	return mdb, err
}

// Closes the dbs, and nils out the arr.
func closeAllDbs(dbs []*db.DB) {
	for dbsi, d := range dbs {
		if d != nil {
			if err := d.DeleteDB(); err != nil {
				log.Infof("Failed to delete DB %d: %v", dbsi, err)
			}

			dbs[dbsi] = nil
		}
	}
}

// Closes the multiple dbs for multi_asic, and nils out the arr.
func closeAllMdbs(mdb map[string][db.MaxDB]*db.DB) {
	for name, db := range mdb {
		if db[:] != nil {
			closeAllDbs(db[:])
		}
		delete(mdb, name)
	}
}

// Compare - Implement Compare method for priority queue for SubscribeResponse struct
func (val SubscribeResponse) Compare(other queue.Item) int {
	o := other.(*SubscribeResponse)
	if val.Timestamp > o.Timestamp {
		return 1
	} else if val.Timestamp == o.Timestamp {
		return 0
	}
	return -1
}

func getDBOptions(dbNo db.DBNum, opts ...func(*db.Options)) db.Options {
	o := db.Options{DBNo: dbNo}
	for _, setopt := range opts {
		setopt(&o)
	}
	switch dbNo {
	case db.ApplDB, db.CountersDB, db.AsicDB, db.FlexCounterDB, db.LogLevelDB, db.ErrorDB:
		o.TableNameSeparator = ":"
		o.KeySeparator = ":"
	case db.ConfigDB, db.StateDB, db.SnmpDB:
		o.TableNameSeparator = "|"
		o.KeySeparator = "|"
	}

	// If MDBName is not set, use the default nameSpace value
	if o.MDBName == "" {
		o.MDBName = "host"
	}

	return o
}

// Define a new function to set the MDBName in Options
func SetMDBName(nameSpace string) func(*db.Options) {
	return func(o *db.Options) {
		o.MDBName = nameSpace
	}
}

func withWriteDisable(o *db.Options) {
	o.IsWriteDisabled = true
}

func withOnChange(o *db.Options) {
	o.IsOnChangeEnabled = true
}
func withForceNewRedisConnection(o *db.Options) {
	o.ForceNewRedisConnection = true
}

func getAppModule(path string, clientVer Version) (*appInterface, *appInfo, error) {
	var app appInterface

	aInfo, err := getAppModuleInfo(path)

	if err != nil {
		return nil, aInfo, err
	}

	if err := validateClientVersion(clientVer, path, aInfo); err != nil {
		return nil, aInfo, err
	}

	app, err = getAppInterface(aInfo.appType)

	if err != nil {
		return nil, aInfo, err
	}

	return &app, aInfo, err
}

func appInitialize(app *appInterface, appInfo *appInfo, path string, payload *[]byte, opts *appOptions, opCode int) error {
	var err error
	var input []byte

	if payload != nil {
		input = *payload
	}

	if appInfo.isNative {
		data := appData{path: path, payload: input}
		data.setOptions(opts)
		(*app).initialize(data)
	} else {
		reqBinder := getRequestBinder(&path, payload, opCode, &(appInfo.ygotRootType))
		ygotStruct, ygotTarget, err := reqBinder.unMarshall()
		if err != nil {
			log.Info("Error in request binding: ", err)
			return err
		}
		data := appData{path: path, payload: input, ygotRoot: ygotStruct, ygotTarget: ygotTarget, ygSchema: reqBinder.targetNodeSchema}
		data.setOptions(opts)
		(*app).initialize(data)
	}

	return err
}

func (data *appData) setOptions(opts *appOptions) {
	if opts != nil {
		data.appOptions = *opts
	}
}
