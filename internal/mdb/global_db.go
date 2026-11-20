package mdb

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// DbGlobal mirrors database_global.json (sonic multi-DB layout).
type DbGlobal struct {
	Includes     []DbInclude `json:"INCLUDES"`
	Version      string      `json:"VERSION"`
	KeySeperator string      `json:"KEY_SEPERATOR"`
}

// DbInclude is one entry in database_global.json INCLUDES.
type DbInclude struct {
	Include       string `json:"include"`
	Namespace     string `json:"namespace"`
	ContainerName string `json:"container_name"`
}

// ConfigPathsForInit returns namespace → database_config.json paths for translib/CVL init.
// Single-DB (numAsic <= 1): only host uses hostConfigPath.
// Multi-DB: paths are read from globalConfigPath (database_global.json).
func ConfigPathsForInit(numAsic int, hostConfigPath, globalConfigPath string) (map[string]string, string, error) {
	if !IsMultiAsic(numAsic) {
		return map[string]string{HostNamespace: hostConfigPath}, "", nil
	}
	return loadGlobalDatabasePaths(globalConfigPath)
}

func loadGlobalDatabasePaths(globalDbFilePath string) (map[string]string, string, error) {
	data, err := os.ReadFile(globalDbFilePath)
	if err != nil {
		return nil, "", err
	}

	var globalDbCfg DbGlobal
	if err := json.Unmarshal(data, &globalDbCfg); err != nil {
		return nil, "", err
	}

	dbConfigMap := make(map[string]string)
	pwd := filepath.Dir(globalDbFilePath) + string(os.PathSeparator)
	for i := range globalDbCfg.Includes {
		inc := globalDbCfg.Includes[i]
		absPath := pwd + inc.Include
		if i == 0 {
			dbConfigMap[HostNamespace] = absPath
			continue
		}
		ns := inc.Namespace
		if ns == "" {
			return nil, "", fmt.Errorf("missing namespace for include %q in %s", inc.Include, globalDbFilePath)
		}
		dbConfigMap[ns] = absPath
	}
	return dbConfigMap, globalDbCfg.KeySeperator, nil
}
