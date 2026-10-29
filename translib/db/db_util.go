package db

import (
	"github.com/Azure/sonic-mgmt-common/internal/mdb"
	"github.com/golang/glog"
)

const (
	DEFAULT_KEY_SEPERATOR = mdb.DefaultKeySeparator
)

func isMultiAsic() bool {
	return mdb.IsMultiAsic(NumAsic)
}

func getNumAsic() int {
	return mdb.ReadNumAsic(DefaultAsicConfFilePath)
}

func fetchDbNameFromGlobalJson() string {
	keys := make([]string, 0, len(multiDbsConfigMap))
	for key := range multiDbsConfigMap {
		keys = append(keys, key)
	}
	return mdb.AsicPrefixFromMapKeys(keys)
}

func mdbSettings() mdb.Settings {
	return mdb.Settings{
		NumAsic:      NumAsic,
		KeySeparator: GlobalKeySeperator,
		AsicPrefix:   fetchDbNameFromGlobalJson(),
	}
}

func GetMultiDbNames() []string {
	return mdb.MultiDBNames(mdbSettings())
}

func getDbNameBySlotNum(slotNum int) string {
	return mdb.DBNameBySlot(mdbSettings(), slotNum)
}

func GetKeySeperatorFromGlobalJson() string {
	if GlobalKeySeperator == "" {
		GlobalKeySeperator = DEFAULT_KEY_SEPERATOR
	}
	glog.Info("GlobalKeySeperator:", GlobalKeySeperator)
	return GlobalKeySeperator
}

func GetMDBNameFromEntity(entity interface{}) string {
	s := mdbSettings()
	s.KeySeparator = GetKeySeperatorFromGlobalJson()
	return mdb.NamespaceFromEntity(s, entity)
}

func GetMDBNameFromOtnOpticalEntity(entity interface{}) string {
	s := mdbSettings()
	s.KeySeparator = GetKeySeperatorFromGlobalJson()
	return mdb.NamespaceFromOtnOpticalEntity(s, entity)
}
