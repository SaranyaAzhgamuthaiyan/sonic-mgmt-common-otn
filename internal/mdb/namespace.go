package mdb

import "strconv"

// AsicPrefixFromMapKeys derives the ASIC DB name prefix (e.g. "asic" from "asic0").
func AsicPrefixFromMapKeys(namespaceKeys []string) string {
	for _, key := range namespaceKeys {
		if key != HostNamespace && len(key) > 0 {
			return key[:len(key)-1]
		}
	}
	return ""
}

// DBNameBySlot maps a 1-based slot number to a multi-DB namespace name.
func DBNameBySlot(s Settings, slotNum int) string {
	if s.NumAsic == 1 || slotNum > s.NumAsic || slotNum < 1 {
		return HostNamespace
	}
	return s.AsicPrefix + strconv.Itoa(slotNum-1)
}

// MultiDBNames returns host plus asic0..asic(N-1) when NumAsic > 1.
func MultiDBNames(s Settings) []string {
	if s.NumAsic == 1 {
		return []string{HostNamespace}
	}
	dbNames := []string{HostNamespace}
	for num := 0; num < s.NumAsic; num++ {
		dbNames = append(dbNames, s.AsicPrefix+strconv.Itoa(num))
	}
	return dbNames
}
