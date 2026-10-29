package mdb

import (
	"strconv"
	"strings"

	"github.com/golang/glog"
)

// NamespaceFromEntity resolves the multi-DB namespace for generic entity keys.
func NamespaceFromEntity(s Settings, entity interface{}) string {
	var slotNum int

	switch t := entity.(type) {
	case uint16:
		slotNum = int(t / 100)
	case uint32:
		slotNum = int(t / 100)
	case string:
		if sl, ok := SlotFromEntityName(s, t, false); ok {
			slotNum = sl
		}
	default:
		glog.Errorf("unexpected type %T", t)
	}

	return DBNameBySlot(s, slotNum)
}

// NamespaceFromOtnOpticalEntity maps optical entity names to asic namespaces.
func NamespaceFromOtnOpticalEntity(s Settings, entity interface{}) string {
	name, ok := entity.(string)
	if !ok {
		return NamespaceFromEntity(s, entity)
	}
	slotNum, _ := SlotFromEntityName(s, name, true)
	return DBNameBySlot(s, slotNum)
}

// SlotFromEntityName returns a 1-based slot (asic index = slot-1).
func SlotFromEntityName(s Settings, name string, allowOtnCompact bool) (int, bool) {
	sep := s.keySeparator()
	parts := strings.Split(name, sep)

	var slotStr string
	switch {
	case len(parts) == 2 && parts[0] == "SLOT":
		slotStr = parts[1]
	case len(parts) >= 3:
		slotStr = parts[2]
	default:
		if !allowOtnCompact {
			return 0, false
		}
		if slot, ok := slotFromCompactEntityName(name); ok {
			return slot, true
		}
		return 0, false
	}

	slot, err := strconv.Atoi(slotStr)
	if err != nil {
		return 0, false
	}
	return slot, true
}

// slotFromCompactEntityName parses OTN-style compact names: a letter prefix, a
// 0-based line-card index as digits, then optional "-..." or Redis key suffix.
// Examples: OA0-0, VOA0-1, OCM1-0|freq|freq, OSC2.
func slotFromCompactEntityName(name string) (int, bool) {
	base := name
	if i := strings.IndexByte(name, '|'); i >= 0 {
		base = name[:i]
	}
	i := 0
	for i < len(base) && isASCIILetter(base[i]) {
		i++
	}
	if i == 0 {
		return 0, false
	}
	j := i
	for j < len(base) && base[j] >= '0' && base[j] <= '9' {
		j++
	}
	if j == i {
		return 0, false
	}
	lc, err := strconv.Atoi(base[i:j])
	if err != nil {
		return 0, false
	}
	return lc + 1, true
}

func isASCIILetter(c byte) bool {
	return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z')
}
