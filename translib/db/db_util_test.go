package db

import "testing"

func TestGetMDBNameFromOtnOpticalEntity(t *testing.T) {
	NumAsic = 4
	multiDbsConfigMap = map[string]map[string]interface{}{
		"asic0": {},
		"host":  {},
	}

	tests := map[string]string{
		"OA0-0":                      "asic0",
		"OA1-0":                      "asic1",
		"VOA0-1":                     "asic0",
		"OCM0-4":                     "asic0",
		"OCM1-0|191262500|191337500": "asic1",
		"AMPLIFIER-1-2-3":            "asic1",
		"ATTENUATOR-1-3-1":           "asic2",
		"SLOT-2":                     "asic1",
	}
	for name, want := range tests {
		if got := GetMDBNameFromOtnOpticalEntity(name); got != want {
			t.Errorf("%s: got %q want %q", name, got, want)
		}
	}
}

func TestGetMDBNameFromEntity_noOtnCompact(t *testing.T) {
	NumAsic = 4
	multiDbsConfigMap = map[string]map[string]interface{}{
		"asic0": {},
		"host":  {},
	}

	if got := GetMDBNameFromEntity("APS-1-2-1"); got != "asic1" {
		t.Fatalf("APS-1-2-1: got %q want asic1", got)
	}
	if got := GetMDBNameFromEntity("OA0-0"); got != "host" {
		t.Fatalf("OA0-0: got %q want host", got)
	}
}
