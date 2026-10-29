package mdb

import "testing"

func TestNamespaceFromOtnOpticalEntity(t *testing.T) {
	s := Settings{
		NumAsic:      4,
		KeySeparator: DefaultKeySeparator,
		AsicPrefix:   "asic",
	}

	tests := map[string]string{
		"OA0-0":                      "asic0",
		"OA1-0":                      "asic1",
		"VOA0-1":                     "asic0",
		"OCM0-4":                     "asic0",
		"OCM1-0|191262500|191337500": "asic1",
		"OSC2-0":                     "asic2",
		"OTDR3":                      "asic3",
		"AMPLIFIER-1-2-3":            "asic1",
		"ATTENUATOR-1-3-1":           "asic2",
		"SLOT-2":                     "asic1",
	}
	for name, want := range tests {
		if got := NamespaceFromOtnOpticalEntity(s, name); got != want {
			t.Errorf("%s: got %q want %q", name, got, want)
		}
	}
}

func TestNamespaceFromEntity_noOtnCompact(t *testing.T) {
	s := Settings{
		NumAsic:      4,
		KeySeparator: DefaultKeySeparator,
		AsicPrefix:   "asic",
	}

	if got := NamespaceFromEntity(s, "APS-1-2-1"); got != "asic1" {
		t.Fatalf("APS-1-2-1: got %q want asic1", got)
	}
	if got := NamespaceFromEntity(s, "OA0-0"); got != HostNamespace {
		t.Fatalf("OA0-0: got %q want host", got)
	}
}
