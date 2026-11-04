package mdb

import (
	"os"
	"path/filepath"
	"testing"
)

func TestConfigPathsForInit_singleAsic(t *testing.T) {
	paths, sep, err := ConfigPathsForInit(1, "/tmp/host/database_config.json", "")
	if err != nil {
		t.Fatal(err)
	}
	if sep != "" {
		t.Fatalf("key separator want empty, got %q", sep)
	}
	if len(paths) != 1 || paths[HostNamespace] != "/tmp/host/database_config.json" {
		t.Fatalf("paths = %v", paths)
	}
}

func TestConfigPathsForInit_multiFromFile(t *testing.T) {
	dir := t.TempDir()
	globalPath := filepath.Join(dir, "database_global.json")
	content := `{
		"INCLUDES": [
			{"include": "../../redis/sonic-db/database_config.json"},
			{"namespace": "asic0", "include": "../../redis0/sonic-db/database_config.json"}
		],
		"VERSION": "1.0",
		"KEY_SEPERATOR": "-"
	}`
	if err := os.WriteFile(globalPath, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}

	paths, sep, err := ConfigPathsForInit(4, "", globalPath)
	if err != nil {
		t.Fatal(err)
	}
	if sep != "-" {
		t.Fatalf("key separator want -, got %q", sep)
	}
	pwd := filepath.Dir(globalPath) + string(os.PathSeparator)
	wantHost := pwd + "../../redis/sonic-db/database_config.json"
	wantAsic0 := pwd + "../../redis0/sonic-db/database_config.json"
	if paths[HostNamespace] != wantHost {
		t.Errorf("host path got %q want %q", paths[HostNamespace], wantHost)
	}
	if paths["asic0"] != wantAsic0 {
		t.Errorf("asic0 path got %q want %q", paths["asic0"], wantAsic0)
	}
}

func TestIsMultiAsic(t *testing.T) {
	if IsMultiAsic(1) {
		t.Fatal("num_asic=1 must not use multi-DB global config path")
	}
	if !IsMultiAsic(2) {
		t.Fatal("num_asic=2 must use multi-DB global config path")
	}
}
