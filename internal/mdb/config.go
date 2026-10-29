package mdb

const (
	DefaultAsicConfPath  = "/usr/share/sonic/platform/asic.conf"
	DefaultKeySeparator  = "-"
	HostNamespace        = "host"
)

// Settings holds multi-DB namespace parameters shared by translib and CVL.
type Settings struct {
	NumAsic      int
	KeySeparator string
	AsicPrefix   string
}

func (s Settings) keySeparator() string {
	if s.KeySeparator == "" {
		return DefaultKeySeparator
	}
	return s.KeySeparator
}
