package mdb

import (
	"bufio"
	"os"
	"strconv"
	"strings"

	"github.com/golang/glog"
)

func IsMultiAsic(numAsic int) bool {
	return numAsic > 0
}

// ReadNumAsic parses num_asic= from the platform asic.conf file.
func ReadNumAsic(asicConfPath string) int {
	file, err := os.Open(asicConfPath)
	if err != nil {
		glog.Warning("Cannot find the asic.conf file, set num_asic to 1 by default")
		return 1
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	scanner.Split(bufio.ScanLines)
	numAsics := 1
	for scanner.Scan() {
		tokens := strings.Split(scanner.Text(), "=")
		if len(tokens) != 2 {
			continue
		}
		if strings.ToLower(tokens[0]) != "num_asic" {
			continue
		}
		num, err := strconv.Atoi(tokens[1])
		if err != nil {
			glog.Warning("invalid line in asic.config")
			continue
		}
		numAsics = num
	}
	return numAsics
}
