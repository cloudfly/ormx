package cache

import (
	"fmt"
	"os"
	"path"
	"strconv"
	"strings"
)

// GetMemoryLimit returns cgroup memory limit
func GetMemoryLimit() int64 {
	// Try determining the amount of memory inside docker container.
	// See https://stackoverflow.com/questions/42187085/check-mem-limit-within-a-docker-container
	//
	// Read memory limit according to https://unix.stackexchange.com/questions/242718/how-to-find-out-how-much-memory-lxc-container-is-allowed-to-consume
	// This should properly determine the limit inside lxc container.
	// See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/84
	n, err := getMemStat("memory.limit_in_bytes")
	if err == nil {
		return n
	}
	n, err = getMemStatV2("memory.max")
	if err != nil {
		return 0
	}
	return n
}

func getMemStatV2(statName string) (int64, error) {
	// See https: //www.kernel.org/doc/html/latest/admin-guide/cgroup-v2.html#memory-interface-files
	return getStatGeneric(statName, "/sys/fs/cgroup", "/proc/self/cgroup", "")
}

func getMemStat(statName string) (int64, error) {
	return getStatGeneric(statName, "/sys/fs/cgroup/memory", "/proc/self/cgroup", "memory")
}

func getStatGeneric(statName, sysfsPrefix, cgroupPath, cgroupGrepLine string) (int64, error) {
	data, err := getFileContents(statName, sysfsPrefix, cgroupPath, cgroupGrepLine)
	if err != nil {
		return 0, err
	}
	data = strings.TrimSpace(data)
	n, err := strconv.ParseInt(data, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("cannot parse %q: %w", cgroupPath, err)
	}
	return n, nil
}

func getFileContents(statName, sysfsPrefix, cgroupPath, cgroupGrepLine string) (string, error) {
	filepath := path.Join(sysfsPrefix, statName)
	data, err := os.ReadFile(filepath)
	if err == nil {
		return string(data), nil
	}
	cgroupData, err := os.ReadFile(cgroupPath)
	if err != nil {
		return "", err
	}
	subPath, err := grepFirstMatch(string(cgroupData), cgroupGrepLine, 2, ":")
	if err != nil {
		return "", fmt.Errorf("cannot find cgroup path for %q in %q: %w", cgroupGrepLine, cgroupPath, err)
	}
	filepath = path.Join(sysfsPrefix, subPath, statName)
	data, err = os.ReadFile(filepath)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// grepFirstMatch searches match line at data and returns item from it by index with given delimiter.
func grepFirstMatch(data string, match string, index int, delimiter string) (string, error) {
	lines := strings.Split(string(data), "\n")
	for _, s := range lines {
		if !strings.Contains(s, match) {
			continue
		}
		parts := strings.Split(s, delimiter)
		if index < len(parts) {
			return strings.TrimSpace(parts[index]), nil
		}
	}
	return "", fmt.Errorf("cannot find %q in %q", match, data)
}
