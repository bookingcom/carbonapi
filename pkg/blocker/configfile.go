package blocker

import (
	"fmt"
	"os"
	"sync"
)

// configFileManager interface provides abstraction for managing config files
type configFileManager interface {
	load() ([]byte, error)
	write(output []byte) error
}

// configFile allows to manage configuration file for request block header rules
type configFile struct {
	fileLock            sync.Mutex
	blockRuleConfigName string
}

// NewConfigFile creates config file instance
func newConfigFile(blockHeaderFile string) configFileManager {
	return &configFile{
		blockRuleConfigName: blockHeaderFile,
	}
}

// load loads contents of block rule config file
func (cf *configFile) load() ([]byte, error) {
	cf.fileLock.Lock()
	defer cf.fileLock.Unlock()
	if _, err := os.Stat(cf.blockRuleConfigName); err == nil {
		return os.ReadFile(cf.blockRuleConfigName)
	} else if os.IsNotExist(err) {
		return []byte{}, nil
	} else {
		return []byte{}, fmt.Errorf("error while checking existense of file: %w", err)
	}
}

// write saves rules to file
func (cf *configFile) write(output []byte) error {
	cf.fileLock.Lock()
	defer cf.fileLock.Unlock()
	err := os.WriteFile(cf.blockRuleConfigName, output, 0600)
	return err
}
