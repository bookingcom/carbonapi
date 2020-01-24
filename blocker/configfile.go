package blocker

import (
	"io/ioutil"
	"os"
	"sync"

	"github.com/pkg/errors"
)

//configFileManager interface provides abstraction for managing config files
type configFileManager interface {
	load() ([]byte, error)
	write(output []byte) error
}

//configFile allows to manage configuration file for request block header rules
type configFile struct {
	fileLock            sync.Mutex
	blockRuleConfigName string
}

//NewConfigFile creates config file instance
func newConfigFile(blockHeaderFile string) configFileManager {
	return &configFile{
		blockRuleConfigName: blockHeaderFile,
	}
}

//load loads contents of block rule config file
func (cf *configFile) load() ([]byte, error) {
	cf.fileLock.Lock()
	defer cf.fileLock.Unlock()
	if _, err := os.Stat(cf.blockRuleConfigName); err == nil {
		return ioutil.ReadFile(cf.blockRuleConfigName)
	} else if os.IsNotExist(err) {
		return []byte{}, nil
	} else {
		return []byte{}, errors.Wrap(err, "error while checking existense of file")
	}
}

//write saves rules to file
func (cf *configFile) write(output []byte) error {
	cf.fileLock.Lock()
	defer cf.fileLock.Unlock()
	err := ioutil.WriteFile(cf.blockRuleConfigName, output, 0644)
	return err
}
