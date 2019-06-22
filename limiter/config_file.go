package limiter

import (
	"io/ioutil"
	"os"
	"sync"

	"github.com/pkg/errors"
)

//iConfigFile interface provides abstraction for managing
type iConfigFile interface {
	load() ([]byte, error)
	write(output []byte) error
	remove() error
	isValid() bool
}

//configFile allows to manage configuration file for request block header rules
type configFile struct {
	fileLock            sync.Mutex
	blockRuleConfigName string
}

//NewConfigFile creates config file instance
func newConfigFile(blockHeaderFile string) iConfigFile {
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

//remove removes block rules config file
func (cf *configFile) remove() error {
	return os.Remove(cf.blockRuleConfigName)
}

//isValid checks if file can be used to store rules
func (cf *configFile) isValid() bool {
	return cf.blockRuleConfigName != ""
}

//write saves rules to file
func (cf *configFile) write(output []byte) error {
	cf.fileLock.Lock()
	defer cf.fileLock.Unlock()
	err := ioutil.WriteFile(cf.blockRuleConfigName, output, 0644)
	return err
}
