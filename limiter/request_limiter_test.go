package limiter

import (
	"errors"
	"net/http"
	"testing"

	"github.com/lomik/zapwriter"
	"go.uber.org/zap"
	yaml "gopkg.in/yaml.v2"
)

type configFileMock struct {
	BlockRuleConfigName string
	BinToLoad           []byte
	IsRemoved           bool
	IsReplaced          bool
	BinReplace          []byte
	baseConfigFile      iConfigFile
	IsLoaded            bool

	ShouldFailOnWrite bool
	ShouldFailOnRead  bool
}

func newConfigFileMock(configFileName string, binToLoad []byte) *configFileMock {
	return &configFileMock{
		BlockRuleConfigName: configFileName,
		BinToLoad:           binToLoad,
		baseConfigFile:      newConfigFile(configFileName),
	}
}

func (cf *configFileMock) load() ([]byte, error) {
	if cf.ShouldFailOnRead {
		return []byte{}, errors.New("ShouldFailOnRead")
	}
	cf.IsLoaded = true
	return cf.BinToLoad, nil
}

func (cf *configFileMock) remove() error {
	cf.IsRemoved = true
	return nil
}

func (cf *configFileMock) isValid() bool {
	return cf.baseConfigFile.isValid()
}

func (cf *configFileMock) write(output []byte) error {
	if cf.ShouldFailOnWrite {
		return errors.New("ShouldFailOnWrite")
	}
	cf.IsReplaced = true
	cf.BinReplace = output
	return nil
}

func getTestLogger() *zap.Logger {
	return zapwriter.Logger("test")
}

func TestShouldNotBlock(t *testing.T) {
	req, err := http.NewRequest("GET", "nothing", nil)
	if err != nil {
		t.Error(err)
	}

	req.Header.Add("foo", "bar")
	r := Rule{"foo": "block"}

	blockrule := NewRequestLimiter("", 0, getTestLogger())
	blockrule.rules.Store(RuleConfig{Rules: []Rule{r}})

	if blockrule.ShouldBlockRequest(req) {
		t.Error("Should not have blocked this request")
	}
}

func TestShouldNotBlockWithoutRule(t *testing.T) {
	req, err := http.NewRequest("GET", "nothing", nil)
	if err != nil {
		t.Error(err)
	}

	req.Header.Add("foo", "bar")
	// no rules are set

	requestLimiter := NewRequestLimiter("", 0, getTestLogger())

	if requestLimiter.ShouldBlockRequest(req) {
		t.Error("Req should not be blocked")
	}
}

func TestShouldBlock(t *testing.T) {
	req, err := http.NewRequest("GET", "nothing", nil)
	if err != nil {
		t.Error(err)
	}

	req.Header.Add("foo", "bar")
	r := Rule{"foo": "bar"}

	requestLimiter := NewRequestLimiter("", 0, getTestLogger())
	requestLimiter.rules.Store(RuleConfig{Rules: []Rule{r}})

	if !requestLimiter.ShouldBlockRequest(req) {
		t.Error("Req should be blocked")
	}
}

func TestUnblockShouldDeleteFile(t *testing.T) {

	requestLimiter := NewRequestLimiter("", 0, getTestLogger())
	configMock := newConfigFileMock("", []byte{})
	requestLimiter.config = configMock
	requestLimiter.Unblock()
	if !configMock.IsRemoved {
		t.Error("Unblock should delete config file")
	}
}

func TestAddNewRulesForEmptyFileNameDoesNothing(t *testing.T) {

	fileName := ""
	requestLimiter := NewRequestLimiter(fileName, 0, getTestLogger())
	configMock := newConfigFileMock(fileName, []byte{})
	requestLimiter.config = configMock
	if requestLimiter.AddNewRules(make(map[string][]string)) {
		t.Error("Empty config file name should not be sufficient to save rules")
	}
}

func TestAddNewRulesEmptyParamsIgnored(t *testing.T) {

	fileName := "ConfigName.yaml"
	requestLimiter := NewRequestLimiter(fileName, 0, getTestLogger())
	configMock := newConfigFileMock(fileName, []byte{})
	requestLimiter.config = configMock

	params := make(map[string][]string)
	params[""] = []string{"nonValid"}
	params["x-non-valid"] = []string{""}
	if requestLimiter.AddNewRules(params) || configMock.IsReplaced {
		t.Error("When none of header/value pair contains non-empty elements, headers can't be saved")
	}
}

func TestAddNewRulesEmptyParamsWithNonEmptyParams_SavesNonEmpty(t *testing.T) {

	fileName := "ConfigName.yaml"
	requestLimiter := NewRequestLimiter(fileName, 0, getTestLogger())
	testConf := map[string]string{"header1": "value1", "header2": "value2"}
	initConfig := RuleConfig{Rules: []Rule{testConf}}
	retConfValues, err := yaml.Marshal(&initConfig)

	if err != nil {
		t.Error("Error when test config was preparing")
	}

	configMock := newConfigFileMock(fileName, retConfValues)
	requestLimiter.config = configMock

	params := make(map[string][]string)
	params[""] = []string{"nonValid"}
	params["x-non-valid"] = []string{""}
	params["x-valid"] = []string{"value"}
	if !requestLimiter.AddNewRules(params) || !configMock.IsLoaded || !configMock.IsReplaced {
		t.Error("When none of header/value pair contains non-empty elements, headers can't be saved")
	}

	var newRuleConfig RuleConfig
	marshErr := yaml.Unmarshal(configMock.BinReplace, &newRuleConfig)
	if marshErr != nil && len(newRuleConfig.Rules) != 2 {
		t.Error("Error while trying to umarshal updated config")
	}

	if (newRuleConfig.Rules[0]["header1"] != "value1") ||
		(newRuleConfig.Rules[0]["header2"] != "value2") ||
		(newRuleConfig.Rules[1]["x-valid"] != "value") {
		t.Error("Total saved header rules does not correct")
	}
}

func TestAddNewRulesFailOnWrite_ReturnsFalse(t *testing.T) {

	fileName := "ConfigName.yaml"
	requestLimiter := NewRequestLimiter(fileName, 0, getTestLogger())
	configMock := newConfigFileMock(fileName, []byte{})
	configMock.ShouldFailOnWrite = true
	requestLimiter.config = configMock

	params := make(map[string][]string)
	params[""] = []string{"nonValid"}
	params["x-valid"] = []string{"value"}
	if requestLimiter.AddNewRules(params) || configMock.IsReplaced {
		t.Error("Error happens on save causes rules not be updated")
	}
}

func TestReloadRulesReadFails_ReturnEmptyRules(t *testing.T) {

	fileName := "ConfigName.yaml"
	requestLimiter := NewRequestLimiter(fileName, 0, getTestLogger())
	testConf := map[string]string{"header1": "value1", "header2": "value2"}
	initConfig := RuleConfig{Rules: []Rule{testConf}}
	retConfValues, err := yaml.Marshal(&initConfig)

	if err != nil {
		t.Error("Error when test config was preparing")
	}

	configMock := newConfigFileMock(fileName, retConfValues)
	configMock.ShouldFailOnRead = true
	requestLimiter.config = configMock

	requestLimiter.ReloadRules()
	reloadedConf := requestLimiter.rules.Load().(RuleConfig)
	if reloadedConf.Rules != nil {
		t.Error("Error happens on load causes rules to be empty")
	}
}

func TestReloadRulesUnmarshalFails_ReturnEmptyRules(t *testing.T) {

	fileName := "ConfigName.yaml"
	requestLimiter := NewRequestLimiter(fileName, 0, getTestLogger())
	testConf := map[string]string{"header1": "value1", "header2": "value2"}
	initConfig := RuleConfig{Rules: []Rule{testConf}}
	retConfValues, err := yaml.Marshal(&initConfig)
	retConfValues = append(retConfValues, 1, 2, 3, 4, 5)

	if err != nil {
		t.Error("Error when test config was preparing")
	}

	configMock := newConfigFileMock(fileName, retConfValues)
	requestLimiter.config = configMock

	requestLimiter.ReloadRules()
	reloadedConf := requestLimiter.rules.Load().(RuleConfig)
	if reloadedConf.Rules != nil {
		t.Error("Error happens on save causes rules not be updated")
	}
}

func TestReloadRulesSuccess_ReturnsRules(t *testing.T) {

	fileName := "ConfigName.yaml"
	requestLimiter := NewRequestLimiter(fileName, 0, getTestLogger())
	testConf := map[string]string{"header1": "value1", "header2": "value2"}
	initConfig := RuleConfig{Rules: []Rule{testConf}}
	retConfValues, err := yaml.Marshal(&initConfig)

	if err != nil {
		t.Error("Error when test config was preparing")
	}

	configMock := newConfigFileMock(fileName, retConfValues)
	requestLimiter.config = configMock

	requestLimiter.ReloadRules()
	reloadedConf := requestLimiter.rules.Load().(RuleConfig)
	if reloadedConf.Rules != nil && len(reloadedConf.Rules) == 2 {
		t.Error("Header rules were not loaded")
	}
}

func TestScheduleRuleReload_UpdatePeriodZero_NotSchedule(t *testing.T) {
	fileName := "ConfigName.yaml"
	requestLimiter := NewRequestLimiter(fileName, 0, getTestLogger())
	if requestLimiter.ScheduleRuleReload() {
		t.Error("Rule update scheduled with empty period")
	}
}

func TestScheduleRuleReload_UpdatePeriodNonZero_Schedule(t *testing.T) {
	fileName := "ConfigName.yaml"
	requestLimiter := NewRequestLimiter(fileName, 30, getTestLogger())
	if !requestLimiter.ScheduleRuleReload() {
		t.Error("Rule update not scheduled with non-empty period")
	}
}