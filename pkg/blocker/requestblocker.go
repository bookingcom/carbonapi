package blocker

import (
	"fmt"
	"net/http"
	"net/url"
	"os"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
	yaml "gopkg.in/yaml.v2"
)

// Rule is a request blocking rule
type Rule map[string]string

// RuleConfig represents the request blocking rules
type RuleConfig struct {
	Rules []Rule
}

// RequestBlocker blocks request according to rules that defines which headers are not allowed
type RequestBlocker struct {
	config              configFileManager
	logger              *zap.Logger
	rules               atomic.Value
	updatePeriod        time.Duration
	blockRuleConfigName string
}

// NewRequestBlocker creates a new instance of request blocker without any rules
// and sets name of config files that will be used as storage for rules
func NewRequestBlocker(blockHeaderFile string, updatePeriod time.Duration, logger *zap.Logger) *RequestBlocker {
	instance := &RequestBlocker{
		config:              newConfigFile(blockHeaderFile),
		logger:              logger,
		updatePeriod:        updatePeriod,
		blockRuleConfigName: blockHeaderFile,
	}
	instance.rules.Store(RuleConfig{})
	return instance
}

// ScheduleRuleReload starts reload rules from rules config file with
// frequency defined by updatePeriod
func (rl *RequestBlocker) ScheduleRuleReload() bool {
	if rl.updatePeriod <= 0 {
		return false
	}

	ticker := time.NewTicker(rl.updatePeriod)
	go func() {
		for range ticker.C {
			rl.ReloadRules()
		}
	}()
	return true
}

// ReloadRules loads rules from config and updates blocker with these rules
func (rl *RequestBlocker) ReloadRules() {
	fileData, err := rl.config.load()
	if err != nil {
		rl.logger.Info("failed to load header block rules", zap.Error(err))
		rl.rules.Store(RuleConfig{})
		return
	}

	var rc RuleConfig
	if err := yaml.Unmarshal(fileData, &rc); err != nil {
		rl.logger.Error("couldn't unmarshal block rule file data", zap.Error(err))
		rl.rules.Store(RuleConfig{})
		return
	}

	rl.rules.Store(rc)
}

// AddNewRules updates rule config file with new rules
func (rl *RequestBlocker) AddNewRules(queryParams url.Values) bool {
	if !rl.isValidConfigFileName() {
		return false
	}

	m := make(Rule)
	for k, v := range queryParams {
		if v == nil {
			rl.logger.Error(fmt.Sprintf("Empty value for header %s", k))
		}
		if k == "" || v[0] == "" {
			continue
		}
		m[k] = v[0]
	}

	var rc RuleConfig
	var err1 error
	if len(m) == 0 {
		rl.logger.Error("couldn't create a rule from params")
	} else {
		fileData, err := rl.config.load()
		if err == nil {
			unmarshalErr := yaml.Unmarshal(fileData, &rc)
			if unmarshalErr != nil {
				return false
			}
		}
		err1 = rl.appendRuleToConfig(rc, m)
	}

	return len(m) != 0 && err1 == nil
}

// Unblock deletes rule config file with all defined rules.
// Next time rules will be reloaded, request blocker won't block any request
func (rl *RequestBlocker) Unblock() error {
	return os.Remove(rl.blockRuleConfigName)
}

// ShouldBlockRequest checks request headers against block rules
func (rl *RequestBlocker) ShouldBlockRequest(r *http.Request) bool {
	blockingRules := rl.rules.Load().(RuleConfig)
	for _, rule := range blockingRules.Rules {
		if isBlockingHeaderRule(r, rule) {
			return true
		}
	}
	return false
}

func isBlockingHeaderRule(req *http.Request, r Rule) bool {
	for k, v := range r {
		if req.Header.Get(k) == v {
			return true
		}
	}
	return false
}

func (rl *RequestBlocker) appendRuleToConfig(rc RuleConfig, r Rule) error {
	rc.Rules = append(rc.Rules, r)
	output, err := yaml.Marshal(rc)
	if err == nil {
		rl.logger.Info("updating file", zap.String("ruleConfig", string(output[:])))
		err = rl.config.write(output)
		if err != nil {
			rl.logger.Error("couldn't write rule to file")
		}
	}
	return err
}

// isValid checks if file can be used to store rules
func (rl *RequestBlocker) isValidConfigFileName() bool {
	return rl.blockRuleConfigName != ""
}
