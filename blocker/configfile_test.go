package blocker

import (
	"testing"

	yaml "gopkg.in/yaml.v2"
)

func TestLoadBlockRuleHeaderConfigCorruptedFile(t *testing.T) {
	configFile := newConfigFile("../tests/block_header_files/corrupted_block_header_list.yaml")

	var rc RuleConfig
	fileBytes, err := configFile.load()
	yaml.Unmarshal(fileBytes, &rc)

	if err != nil || rc.Rules != nil {
		t.Error("Corrupted file with rules wasn't processed properly")
	}
}

func TestLoadBlockRuleHeaderConfigCorrectFile(t *testing.T) {
	configFile := newConfigFile("../tests/block_header_files/non_empty_block_header_list.yaml")

	var rc RuleConfig
	fileBytes, err := configFile.load()
	yaml.Unmarshal(fileBytes, &rc)

	if err != nil || rc.Rules == nil || len(rc.Rules) == 0 {
		t.Error("Existing rules were not loaded")
	}
}

func TestLoadBlockRuleHeaderConfigNotExistingFile(t *testing.T) {
	configFile := newConfigFile("../tests/block_header_files/non_existing_block_header_list.yaml")

	var rc RuleConfig
	fileBytes, err := configFile.load()
	yaml.Unmarshal(fileBytes, &rc)

	if err != nil || rc.Rules != nil {
		t.Error("Non existing file with rules wasn't processed properly")
	}
}
