package agent

import (
	"flag"
	"os"
)

type AgentSettings struct {
	AgentConfigFile     string
	Verbose             bool
	TwemproxyPoolName   string `yaml:"twemproxy_pool_name"`
	TwemproxyConfigFile string `yaml:"twemproxy_config_file"`
	Sentinels           []string `yaml:"sentinels"`
	RestartCommand      string `yaml:"restart_command"`
	RestartArgs         string `yaml:"restart_args"`
	LogFile             string `yaml:"log_file"`
}

var Settings AgentSettings = AgentSettings{}

func ValidateSettings() {
	if Settings.TwemproxyPoolName == "" ||
		Settings.TwemproxyConfigFile == "" ||
		len(Settings.Sentinels) == 0 ||
		Settings.RestartCommand == "" {
		flag.Usage()
		os.Exit(1)
	}
}

func init() {
	flag.StringVar(&Settings.AgentConfigFile, "c", "conf/agent.yml", "set configuration file")
	flag.BoolVar(&Settings.Verbose, "verbose", false, "Log generic info")
	flag.Parse()
	ReadYaml(Settings.AgentConfigFile, &Settings)

	SetFileLogger()
	ValidateSettings()
}
