package cfg

import (
	"github.com/golang/glog"
	"github.com/kevinu2/gohangout/common"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"os"
	"path/filepath"
)

type AppConfig struct {
	RuleLoadMode  common.RuleLoadMode  `yaml:"rule-load-mode"`
	DbConfig struct {
		Host     string `yaml:"host"`
		Port     int16  `yaml:"port"`
		Db       string `yaml:"db"`
		User     string `yaml:"user"`
		Password string `yaml:"password"`
	} `yaml:"hangout-pg"`
	RpcConfig struct {
		Port int `yaml:"port"`
		EtcdAddr []string `yaml:"etcd-addr"`
		RegistryPrefix string `yaml:"etcd-prefix-registry"`
		ConfigPrefix string  `yaml:"etcd-prefix-config"`
	} `yaml:"hangout-rpc"`
}

var (
	appConfig *AppConfig
	ConfigFileExists bool
)

func (config *AppConfig) getAppConf(configPath string) (*AppConfig, error) {
	yamlFile, err := ioutil.ReadFile(configPath)
	if err != nil {
		glog.Error(err.Error())
	}
	err = yaml.Unmarshal(yamlFile, config)
	if err != nil {
		glog.Error(err.Error())
		return nil, err
	}
	return config, nil
}

func InitAppConfig() {
	glog.Warningf(" Load application config...")
	configPath := filepath.Join(common.WorkDir, "config/application.yaml")
	_,err := os.Stat(configPath)
	if err != nil {
		glog.Error(err)
		if os.IsExist(err){
			ConfigFileExists = true
		}
	} else {
		ConfigFileExists = true
	}
    if !ConfigFileExists {
	    return
    }
	config, err := (&AppConfig{}).getAppConf(configPath)
	if err != nil {
		// 错误退出
		glog.Fatal(err, "Load application config failed from application.yml")
	}
	appConfig = config
}

func GetAppConfig() *AppConfig {
	return appConfig
}
