package common

import (
    "github.com/golang/glog"
    "io/fs"
    "os"
    "os/exec"
    "path/filepath"
    "strings"
)

const (
    ruleDirName = "config/rules"
    DefaultTaskVendor = "hangout"
)

type UploadArgs struct {
    //base64编码
    Content string
}

type StopArgs struct {
    TaskId string
}

type ResponseCode string
const (
   SUCCESS ResponseCode = "0"
   FAIL    ResponseCode = "1"
)

type RuleLoadMode string
const (
    Cmd  RuleLoadMode = "CMD"
    ConfigDir RuleLoadMode = "CONFIG_DIR"
    Rpc RuleLoadMode = "RPC"
    Db RuleLoadMode = "DB"
    Empty RuleLoadMode = ""
)

var (
    WorkDir  string
)

func init() {
    file, _ := exec.LookPath(os.Args[0])
    path, _ := filepath.Abs(file)
    WorkDir = filepath.Dir(path)
}

func LoadRuleFromConfigDir() []string  {
    ruleDir := filepath.Join(WorkDir, ruleDirName)
    var ruleFiles []string
    err := filepath.WalkDir(ruleDir, func(path string, d fs.DirEntry, err error) error {
        if d.IsDir() {
            return nil
        }
        if strings.HasSuffix(path, ".yaml") || strings.HasSuffix(path, ".yml") {
            ruleFiles = append(ruleFiles, path)
        }
        return nil
    })
    if err != nil {
        glog.Error(err)
    }
    return ruleFiles
}



