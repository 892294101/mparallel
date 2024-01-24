package tools

import (
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
)

func GetHomeDirectory() (s *string, err error) {
	file, _ := exec.LookPath(os.Args[0])
	ExecFilePath, _ := filepath.Abs(file)
	var dir string

	nos := runtime.GOOS
	switch nos {
	case "windows":
		execfileslice := strings.Split(ExecFilePath, `\`)
		HomeDirectory := execfileslice[:len(execfileslice)-2]
		for i, v := range HomeDirectory {
			if v != "" {
				if i > 0 {
					dir += `\` + v
				} else {
					dir += v
				}
			}
		}
	case "linux", "darwin":
		execfileslice := strings.Split(ExecFilePath, "/")
		HomeDirectory := execfileslice[:len(execfileslice)-2]
		for _, v := range HomeDirectory {
			if v != "" {
				dir += `/` + v
			}
		}
	default:
		return nil, errors.Errorf("Unsupported operating system type: %s", nos)
	}

	if dir == "" {
		return nil, errors.Errorf("Get program home directory failed: %s", dir)
	}
	return &dir, nil
}

func IsValidIPAddr(ip string) bool {
	// 先尝试解析为IP地址
	parsedIP := net.ParseIP(ip)
	if parsedIP == nil {
		return false // 解析失败，不是合法的IP地址
	}

	// 判断是不是IPv4地址
	if parsedIP.To4() != nil {
		return true // 是IPv4地址
	}

	// 判断是不是IPv6地址
	if parsedIP.To16() != nil && strings.Contains(ip, ":") {
		return true // 是IPv6地址
	}

	// 其它情况都不是合法的IP地址
	return false
}

type PAuth struct {
	Address  string
	Port     string
	User     string
	Password string
	SID      string
}

func SplitServerAuth(r string) (a []PAuth, platform string, service string, err error) {
	res := strings.Split(r, ";")
	if len(res) == 0 || len(res) != 2 {
		return nil, "", "", errors.Errorf("input host information is illegal: %v", r)
	}
	//root/RLperkwuGEfc@172.16.100.18:22,
	hostInfo := strings.Split(res[0], ",")
	if len(hostInfo) == 0 {
		return nil, "", "", errors.Errorf("input host information is illegal: %v", r)
	}

	for _, s := range hostInfo {
		reg := regexp.MustCompile(`^([\w_-]+)/([\w_-]+)@([\d\.]+):(\d+)$`)
		// 匹配连接字符串
		matches := reg.FindStringSubmatch(s)
		if matches != nil && len(matches) == 5 {
			a = append(a, PAuth{
				Address:  matches[3],
				Port:     matches[4],
				User:     matches[1],
				Password: matches[2],
			})
		} else {
			return nil, "", "", errors.Errorf("input address information is illegal: %v", s)
		}
	}

	reg := regexp.MustCompile(`^(.*)+\/(.*)+$`)
	// 匹配连接字符串
	matches := reg.FindStringSubmatch(res[1])
	if matches != nil && len(matches) == 3 {
		platform = matches[1]
		service = matches[2]
	} else {
		return nil, "", "", errors.Errorf("input address information is illegal: %v", res[1])
	}

	return a, platform, service, nil

}

func StringToInt64(s string) int64 {
	r, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0
	}
	return r
}

func StringToFloat32(s string) float32 {
	r, err := strconv.ParseFloat(s, 32)
	if err != nil {
		return 0
	}
	return float32(r)
}

type CallData struct {
	Id                  string
	Callere164          string // 计费的主叫号码
	Calleraccesse164    string // VOS5000收到的原始主叫
	Calleee164          string
	Calleeaccesse164    string
	Callerip            string
	Callergatewayid     string
	Callerproductid     string
	Callertogatewaye164 string
	Callertype          int64
	Calleeip            string
	Calleegatewayid     string
	Calleeproductid     string
	Calleetogatewaye164 string
	Calleetype          int64
	Billingmode         int64
	Calllevel           int64
	Agentfeetime        int64
	Starttime           int64
	Stoptime            int64
	Callerpdd           int64
	Calleepdd           int64
	Holdtime            int64 // 通话的时长（秒）
	Callerareacode      string
	Feetime             int64 // 通话计费时长（秒）
	Fee                 float32
	Suitefee            float32
	Suitefeetime        int64
	Incomefee           float32
	Customeraccount     string
	Customername        string
	Calleeareacode      string
	Agentfee            float32
	Agentsuitefee       float32
	Agentsuitefeetime   int64
	Agentaccount        string
	Agentname           string
	Flowno              int64
	Softswitchname      string
	Softswitchcallid    int64
	Callercallid        string
	Calleecallid        string
	Enddirection        int64
	Endreason           int64
	Billingtype         int64
	Cdrlevel            int64
	Agentcdr_id         int64
	Failed_gateways     string
}

type CallDataBatchSet struct {
	Id                  []*string
	Callere164          []*string // 计费的主叫号码
	Calleraccesse164    []*string // VOS5000收到的原始主叫
	Calleee164          []*string
	Calleeaccesse164    []*string
	Callerip            []*string
	Callergatewayid     []*string
	Callerproductid     []*string
	Callertogatewaye164 []*string
	Callertype          []*int64
	Calleeip            []*string
	Calleegatewayid     []*string
	Calleeproductid     []*string
	Calleetogatewaye164 []*string
	Calleetype          []*int64
	Billingmode         []*int64
	Calllevel           []*int64
	Agentfeetime        []*int64
	Starttime           []*int64
	Stoptime            []*int64
	Callerpdd           []*int64
	Calleepdd           []*int64
	Holdtime            []*int64 // 通话的时长（秒）
	Callerareacode      []*string
	Feetime             []*int64 // 通话计费时长（秒）
	Fee                 []*float32
	Suitefee            []*float32
	Suitefeetime        []*int64
	Incomefee           []*float32
	Customeraccount     []*string
	Customername        []*string
	Calleeareacode      []*string
	Agentfee            []*float32
	Agentsuitefee       []*float32
	Agentsuitefeetime   []*int64
	Agentaccount        []*string
	Agentname           []*string
	Flowno              []*int64
	Softswitchname      []*string
	Softswitchcallid    []*int64
	Callercallid        []*string
	Calleecallid        []*string
	Enddirection        []*int64
	Endreason           []*int64
	Billingtype         []*int64
	Cdrlevel            []*int64
	Agentcdr_id         []*int64
	Failed_gateways     []*string
}

var CallDataPool = sync.Pool{
	New: func() interface{} {
		return &CallData{}
	},
}

func CallDataGet() *CallData {
	data := CallDataPool.Get().(*CallData)
	return data
}

func CallDataPut(data *CallData) {
	CallDataPool.Put(data)
}

func IsValidDate(date string) bool {
	re := regexp.MustCompile(`^\d{4}(0[1-9]|1[0-2])(0[1-9]|[1-2]\d|3[01])$`)
	return re.MatchString(date)
}

func GetDBTable(s string) (db, table string, err error) {
	reg := regexp.MustCompile(`^(.*)+\.(.*)+$`)
	matches := reg.FindStringSubmatch(s)
	if matches != nil && len(matches) == 3 {
		db = matches[1]
		table = matches[2]
	} else {
		return "", "", errors.Errorf("%s Illegal input, format is <database>.<table>", s)
	}
	return db, table, nil
}

func JsonStr2Bson(str string) (interface{}, error) {
	var want interface{}
	err := bson.UnmarshalExtJSON([]byte(str), true, &want)
	if err != nil {
		return nil, err
	}
	return want, nil
}

// 自定义Panic异常处理,调用方式: 例如Test()函数, 指定defer ErrorCheckOfRecover(Test)
func GetFunctionName(i interface{}, seps ...rune) string {
	u := runtime.FuncForPC(reflect.ValueOf(i).Pointer()).Entry()
	f, _ := runtime.FuncForPC(reflect.ValueOf(i).Pointer()).FileLine(u)
	return f
}

func ErrorCheckOfRecover(n interface{}, log *logrus.Logger) {
	if err := recover(); err != nil {
		log.Errorf("Panic Message: %s", err)
		log.Errorf("Exception File: %s", GetFunctionName(n))
		log.Errorf("Print Stack Message: %s", string(debug.Stack()))
		log.Errorf("Abnormal exit of program")
	}
}
