package zookeeper

import (
	"encoding/json"
	"fmt"
	"github.com/longyufei109/leaf-go/config"
	"github.com/longyufei109/leaf-go/service"
	"github.com/longyufei109/leaf-go/service/snowflake"
	"github.com/samuel/go-zookeeper/zk"
	"log"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type zconf struct {
	zkAddress     string
	listenAddress string
	workID        int
}

type Endpoint struct {
	Ip        string `json:"ip"`
	Port      string `json:"port"`
	Timestamp int64  `json:"timestamp"`
}

var (
	zkAddressNode    string
	listenAddress    string
	workID           int64
	PREFIX_ZK_PATH   string
	PROP_PATH        string
	PATH_FOREVER     string
	ip               string
	port             string
	connectionString string
	zkUser           string
	zkPwd            string
	leafName         string
	lastUpdateTime   int64
	zkConn           *zk.Conn
)

/**
zookeeper方式获取workid
*/
func NewSnowflakeZookeeper(zconf *config.Zookeeper) service.IdGenerator {
	ip = getIp()
	port = zconf.Port
	listenAddress = ip + ":" + zconf.Port
	connectionString = zconf.Address
	zkUser = zconf.User
	zkPwd = zconf.Pwd
	leafName = zconf.LeafName
	configPath()
	initZookeeper()

	conf := snowflake.Config{
		Twepoch: 0,
		WorkerIdGetter: func() int64 {
			return workID
		},
	}
	return snowflake.New(conf)
}

func configPath() {
	PREFIX_ZK_PATH = "/snowflake/" + leafName
	PROP_PATH = os.TempDir() + string(filepath.Separator) + leafName + "/leafconf/" + port + "/workerID.properties"
	PATH_FOREVER = PREFIX_ZK_PATH + "/forever"
}

func initZookeeper() {
	// 创建zk连接地址
	hosts := strings.Split(connectionString, ",")
	// 连接zk
	conn, _, err := zk.Connect(hosts, time.Second*6)
	zkConn = conn
	//defer conn.Close()
	if err != nil {
		fmt.Println(err)
		return
	}
	zkConn.AddAuth("digest", []byte(zkUser+":"+zkPwd))
	exist, _, err := zkConn.Exists(PATH_FOREVER)
	if err != nil {
		log.Println("检测节点错误")
		return
	}
	if exist {
		keys, _, err := zkConn.Children(PATH_FOREVER)
		if err != nil {
			log.Println("获取子节点错误")
			return
		}
		nodeMap := make(map[string]int64)
		realNode := make(map[string]string)
		for _, s := range keys {
			nodeKey := strings.Split(s, "-")
			realNode[nodeKey[0]] = s
			nodeMap[nodeKey[0]], _ = strconv.ParseInt(nodeKey[1], 10, 64)
		}
		workerID, ok := nodeMap[listenAddress]
		if ok {
			zkAddressNode = PATH_FOREVER + "/" + realNode[listenAddress]
			workID = workerID
			//判断时钟回拨
			timeRight, err := checkInitTimeStamp(zkAddressNode)
			if !timeRight || err != nil {
				log.Println("时钟回拨异常")
				return
			}
			ScheduledUploadData(zkAddressNode)
			updateLocalWorkerID(workID)
			log.Println("[Old NODE]find forever node have this endpoint ip-{} port-{} workid-{} childnode and start SUCCESS", ip, port, workID)
		} else {
			newNode, err := createNode(zkConn)
			if err != nil {
				return
			}
			zkAddressNode = newNode
			nodeKey := strings.Split(newNode, "-")
			workID, _ = strconv.ParseInt(nodeKey[1], 10, 64)
			ScheduledUploadData(zkAddressNode)
			updateLocalWorkerID(workID)
			log.Println("[New NODE]can not find node on forever node that endpoint ip-{} port-{} workid-{},create own node on forever node and start SUCCESS ", ip, port, workerID)
		}

	} else {
		zkAddressNode, err := createNode(zkConn)
		if err != nil {
			fmt.Println("创建节点错误")
			return
		}
		updateLocalWorkerID(workID)
		ScheduledUploadData(zkAddressNode)
	}

}

func createNode(conn *zk.Conn) (string, error) {
	// 开始监听path
	var exist, _, err = conn.Exists(PATH_FOREVER + "/" + listenAddress + "-")
	if err != nil {
		fmt.Println(err)
		return "", err
	}
	if !exist {
		acls := zk.WorldACL(zk.PermAll)
		var data, err = buildData()
		if err != nil {
			fmt.Printf("创建失败: %v\n", err)
			return "", err
		}
		// flags有4种取值：
		// 0:永久，除非手动删除
		// zk.FlagEphemeral = 1:短暂，session断开则该节点也被删除
		// zk.FlagSequence  = 2:会自动在节点后面添加序号
		// 3:Ephemeral和Sequence，即，短暂且自动添加序号
		var flags int32 = 2
		s, err := conn.Create(PATH_FOREVER+"/"+listenAddress+"-", data, flags, acls)
		if err != nil {
			fmt.Printf("创建失败: %v\n", err)
			return "", err
		}
		return s, nil
	}
	return "", nil
}

func buildData() ([]byte, error) {
	endpoint := &Endpoint{ip, port, time.Now().UnixNano() / 1e6}
	var data, err = endpoint.Encode()
	if err != nil {
		return nil, err
	}
	return data, nil
}

func checkInitTimeStamp(zkAddressNode string) (bool, error) {
	bytes, _, err := zkConn.Get(zkAddressNode)
	if err != nil {
		return false, err
	}
	endPoint, err := Decode(bytes)
	if err != nil {
		return false, err
	}
	return !(endPoint.Timestamp > (time.Now().UnixNano() / 1e6)), nil
}

/**
* 在节点文件系统上缓存一个workid值,zk失效,机器重启时保证能够正常启动
*
* @param workerID
 */
func updateLocalWorkerID(workerID int64) error {
	f, err := os.OpenFile(PROP_PATH, os.O_CREATE|os.O_RDWR|os.O_TRUNC, os.ModeExclusive|os.ModePerm)
	if err != nil {
		return err
	}
	defer f.Close()

	f.WriteString("workerID=" + strconv.FormatInt(workerID, 10))
	return nil
}

func ScheduledUploadData(zkAddressNode string) {
	ticker := time.NewTicker(time.Second * 3)
	go func() {
		for _ = range ticker.C {
			if time.Now().UnixNano()/1e6 < lastUpdateTime {
				return
			}
			var data, err = buildData()
			if err != nil {
				fmt.Printf("创建失败: %v\n", err)
				continue
			}
			_, stat, err := zkConn.Get(zkAddressNode)
			if err != nil {
				fmt.Println("获取zk数据失败", err)
				continue
			}
			_, err = zkConn.Set(zkAddressNode, data, stat.Version)
			if err != nil {
				fmt.Printf("数据修改失败: %v\n", err)
				continue
			}
		}
	}()
}

func (obj *Endpoint) Encode() ([]byte, error) {
	//buf := new(bytes.Buffer)
	//
	//if err := binary.Write(buf, binary.LittleEndian, obj); err != nil {
	//	return nil, err
	//}
	//
	//return buf.Bytes(), nil
	var buf []byte
	var err error

	if buf, err = json.Marshal(obj); err != nil {
		log.Fatal("json marshal error:", err)
		return nil, err
	}
	return buf, nil
}

func Decode(b []byte) (*Endpoint, error) {

	obj := &Endpoint{}
	var err error
	if err = json.Unmarshal(b, obj); err != nil {
		return nil, err
	}
	return obj, nil
}

func getIp() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		fmt.Println(err)
		return ""
	}
	for _, value := range addrs {
		if ipnet, ok := value.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String()
			}
		}
	}
	return ""
}
