package agent

import (
	"fmt"
	"os"
	"os/exec"
	"strings"

	"github.com/garyburd/redigo/redis"
)

type TwemproxyConfig struct {
	Listen            string   `yaml:"listen,omitempty"`
	Hash              string   `yaml:"hash,omitempty"`
	Distribution      string   `yaml:"distribution,omitempty"`
	AutoEjectHosts    bool     `yaml:"auto_eject_hosts,omitempty"`
	Redis             bool     `yaml:"redis,omitempty"`
	ServerConnections int      `yaml:"server_connections,omitempty"`
	Timeout           int      `yaml:"timeout,omitempty"`
	RetryTimeout      int      `yaml:"server_retry_timeout,omitempty"`
	FailureLimit      int      `yaml:"server_failure_limit,omitempty"`
	Servers           []string `yaml:"servers,omitempty"`
}

var twemproxyConfig map[string]TwemproxyConfig

func UpdateMaster(master_name string, ip string, port string) bool {
	address := ComposeRedisAddress(ip, port)
	Debug(fmt.Sprintf("Updating master %s to %s.", master_name, address))
	servers := twemproxyConfig[Settings.TwemproxyPoolName].Servers
	for i := range servers {
		server_data := strings.Split(servers[i], string(' '))
		address_data := strings.Split(server_data[0], string(':'))
		old_address := ComposeRedisAddress(address_data[0], address_data[1])
		old_port := address_data[1]

		if len(server_data) < 2 {
			panic("server name was not configured in twemproxy conf")
		}
		server_name := server_data[1]

		if master_name == server_name &&
			address != old_address ||
			port != old_port {
			twemproxyConfig[Settings.TwemproxyPoolName].Servers[i] = fmt.Sprint(address, ":1 ", master_name)
			return true
		}
	}

	return false
}

func LoadTwemproxyConfig() {
	Debug("Loading Twemproxy config.")
	ReadYaml(Settings.TwemproxyConfigFile, &twemproxyConfig)
}

func SaveTwemproxyConfig() {
	Debug("Saving Twemproxy config.")
	WriteYaml(Settings.TwemproxyConfigFile, &twemproxyConfig)
}

func RestartTwemproxy() error {
	Debug("Restarting Twemproxy.")
	args := strings.Split(Settings.RestartArgs, string(' '))
	cmd := exec.Command(Settings.RestartCommand, args...)
	cmd.Env = append(os.Environ(), Settings.RestartEnv)
	out, err := cmd.Output()

	if err != nil {
		Debug(fmt.Sprintf("Cannot restart twemproxy. output: %s. error: %s", out, err))
	}

	return err
}

func GetSentinel() (redis.Conn, error) {
	var c redis.Conn
	var err error
	for i := range Settings.Sentinels {
		c, err = redis.Dial("tcp", Settings.Sentinels[i])
		if err == nil {
			Debug(fmt.Sprintf("Connected to sentinel %s", Settings.Sentinels[i]))
			break
		}
		Debug(fmt.Sprintf("Sentinel %s is not reachable", Settings.Sentinels[i]))
	}

	return c, err
}

func SwitchMaster(master_name string, ip string, port string) error {
	Debug("Received switch-master.")
	if UpdateMaster(master_name, ip, port) {
		SaveTwemproxyConfig()
		err := RestartTwemproxy()
		return err
	} else {
		return nil
	}
}

func ValidateCurrentMaster() error {
	c, err := GetSentinel()
	if err != nil {
		return err
	}

	reply, err := redis.Values(c.Do("SENTINEL", "masters"))

	if err != nil {
		return err
	}

	var sentinel_info []string

	reply, err = redis.Scan(reply, &sentinel_info)
	if err != nil {
		return err
	}
	master_name := sentinel_info[1]
	ip := sentinel_info[3]
	port := sentinel_info[5]

	err = SwitchMaster(master_name, ip, port)

	return err
}

func SubscribeToSentinel() {
	c, err := GetSentinel()
	if err != nil {
		Fatal("Cannot connect to any sentinel.")
	}

	err = ValidateCurrentMaster()
	if err != nil {
		Fatal("Cannot switch to current master")
	}
	psc := redis.PubSubConn{c}
	Debug("Subscribing to sentinel (+switch-master).")
	psc.Subscribe("+switch-master")
	for {
		switch v := psc.Receive().(type) {
		case redis.Message:
			Debug(fmt.Sprintf("%s: message: %s", v.Channel, v.Data))
			data := strings.Split(string(v.Data), string(' '))
			SwitchMaster(data[0], data[3], data[4])
		case redis.Subscription:
			Debug(fmt.Sprintf("%s: %s %d", v.Channel, v.Kind, v.Count))
		case error:
			Debug("Subscription error, trying to fallback")
			c, err = GetSentinel()
			if err != nil {
				Fatal("Error with redis connection:", psc)
			}

			psc = redis.PubSubConn{c}
			Debug("Subscribing to sentinel (+switch-master).")
			psc.Subscribe("+switch-master")
		}
	}
}

func Run() {
	LoadTwemproxyConfig()
	SubscribeToSentinel()
}
