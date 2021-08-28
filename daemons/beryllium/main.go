package main

import (
	_ "embed"
	"flag"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"sync"
	"text/template"

	"github.com/fosshostorg/aarch64/daemons/internal/commons"
	"github.com/fosshostorg/aarch64/daemons/internal/message"
	jsoniter "github.com/json-iterator/go"
	"github.com/nsqio/go-nsq"
	"go.uber.org/zap"
)

//go:embed haproxy.template.cfg
var cfg_file string

var json = jsoniter.ConfigCompatibleWithStandardLibrary

func NewNSQHandler(l *zap.Logger, proxyConfigPath string, proxyCachePath string) *NSQHandler {
	return &NSQHandler{
		l:               l,
		proxyConfigPath: proxyConfigPath,
		proxyCachePath:  proxyCachePath,
		data:            make(map[string]string),
	}
}

type NSQHandler struct {
	l               *zap.Logger
	proxyConfigPath string
	proxyCachePath  string
	data            map[string]string
	mutex           sync.Mutex
}

func (h *NSQHandler) HandleMessage(m *nsq.Message) error {
	// Message Body Should Not be Empty
	if m.Body == nil || len(m.Body) <= 0 {
		h.l.Error("message body is empty")
		return nil
	}

	// Decode Message Body
	var msg message.Message
	if err := json.Unmarshal(m.Body, &msg); err != nil {
		h.l.Error("failed to unmarshal message", zap.Error(err))
		return nil
	}

	// Name and IP Should Not be Empty
	if msg.MessageData.Name == "" || msg.MessageData.IP == "" {
		h.l.Error("name or ip is empty")
		return nil
	}

	// Prevent Data Races while Handling Messages
	h.mutex.Lock()
	switch msg.Action {
	case message.AddProxy:
		h.addProxy(&msg.MessageData)
		h.GenerateConfig()
		h.SaveProxies()
		h.ReloadProxy()
		break
	case message.DeleteProxy:
		h.deleteProxy(&msg.MessageData)
		h.GenerateConfig()
		h.SaveProxies()
		h.ReloadProxy()
		break
	default:
		h.l.Error("unknown action")
		return nil
	}
	h.mutex.Unlock()

	return nil
}

func (h *NSQHandler) addProxy(data *message.MessageData) error {
	h.data[data.Name] = data.IP
	h.l.Info("Added Proxy", zap.String("Domain Name", data.Name), zap.String("IP", data.IP))
	return nil
}

func (h *NSQHandler) deleteProxy(data *message.MessageData) error {
	delete(h.data, data.Name)
	h.l.Info("Deleted Proxy", zap.String("Domain Name", data.Name), zap.String("IP", data.IP))
	return nil
}

func (h *NSQHandler) GenerateConfig() error {
	funcMap := template.FuncMap{
		"replaceWithUnderscores": func(input string) string {
			return strings.ReplaceAll(input, ".", "_")
		},
	}
	tmpl, err := template.New("haproxy.template.cfg").Funcs(funcMap).Parse(cfg_file)
	if err != nil {
		h.l.Error("Failed to Parse Template", zap.Error(err))
		return nil
	}
	configFile, err := os.Create(h.proxyConfigPath)
	if err != nil {
		h.l.Error("Failed to Open Config File", zap.Error(err))
		return nil
	}
	defer configFile.Close()
	if err = tmpl.Execute(configFile, h.data); err != nil {
		h.l.Error("Failed to Write to Config File", zap.Error(err))
		return nil
	}
	return nil
}

func (h *NSQHandler) ReloadProxy() error {
	_, err := exec.Command("/bin/bash", "-c", "/usr/sbin/haproxy -f /etc/haproxy/haproxy.cfg -p /run/haproxy.pid -x /run/haproxy/admin.sock -sf $(cat /run/haproxy.pid)").Output()
	if err != nil {
		h.l.Info("Failed to Reload Proxy", zap.Error(err))
		return nil
	}
	return nil
}

func (h *NSQHandler) SaveProxies() error {
	file, err := os.Create(h.proxyCachePath)
	defer file.Close()
	if err != nil {
		h.l.Error("Failed to Create beryllium.json", zap.Error(err))
		return nil
	}
	encoder := json.NewEncoder(file)
	if err = encoder.Encode(h.data); err != nil {
		h.l.Error("Failed to Save beryllium.json", zap.Error(err))
		return nil
	}
	return nil
}

func (h *NSQHandler) LoadProxies() error {
	file, err := os.Open(h.proxyCachePath)
	if err != nil {
		h.l.Info("Unable to Open beryllium.json")
		return nil
	}
	defer file.Close()
	decoder := json.NewDecoder(file)
	if err = decoder.Decode(&h.data); err != nil {
		h.l.Error("Failed to Load beryllium.json", zap.Error(err))
		h.data = make(map[string]string)
		return nil
	}
	return nil
}

func main() {
	l, _ := zap.NewDevelopment()

	var (
		nsqConnectURI   string
		proxyConfigPath string
		proxyCachePath  string
	)

	// Parse Flags
	flag.StringVar(&nsqConnectURI, "nsq-connect-uri", commons.NSQCoreUrl, "The URI for NSQ producers & consumers to connect to")
	flag.StringVar(&proxyConfigPath, "proxy-config-path", commons.ProxyConfigPath, "The path to the proxy configuration file")
	flag.StringVar(&proxyCachePath, "proxy-cache-path", commons.ProxyCachePath, "The path to the proxy cache file")
	flag.Parse()

	// Connect to NSQ
	hostname := commons.GetHostname()
	if hostname == "" {
		l.Fatal("failed to read hostname")
	}
	nh := NewNSQHandler(l, proxyConfigPath, proxyCachePath)
	nh.LoadProxies()
	nh.GenerateConfig()
	nh.ReloadProxy()
	nsqConsumer := commons.CreateNSQConsumer(nsqConnectURI, "aarch64-proxy", hostname, nh)
	l.Info("Successfully Connected to NSQ")
	defer nsqConsumer.Stop()
	l.Info("Beryllium has Started!!!")

	// Handle Shutting Down
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	signal.Notify(c, os.Kill)
	<-c
	nh.SaveProxies()
	l.Info("Beryllium Shutting Down...Byeeee")
}
