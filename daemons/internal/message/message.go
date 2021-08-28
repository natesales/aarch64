package message

type VMData struct {
	ID          string `bson:"_id" json:"_id"`
	Hostname    string `bson:"hostname" json:"hostname"`
	Pop         string `bson:"pop" json:"pop"`
	Project     string `bson:"project" json:"project"`
	Os          string `bson:"os" json:"os"`
	Vcpus       int    `bson:"vcpus" json:"vcpus"`
	Memory      int    `bson:"memory" json:"memory"`
	Ssd         int    `bson:"ssd" json:"ssd"`
	Host        int    `bson:"host" json:"host"`
	Password    int    `bson:"password" json:"password"`
	Phoned_home bool   `bson:"phoned_home" json:"phoned_home"`
	Created     struct {
		By string  `bson:"by"`
		At float64 `bson:"at"` // unix timestamp
	} `bson:"created"`
	Index   int    `bson:"index"`
	Prefix  string `bson:"prefix"`
	Gateway string `bson:"gateway"`
	Address string `bson:"address"`
	State   int    `bson:"state"`
}

type MessageData struct {
	Name   string      `json:"name"`
	Source string      `json:"source"`
	Event  ActionEvent `json:"event"`
	Num    int         `json:"state"`
	IP     string      `json:"ip"`
}

type ErrorMessage struct {
	Error     string `json:"error"`
	Text      string `json:"text"`
	Host      string `json:"host"`
	NSQMsg    string `json:"nsq_msg"`
	MachineID int64  `json:"machine_id"`
}

type Message struct {
	ID          int64       `json:"id"`
	Action      Action      `json:"action"`
	MessageData MessageData `json:"message_data"`
	VMData      VMData      `json:"vm_data"`
}

type Action int64

const (
	ChangeState Action = iota
	NewVMState
	// Beryllium HAProxy Update Actions
	AddProxy
	DeleteProxy
)

type ActionEvent int64

const (
	StateShutdown ActionEvent = iota
	StateStartup
	StateReset
	StateReboot
	StateStop
)
