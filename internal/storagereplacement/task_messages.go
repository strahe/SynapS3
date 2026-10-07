package storagereplacement

const (
	MessageRetireDataSet        = "replacement.retire_dataset"
	MessageReplacementActivated = "replacement.activated"
)

type RetireDataSet struct {
	ReplacementID int64
	BindingID     int64
}

func (RetireDataSet) MessageType() string { return MessageRetireDataSet }

type ReplacementActivated struct {
	BucketID  int64
	CopyIndex int
}

func (ReplacementActivated) MessageType() string { return MessageReplacementActivated }
