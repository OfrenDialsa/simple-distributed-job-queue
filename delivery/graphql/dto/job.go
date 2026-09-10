package dto

type EnqueueRequest struct {
	Task string `json:"task"`
	Key  string `json:"key"`
}
