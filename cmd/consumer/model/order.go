package model

type Item struct {
	Id       int32   `json:"id"`
	Name     string  `json:"name"`
	Quantity int32   `json:"quantity"`
	Cost     float32 `json:"cost"`
}

type Order struct {
	UserId    uint32  `json:"user_id"`
	Items     []Item  `json:"items"`
	TotalCost float32 `json:"total_cost"`
}
