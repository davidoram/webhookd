package view

type SubscriptionCollection struct {
	Subscriptions []Subscription `json:"_data"`
	Offset        int64          `json:"_offset"`
	Limit         int64          `json:"_limit"`
}

func NewSubscriptionCollection(subs []Subscription, offset, limit int64) SubscriptionCollection {
	return SubscriptionCollection{
		Subscriptions: subs,
		Offset:        offset,
		Limit:         limit,
	}
}

// // MarshallForAPI returns the SubscriptionCollection as a JSON byte array, in the format suitable for rendering to the API
// func (s SubscriptionCollection) MarshallForAPI() ([]byte, error) {
// 	return json.Marshal(s)
// }

func (s SubscriptionCollection) HasNextBatch() bool {
	return len(s.Subscriptions) == int(s.Limit)
}
