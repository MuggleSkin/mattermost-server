package model

import "net/http"

type Leader struct {
	Address  string `json:"address"`
	ExpireAt int64  `json:"expire_at"`
}

func (leader *Leader) IsValid() *AppError {
	if len(leader.Address) == 0 {
		return NewAppError("Leader.IsValid", "model.leader.address.app_error", nil, "", http.StatusBadRequest)
	}
	return nil
}
