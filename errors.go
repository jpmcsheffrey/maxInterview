package historicloader

import "errors"

var (
	ErrEndOfData = errors.New("end of available data")

	ErrInvalidRequest = errors.New("invalid load request")
)
