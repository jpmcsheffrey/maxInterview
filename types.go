package historicloader

type Record struct {
	Index       int
	TimestampMS int64
	Price       float64
}

type WindowLoadPositions struct {
	EndOfWindowIndex int
	StartIndex       *int
}

type Repository interface {
	GetRecords(startIndex, limit int) ([]Record, error)
}

type BatchProcessor interface {
	ProcessBatch(records []Record) (Record, error)
}

type InMemoryRepository struct {
	records []Record
}

func (r *InMemoryRepository) GetRecords(startIndex, limit int) ([]Record, error) {
	if startIndex < 0 || limit < 1 || len(r.records) == 0 {
		return []Record{}, nil
	}

	start := -1
	for i, record := range r.records {
		if record.Index >= startIndex {
			start = i
			break
		}
	}
	if start == -1 {
		return []Record{}, nil
	}

	end := start + limit
	if end > len(r.records) {
		end = len(r.records)
	}

	return append([]Record(nil), r.records[start:end]...), nil
}
