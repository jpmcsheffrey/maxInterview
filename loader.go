package historicloader

import (
	"fmt"
	"time"
)

type HistoricLoader struct {
	repository Repository
	processor  BatchProcessor
	batchSize  int
}

type batchResult struct {
	records []Record
	err     error
}

type loadState struct {
	nextLoadStartTimeMS int64
	windowCutoffMS      int64
	resumeIndex         *int
}

func NewHistoricLoader(repository Repository, processor BatchProcessor, batchSize int) (*HistoricLoader, error) {
	if repository == nil || processor == nil || batchSize < 1 {
		return nil, ErrInvalidRequest
	}

	return &HistoricLoader{
		repository: repository,
		processor:  processor,
		batchSize:  batchSize,
	}, nil
}

func (hLoader *HistoricLoader) LoadWindow(
	position WindowLoadPositions,
	absoluteEndIndex int,
	historyDuration time.Duration,
	nextLoadStartTimeIncrement time.Duration,
) (WindowLoadPositions, error) {
	if err := hLoader.validateRequest(position, absoluteEndIndex, historyDuration, nextLoadStartTimeIncrement); err != nil {
		return WindowLoadPositions{}, err
	}

	windowEnd, err := hLoader.getRecordAt(position.EndOfWindowIndex)
	if err != nil {
		return WindowLoadPositions{}, fmt.Errorf("load window end record: %w", err)
	}

	start, err := hLoader.startRecord(position, windowEnd, historyDuration)
	if err != nil {
		return WindowLoadPositions{}, fmt.Errorf("find start record: %w", err)
	}

	state := loadState{
		nextLoadStartTimeMS: start.TimestampMS + nextLoadStartTimeIncrement.Milliseconds(),
		windowCutoffMS:      windowEnd.TimestampMS,
	}
	if position.StartIndex != nil {
		state.windowCutoffMS += historyDuration.Milliseconds()
	}

	final, err := hLoader.loadBatches(start.Index, absoluteEndIndex, &state)
	if err != nil {
		return WindowLoadPositions{}, err
	}

	return WindowLoadPositions{
		EndOfWindowIndex: final.Index,
		StartIndex:       state.resumeIndex,
	}, nil
}

func (hLoader *HistoricLoader) validateRequest(
	position WindowLoadPositions,
	absoluteEndIndex int,
	historyDuration time.Duration,
	nextLoadStartTimeIncrement time.Duration,
) error {
	if position.EndOfWindowIndex < 0 || absoluteEndIndex <= position.EndOfWindowIndex {
		return ErrInvalidRequest
	}
	if position.StartIndex != nil && (*position.StartIndex < 0 || *position.StartIndex >= absoluteEndIndex) {
		return ErrInvalidRequest
	}
	if historyDuration < 0 || nextLoadStartTimeIncrement <= 0 {
		return ErrInvalidRequest
	}
	return nil
}

func (hLoader *HistoricLoader) startRecord(
	position WindowLoadPositions,
	windowEnd Record,
	historyDuration time.Duration,
) (Record, error) {
	if position.StartIndex != nil {
		return hLoader.getRecordAt(*position.StartIndex)
	}
	return hLoader.closestRecordToTarget(windowEnd, historyDuration)
}

func (hLoader *HistoricLoader) getRecordAt(index int) (Record, error) {
	records, err := hLoader.repository.GetRecords(index, 1)
	if err != nil {
		return Record{}, err
	}
	if len(records) == 0 {
		return Record{}, ErrEndOfData
	}

	record := records[0]
	if record.Index != index {
		return Record{}, ErrEndOfData
	}
	return record, nil
}

func (hLoader *HistoricLoader) closestRecordToTarget(windowEnd Record, historyDuration time.Duration) (Record, error) {
	targetTimestampMS := windowEnd.TimestampMS - historyDuration.Milliseconds()
	low := 0
	high := windowEnd.Index

	var closest Record
	found := false
	closestDistance := int64(0)

	for low <= high {
		mid := low + (high-low)/2
		record, err := hLoader.getRecordAt(mid)
		if err != nil {
			return Record{}, err
		}

		distance := distanceMS(record.TimestampMS, targetTimestampMS)
		if !found || distance < closestDistance || (distance == closestDistance && record.Index < closest.Index) {
			closest = record
			closestDistance = distance
			found = true
		}

		switch {
		case record.TimestampMS == targetTimestampMS:
			return record, nil
		case record.TimestampMS > targetTimestampMS:
			high = mid - 1
		default:
			low = mid + 1
		}
	}

	return closest, nil
}

func (hLoader *HistoricLoader) loadBatches(startIndex, absoluteEndIndex int, state *loadState) (Record, error) {
	records, err := hLoader.fetchBatch(startIndex, absoluteEndIndex)
	if err != nil {
		return Record{}, err
	}

	for {
		first := records[0]
		last := records[len(records)-1]

		var preload <-chan batchResult
		nextIndex := last.Index + 1
		if len(records) == hLoader.batchSize && nextIndex < absoluteEndIndex {
			preload = hLoader.preloadBatch(nextIndex, absoluteEndIndex)
		}

		processedLast, err := hLoader.processor.ProcessBatch(records)
		if err != nil {
			return Record{}, fmt.Errorf("process batch starting at index %d: %w", first.Index, err)
		}

		hLoader.updateResumeIndex(state, processedLast)
		if processedLast.TimestampMS >= state.windowCutoffMS {
			return processedLast, nil
		}
		if nextIndex >= absoluteEndIndex {
			return Record{}, ErrEndOfData
		}

		if preload == nil {
			records, err = hLoader.fetchBatch(nextIndex, absoluteEndIndex)
			if err != nil {
				return Record{}, err
			}
			continue
		}

		records, err = waitForBatch(preload)
		if err != nil {
			return Record{}, err
		}
	}
}

func (hLoader *HistoricLoader) fetchBatch(startIndex, absoluteEndIndex int) ([]Record, error) {
	records, err := hLoader.repository.GetRecords(startIndex, hLoader.batchSize)
	if err != nil {
		return nil, fmt.Errorf("get records starting at index %d: %w", startIndex, err)
	}

	if len(records) == 0 {
		return nil, ErrEndOfData
	}
	return records, nil
}

func (hLoader *HistoricLoader) preloadBatch(startIndex, absoluteEndIndex int) <-chan batchResult {
	results := make(chan batchResult, 1)

	go func() {
		records, err := hLoader.fetchBatch(startIndex, absoluteEndIndex)
		results <- batchResult{records: records, err: err}
	}()

	return results
}

func (hLoader *HistoricLoader) updateResumeIndex(state *loadState, record Record) {
	if state.resumeIndex == nil && record.TimestampMS >= state.nextLoadStartTimeMS {
		index := record.Index
		state.resumeIndex = &index
	}
}

func waitForBatch(results <-chan batchResult) ([]Record, error) {
	result := <-results
	return result.records, result.err
}

func distanceMS(a, b int64) int64 {
	if a > b {
		return a - b
	}
	return b - a
}
