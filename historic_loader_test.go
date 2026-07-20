package historicloader

import (
	"errors"
	"slices"
	"testing"
	"time"
)

// Used as an example test suite, some tests don't pass, that is to highlight the bugs. Use as a blueprint for decent testing
const (
	batchSize   = 10
	oneMinuteMS = int64(60_000)
)

type fakeRepository struct {
	records []Record
}

func newFakeRepository(records []Record) *fakeRepository {
	copied := append([]Record(nil), records...)
	slices.SortFunc(copied, func(a, b Record) int {
		switch {
		case a.Index < b.Index:
			return -1
		case a.Index > b.Index:
			return 1
		default:
			return 0
		}
	})

	return &fakeRepository{records: copied}
}

func (r *fakeRepository) GetRecords(startIndex, limit int) ([]Record, error) {
	if limit < 1 {
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

type fakeProcessor struct {
	batches [][]Record
}

func (p *fakeProcessor) ProcessBatch(records []Record) (Record, error) {
	p.batches = append(p.batches, append([]Record(nil), records...))

	if len(records) == 0 {
		return Record{}, ErrEndOfData
	}
	return records[len(records)-1], nil
}

func (p *fakeProcessor) recordsFromWindow(startBatch int) []Record {
	var records []Record
	for _, batch := range p.batches[startBatch:] {
		records = append(records, batch...)
	}
	return append([]Record(nil), records...)
}

func TestHistoricLoaderHappyPath(t *testing.T) {
	loader, processor := newLoader(t, buildRecords(400, -1, 0))

	got, err := loader.LoadWindow(WindowLoadPositions{EndOfWindowIndex: 200}, 300, time.Hour, time.Hour)
	if err != nil {
		t.Fatalf("LoadWindow returned error: %v", err)
	}

	assertPosition(t, got, WindowLoadPositions{EndOfWindowIndex: 209, StartIndex: translatedIntPtr(209)})
	assertIndexesEqual(t, processor.recordsFromWindow(0), indexRange(140, 210))
	assertStrictlyIncreasing(t, processor.recordsFromWindow(0))
}

func TestHistoricLoaderMultipleSequentialCalls(t *testing.T) {
	loader, processor := newLoader(t, buildRecords(400, -1, 0))

	first, err := loader.LoadWindow(WindowLoadPositions{EndOfWindowIndex: 200}, 300, time.Hour, time.Hour)
	if err != nil {
		t.Fatalf("first LoadWindow returned error: %v", err)
	}
	assertPosition(t, first, WindowLoadPositions{EndOfWindowIndex: 209, StartIndex: translatedIntPtr(209)})
	firstBatchCount := len(processor.batches)
	assertIndexesEqual(t, processor.recordsFromWindow(0), indexRange(140, 210))

	second, err := loader.LoadWindow(first, 300, time.Hour, time.Hour)
	if err != nil {
		t.Fatalf("second LoadWindow returned error: %v", err)
	}

	assertPosition(t, second, WindowLoadPositions{EndOfWindowIndex: 278, StartIndex: translatedIntPtr(278)})
	assertIndexesEqual(t, processor.recordsFromWindow(firstBatchCount), indexRange(209, 279))
	assertIndexesEqual(t, processor.recordsFromWindow(0), indexRange(140, 279))
	assertStrictlyIncreasing(t, processor.recordsFromWindow(0))
}

func TestHistoricLoaderAbsoluteEndIndexReached(t *testing.T) {
	loader, processor := newLoader(t, buildRecords(400, -1, 0))

	first, err := loader.LoadWindow(WindowLoadPositions{EndOfWindowIndex: 200}, 250, time.Hour, time.Hour)
	if err != nil {
		t.Fatalf("first LoadWindow returned error: %v", err)
	}
	assertPosition(t, first, WindowLoadPositions{EndOfWindowIndex: 209, StartIndex: translatedIntPtr(209)})

	_, err = loader.LoadWindow(first, 250, time.Hour, time.Hour)
	if !errors.Is(err, ErrEndOfData) {
		t.Fatalf("expected ErrEndOfData, got %v", err)
	}

	assertNoIndexAtOrBeyond(t, processor.recordsFromWindow(0), 250)
	assertIndexesEqual(t, processor.recordsFromWindow(0), indexRange(140, 250))
	assertStrictlyIncreasing(t, processor.recordsFromWindow(0))
}

func TestHistoricLoaderDifferentHistoryAndNextLoadStartTimeIncrementDurations(t *testing.T) {
	loader, processor := newLoader(t, buildRecords(400, -1, 0))

	got, err := loader.LoadWindow(WindowLoadPositions{EndOfWindowIndex: 200}, 300, 2*time.Hour, time.Hour)
	if err != nil {
		t.Fatalf("LoadWindow returned error: %v", err)
	}

	assertPosition(t, got, WindowLoadPositions{EndOfWindowIndex: 209, StartIndex: translatedIntPtr(149)})
	assertIndexesEqual(t, processor.recordsFromWindow(0), indexRange(80, 210))
	assertStrictlyIncreasing(t, processor.recordsFromWindow(0))
}

func TestHistoricLoaderHistoryDurationLongerThanNextLoadStartTimeIncrementSubHour(t *testing.T) {
	loader, processor := newLoader(t, buildRecords(300, -1, 0))
	historyDuration := 90 * time.Minute
	nextLoadStartTimeIncrement := 30 * time.Minute

	got, err := loader.LoadWindow(WindowLoadPositions{EndOfWindowIndex: 150}, 220, historyDuration, nextLoadStartTimeIncrement)
	if err != nil {
		t.Fatalf("LoadWindow returned error: %v", err)
	}

	assertPosition(t, got, WindowLoadPositions{EndOfWindowIndex: 159, StartIndex: translatedIntPtr(99)})
	assertIndexesEqual(t, processor.recordsFromWindow(0), indexRange(60, 160))
	assertStrictlyIncreasing(t, processor.recordsFromWindow(0))
}

func TestHistoricLoaderDifferentDurationsBackToBack(t *testing.T) {
	loader, processor := newLoader(t, buildRecords(400, -1, 0))

	first, err := loader.LoadWindow(WindowLoadPositions{EndOfWindowIndex: 200}, 300, 2*time.Hour, time.Hour)
	if err != nil {
		t.Fatalf("first LoadWindow returned error: %v", err)
	}
	assertPosition(t, first, WindowLoadPositions{EndOfWindowIndex: 209, StartIndex: translatedIntPtr(149)})
	firstBatchCount := len(processor.batches)
	firstRecords := processor.recordsFromWindow(0)
	assertIndexesEqual(t, firstRecords, indexRange(80, 210))
	assertStrictlyIncreasing(t, firstRecords)

	second, err := loader.LoadWindow(first, 300, 2*time.Hour, time.Hour)
	if err != nil {
		t.Fatalf("second LoadWindow returned error: %v", err)
	}
	secondRecords := processor.recordsFromWindow(firstBatchCount)

	assertPosition(t, second, WindowLoadPositions{EndOfWindowIndex: 278, StartIndex: translatedIntPtr(218)})
	assertIndexesEqual(t, secondRecords, indexRange(149, 279))
	assertStrictlyIncreasing(t, secondRecords)
}

func TestHistoricLoaderResumedLoadExtendsWindowCutoffByNextLoadStartTimeIncrement(t *testing.T) {
	loader, _ := newLoader(t, buildRecords(300, -1, 0))
	historyDuration := 90 * time.Minute
	nextLoadStartTimeIncrement := 30 * time.Minute

	first, err := loader.LoadWindow(WindowLoadPositions{EndOfWindowIndex: 150}, 260, historyDuration, nextLoadStartTimeIncrement)
	if err != nil {
		t.Fatalf("first LoadWindow returned error: %v", err)
	}
	assertPosition(t, first, WindowLoadPositions{EndOfWindowIndex: 159, StartIndex: translatedIntPtr(99)})

	second, err := loader.LoadWindow(first, 260, historyDuration, nextLoadStartTimeIncrement)
	if err != nil {
		t.Fatalf("second LoadWindow returned error: %v", err)
	}

	assertPosition(t, second, WindowLoadPositions{EndOfWindowIndex: 198, StartIndex: translatedIntPtr(138)})
}

func TestHistoricLoaderTimestampGapInFirstContext(t *testing.T) {
	loader, processor := newLoader(t, buildRecords(400, 170, 10*time.Minute))

	got, err := loader.LoadWindow(WindowLoadPositions{EndOfWindowIndex: 200}, 300, time.Hour, time.Hour)
	if err != nil {
		t.Fatalf("LoadWindow returned error: %v", err)
	}

	processed := processor.recordsFromWindow(0)
	assertPosition(t, got, WindowLoadPositions{EndOfWindowIndex: 209, StartIndex: translatedIntPtr(209)})
	assertIndexesEqual(t, processed, indexRange(150, 210))
	assertStrictlyIncreasing(t, processed)
}

func TestHistoricLoaderDoesNotReprocessResumeBoundaryRecord(t *testing.T) {
	loader, processor := newLoader(t, buildRecords(400, -1, 0))

	first, err := loader.LoadWindow(WindowLoadPositions{EndOfWindowIndex: 200}, 300, time.Hour, time.Hour)
	if err != nil {
		t.Fatalf("first LoadWindow returned error: %v", err)
	}

	firstBatchCount := len(processor.batches)

	_, err = loader.LoadWindow(first, 300, time.Hour, time.Hour)
	if err != nil {
		t.Fatalf("second LoadWindow returned error: %v", err)
	}

	secondRecords := processor.recordsFromWindow(firstBatchCount)
	if len(secondRecords) == 0 {
		t.Fatalf("second load processed no records")
	}

	if secondRecords[0].Index == *first.StartIndex {
		t.Fatalf("second load reprocessed resume boundary index %d", *first.StartIndex)
	}
}

func TestHistoricLoaderTimestampGapInLaterContext(t *testing.T) {
	loader, processor := newLoader(t, buildRecords(400, 230, 10*time.Minute))

	first, err := loader.LoadWindow(WindowLoadPositions{EndOfWindowIndex: 200}, 300, time.Hour, time.Hour)
	if err != nil {
		t.Fatalf("first LoadWindow returned error: %v", err)
	}
	assertPosition(t, first, WindowLoadPositions{EndOfWindowIndex: 209, StartIndex: translatedIntPtr(209)})
	firstBatchCount := len(processor.batches)
	assertIndexesEqual(t, processor.recordsFromWindow(0), indexRange(140, 210))

	second, err := loader.LoadWindow(first, 300, time.Hour, time.Hour)
	if err != nil {
		t.Fatalf("second LoadWindow returned error: %v", err)
	}
	secondRecords := processor.recordsFromWindow(firstBatchCount)

	assertPosition(t, second, WindowLoadPositions{EndOfWindowIndex: 268, StartIndex: translatedIntPtr(268)})
	assertIndexesEqual(t, secondRecords, indexRange(209, 269))
	assertIndexesEqual(t, processor.recordsFromWindow(0), indexRange(140, 269))
	assertStrictlyIncreasing(t, processor.recordsFromWindow(0))
}

func newLoader(t *testing.T, records []Record) (*HistoricLoader, *fakeProcessor) {
	t.Helper()

	processor := &fakeProcessor{}
	loader, err := NewHistoricLoader(
		newFakeRepository(records),
		processor,
		batchSize,
	)
	if err != nil {
		t.Fatalf("NewHistoricLoader returned error: %v", err)
	}

	return loader, processor
}

func buildRecords(count int, gapAt int, gap time.Duration) []Record {
	records := make([]Record, count)
	for i := range records {
		timestampMS := int64(i) * oneMinuteMS
		if gapAt >= 0 && i >= gapAt {
			timestampMS += gap.Milliseconds()
		}

		records[i] = Record{
			Index:       i,
			TimestampMS: timestampMS,
			Price:       float64(i),
		}
	}
	return records
}

func indexRange(start, endExclusive int) []int {
	indexes := make([]int, 0, endExclusive-start)
	for i := start; i < endExclusive; i++ {
		indexes = append(indexes, i)
	}
	return indexes
}

func recordIndexes(records []Record) []int {
	indexes := make([]int, len(records))
	for i, record := range records {
		indexes[i] = record.Index
	}
	return indexes
}

func translatedIntPtr(v int) *int {
	return &v
}

func assertPosition(t *testing.T, got, want WindowLoadPositions) {
	t.Helper()

	if got.EndOfWindowIndex != want.EndOfWindowIndex {
		t.Fatalf("WindowIndex: got %d, want %d", got.EndOfWindowIndex, want.EndOfWindowIndex)
	}

	if got.StartIndex == nil && want.StartIndex == nil {
		return
	}
	if got.StartIndex == nil || want.StartIndex == nil {
		t.Fatalf("ResumeIndex: got %v, want %v", optionalIndexValue(got.StartIndex), optionalIndexValue(want.StartIndex))
	}
	if *got.StartIndex != *want.StartIndex {
		t.Fatalf("ResumeIndex: got %d, want %d", *got.StartIndex, *want.StartIndex)
	}
}

func optionalIndexValue(index *int) any {
	if index == nil {
		return nil
	}
	return *index
}

func assertIndexesEqual(t *testing.T, records []Record, want []int) {
	t.Helper()

	got := recordIndexes(records)
	if !slices.Equal(got, want) {
		t.Fatalf("processed indexes:\ngot  %v\nwant %v", got, want)
	}
}

func assertStrictlyIncreasing(t *testing.T, records []Record) {
	t.Helper()

	indexes := recordIndexes(records)
	for i := 1; i < len(indexes); i++ {
		if indexes[i] <= indexes[i-1] {
			t.Fatalf("indexes are not strictly increasing at offset %d: got %v", i, indexes)
		}
	}
}

func assertNoIndexAtOrBeyond(t *testing.T, records []Record, exclusiveEnd int) {
	t.Helper()

	for _, record := range records {
		if record.Index >= exclusiveEnd {
			t.Fatalf("processed index %d at or beyond exclusive absoluteEndIndex %d", record.Index, exclusiveEnd)
		}
	}
}
