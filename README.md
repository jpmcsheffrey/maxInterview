# Historic Loader

## For candidate

You have been given an implementation of a historic data loader and its specification.

Your task is to write tests for the existing behaviour.

Focus on:

- understanding the expected behaviour from the specification
- identifying the important scenarios to test
- choosing an appropriate level of coverage
- keeping the tests clear and maintainable

Do not modify the production code.

### Loader Specification

Batch: an ordered group of records with increasing indexes.

Load Window:  



`LoadWindow(position WindowLoadPositions, absoluteEndIndex int, historyDuration time.Duration, nextLoadStartTimeIncrement time.Duration)` loads one historical window of records in batches.

Parameters:

`historyDuration time.Duration` the total time of a window
`nextLoadStartTimeIncrement time.Duration` how far the start of the window will increment by for the next load window
`absoluteEndIndex int` the absolute end limit index
position WindowLoadPositions:
`EndOfWindowIndex int` the end of the window
`StartIndex       *int` acts as the start index for the load window. Is nil on the first load, a search is performed to find it based on history duration.

Records must be ordered by increasing `Index` and increasing `TimestampMS`. Repository reads start at an inclusive index. `absoluteEndIndex` is an exclusive boundary for getting a new batch; a batch that already started may process past it.

When the next batch would start at or beyond `absoluteEndIndex`, `LoadWindow` returns `ErrEndOfData`, simulating that no more records are available.

For the first call, pass `WindowLoadPositions{EndOfWindowIndex: n, StartIndex: nil}`. The loader finds the start record closest to `timestamp at EndOfWindowIndex - historyDuration`.

For later calls, pass the `WindowLoadPositions` returned by the previous successful call. When `StartIndex` is set, the loader starts from that index instead of searching again.

During a window load, the next load start time is calculated as `start record timestamp + nextLoadStartTimeIncrement`.

For the first load, the window end time is the timestamp at `EndOfWindowIndex`. On consecutive loads, the window end time advances by `historyDuration`.

To process the stream, repeatedly call `LoadWindow`, store the returned position only on success, and pass it into the next call. Stop on `ErrEndOfData`. Surface other errors without advancing the stored position.

## For Interviewer

Provide the above 'for candidate' section to interviewee.
The code has an intentional bug in it:

- Resumed loads reprocess the resume boundary record. `StartIndex` identifies the first record that crossed the next-load-start time, but the next call starts from that same index, causing that boundary record to be processed twice across repeated loads.

### In-Person Follow-Up

Implement a higher-level manager that repeatedly calls `LoadWindow`, as described below.
Add async cancellation support so an ongoing load can be stopped cleanly partway through processing.

### Higher-Level Manager Behaviour

A higher-level manager is responsible for calling `LoadWindow` repeatedly. It should keep the latest load position, pass it into the next call, and store the returned end-of-window index and start index only after a successful load.

The manager should advance by using the indexes returned by the loader, not by guessing or recalculating them itself. 

The manager should stop cleanly when the loader reports end of the data. Other loader errors should be surfaced to the caller and should not advance the stored indexes.

### Async Cancellation

There are multiple acceptable ways to achieve this. For example, pass `context.Context` into the loader and check for cancellation after each batch.

