package parser

import "time"

// TimeShiftOffset is shared by fetch planning and evaluation. A single shift
// preserves wall-clock time only when neither range crosses an offset change.
func TimeShiftOffset(e Expr, from, until int32, location *time.Location) (int32, error) {
	offs, err := e.GetIntervalArg(1, -1)
	if err != nil {
		return 0, err
	}
	key := "alignDST"
	if _, ok := e.NamedArgs()["alignDst"]; ok {
		key = "alignDst" // graphite-web's parameter spelling
	}
	alignDST, err := e.GetBoolNamedOrPosArgDefault(key, 3, false)
	if err != nil {
		return 0, err
	}
	if !alignDST {
		return offs, nil
	}
	if location == nil {
		location = time.Local
	}
	start := time.Unix(int64(from), 0).In(location)
	end := time.Unix(int64(until), 0).In(location)
	shiftedStart := start.Add(time.Duration(offs) * time.Second)
	shiftedEnd := end.Add(time.Duration(offs) * time.Second)
	_, startOffset := start.Zone()
	_, endOffset := end.Zone()
	_, shiftedStartOffset := shiftedStart.Zone()
	_, shiftedEndOffset := shiftedEnd.Zone()
	_, nextChange := start.ZoneBounds()
	_, shiftedNextChange := shiftedStart.ZoneBounds()
	if startOffset == endOffset && shiftedStartOffset == shiftedEndOffset &&
		(nextChange.IsZero() || end.Before(nextChange)) &&
		(shiftedNextChange.IsZero() || shiftedEnd.Before(shiftedNextChange)) {
		offs += int32(startOffset - shiftedStartOffset)
	}
	return offs, nil
}
