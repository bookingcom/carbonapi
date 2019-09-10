package date

import (
	"fmt"
	"testing"
	"time"
)

func TestDateParamToEpoch(t *testing.T) {

	defaultTimeZone := time.Local
	timeNow = func() time.Time {
		//16 Aug 1994 15:30
		return time.Date(1994, time.August, 16, 15, 30, 0, 100, defaultTimeZone)
	}

	const shortForm = "15:04 2006-Jan-02"
	const defaultTsStr = "17:30 2019-Apr-25"

	var tests = []struct {
		input  string
		output string
		error  bool
	}{
		{"midnight", "00:00 1994-Aug-16", false},
		{"noon", "12:00 1994-Aug-16", false},
		{"teatime", "16:00 1994-Aug-16", false},
		{"tomorrow", "00:00 1994-Aug-17", false},

		{"noon 08/12/94", "12:00 1994-Aug-12", false},
		{"midnight 20060812", "00:00 2006-Aug-12", false},
		{"noon tomorrow", "12:00 1994-Aug-17", false},

		{"17:04 19940812", "17:04 1994-Aug-12", false},
		{"-1day", "15:30 1994-Aug-15", false},
		{"19940812", "00:00 1994-Aug-12", false},
		{"now", "15:30 1994-Aug-16", false},
		{"hh:mm 19940812", "00:00 1994-Aug-12", true},
		{"12:30:00 19940812", "00:00 1994-Aug-12", true},
		{"12:mm 19940812", "00:00 1994-Aug-12", true},
		{"today", "00:00 1994-Aug-16", false},
		{"yesterday", "00:00 1994-Aug-15", false},
		{"1556201160", "16:06 2019-Apr-25", false},
		{"", defaultTsStr, false},
		{"-something", defaultTsStr, true},
		{"17:04 19940812 1001", defaultTsStr, true},
		{"12:30 08/15/06", "12:30 2006-Aug-15", false},
		{"12:30 08-15-06", defaultTsStr, true},
		{"08/15/06 12:30", defaultTsStr, true},
		{"+5m", defaultTsStr, true},
	}

	defaultTime, _ := time.ParseInLocation(shortForm, defaultTsStr, defaultTimeZone)
	defaultTs := defaultTime.Unix()

	for _, tt := range tests {
		got, parseErr := DateParamToEpoch(tt.input, "Local", defaultTs, defaultTimeZone)
		ts, err := time.ParseInLocation(shortForm, tt.output, defaultTimeZone)
		if err != nil {
			panic(fmt.Sprintf("error parsing time: %q: %v", tt.output, err))
		}
		if (tt.error && parseErr == nil) || (!tt.error && parseErr != nil) {
			t.Errorf("dateParamToEpoch(%q, 0)=%v, want error: %v", tt.input, got, tt.error)
			continue
		}

		if !tt.error {
			want := int32(ts.Unix())
			if got != want {
				t.Errorf("dateParamToEpoch(%q, 0)=%v, want %v", tt.input, got, want)
			}
		}
	}
}
