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
	}{
		{"midnight", "00:00 1994-Aug-16"},
		{"noon", "12:00 1994-Aug-16"},
		{"teatime", "16:00 1994-Aug-16"},
		{"tomorrow", "00:00 1994-Aug-17"},

		{"noon 08/12/94", "12:00 1994-Aug-12"},
		{"midnight 20060812", "00:00 2006-Aug-12"},
		{"noon tomorrow", "12:00 1994-Aug-17"},

		{"17:04 19940812", "17:04 1994-Aug-12"},
		{"-1day", "15:30 1994-Aug-15"},
		{"19940812", "00:00 1994-Aug-12"},
		{"now", "15:30 1994-Aug-16"},
		{"hh:mm 19940812", "00:00 1994-Aug-12"},
		{"12:30:00 19940812", "00:00 1994-Aug-12"},
		{"12:mm 19940812", "00:00 1994-Aug-12"},
		{"today", "00:00 1994-Aug-16"},
		{"yesterday", "00:00 1994-Aug-15"},
		{"1556201160", "16:06 2019-Apr-25"},
		{"", defaultTsStr},
		{"-something", defaultTsStr},
		{"17:04 19940812 1001", defaultTsStr},
		{"12:30 08/15/06", "12:30 2006-Aug-15"},
		{"12:30 08-15-06", defaultTsStr},
		{"08/15/06 12:30", defaultTsStr},
		{"+5m", defaultTsStr},
	}

	defaultTime, _ := time.ParseInLocation(shortForm, defaultTsStr, defaultTimeZone)
	defaultTs := defaultTime.Unix()

	for _, tt := range tests {
		got := DateParamToEpoch(tt.input, "Local", defaultTs, defaultTimeZone)
		ts, err := time.ParseInLocation(shortForm, tt.output, defaultTimeZone)
		if err != nil {
			panic(fmt.Sprintf("error parsing time: %q: %v", tt.output, err))
		}

		want := int32(ts.Unix())
		if got != want {
			t.Errorf("dateParamToEpoch(%q, 0)=%v, want %v", tt.input, got, want)
		}
	}
}
