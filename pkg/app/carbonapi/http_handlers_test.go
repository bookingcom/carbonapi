package carbonapi

import (
	"errors"
	"testing"

	typ "github.com/bookingcom/carbonapi/pkg/types"
	"go.uber.org/zap"
)

func TestGetCompleterQuery(t *testing.T) {
	metricTestCases := []string{"foo.bar", "foo/bar", "foo.b", "foo.", "/", "", "."}
	metricCompleterResponse := []string{"foo.bar*", "foo.bar*", "foo.b*", "foo.*", ".*", ".*", ".*"}

	for i, metricTestCase := range metricTestCases {
		response := getCompleterQuery(metricTestCase)
		if metricCompleterResponse[i] != response {
			t.Error("should be same")
		}
	}
}

func TestFindCompleter(t *testing.T) {
	metricTestCases := []typ.Matches{
		{Name: "foo.bar", Matches: []typ.Match{}},
		{Name: "foo.ba*", Matches: []typ.Match{
			{Path: "foo.bat", IsLeaf: true},
		}},
	}
	metricFindCompleterResponse := []string{
		"{\"metrics\":[]}\n",
		"{\"metrics\":[{\"path\":\"foo.bat\",\"name\":\"bat\",\"is_leaf\":\"1\"}]}\n",
	}

	for i, metricTestCase := range metricTestCases {
		response, _ := findCompleter(metricTestCase)
		if metricFindCompleterResponse[i] != string(response) {
			t.Error("should be same")
		}
	}

}

func TestExpandEncoder(t *testing.T) {
	var tests = []struct {
		name        string
		metricIn    []typ.Matches
		metricOut   string
		leavesOnly  bool
		groupByExpr bool
	}{
		{
			name: "test1",
			metricIn: []typ.Matches{
				{
					Name: "foo.ba*",
					Matches: []typ.Match{
						{Path: "foo.bar", IsLeaf: false},
						{Path: "foo.bat", IsLeaf: true},
					},
				},
			},
			metricOut:   "{\"results\":[\"foo.bar\",\"foo.bat\"]}\n",
			leavesOnly:  false,
			groupByExpr: false,
		},
		{
			name: "test2",
			metricIn: []typ.Matches{
				{
					Name: "foo.ba*",
					Matches: []typ.Match{
						{Path: "foo.bar", IsLeaf: false},
						{Path: "foo.bat", IsLeaf: true},
					},
				},
			},
			metricOut:   "{\"results\":[\"foo.bat\"]}\n",
			leavesOnly:  true,
			groupByExpr: false,
		},
		{
			name: "test3",
			metricIn: []typ.Matches{
				{
					Name: "foo.ba*",
					Matches: []typ.Match{
						{Path: "foo.bar", IsLeaf: false},
						{Path: "foo.bat", IsLeaf: true},
					},
				},
			},
			metricOut:   "{\"results\":{\"foo.ba*\":[\"foo.bar\",\"foo.bat\"]}}\n",
			leavesOnly:  false,
			groupByExpr: true,
		},
		{
			name: "test4",
			metricIn: []typ.Matches{
				{
					Name: "foo.ba*",
					Matches: []typ.Match{
						{Path: "foo.bar", IsLeaf: false},
						{Path: "foo.bat", IsLeaf: true},
					},
				},
				{
					Name: "foo.ba*.*",
					Matches: []typ.Match{
						{Path: "foo.bar", IsLeaf: false},
						{Path: "foo.bat", IsLeaf: true},
						{Path: "foo.bar.baz", IsLeaf: true},
					},
				},
			},
			metricOut:   "{\"results\":{\"foo.ba*\":[\"foo.bar\",\"foo.bat\"],\"foo.ba*.*\":[\"foo.bar.baz\"]}}\n",
			leavesOnly:  false,
			groupByExpr: true,
		},
	}
	for _, tst := range tests {
		tst := tst
		t.Run(tst.name, func(t *testing.T) {
			response, _ := expandEncoder(tst.metricIn, tst.leavesOnly, tst.groupByExpr)
			if tst.metricOut != string(response) {
				t.Errorf("%v should be same as %v", tst.metricOut, string(response))
			}
		})
	}
}

func TestOptimistErrsFanIn(t *testing.T) {
	var tests = []struct {
		name       string
		in         []error
		n          int
		isErr      bool
		isNotFound bool
	}{
		{
			name: "1 err, 1 result",
			in: []error{
				errors.New("some error"),
			},
			n:          1,
			isErr:      true,
			isNotFound: false,
		},
		{
			name: "1 not found err, 1 result",
			in: []error{
				typ.ErrMetricsNotFound,
			},
			n:          1,
			isErr:      true,
			isNotFound: true,
		},
		{
			name:       "no errs, no results",
			in:         []error{},
			n:          0,
			isErr:      false,
			isNotFound: false,
		},
		{
			name: "2 mixed errs, 2 results",
			in: []error{
				errors.New("some error"),
				typ.ErrMetricsNotFound,
			},
			n:          2,
			isErr:      true,
			isNotFound: false,
		},
		{
			name: "1 not found err, 2 results,",
			in: []error{
				typ.ErrMetricsNotFound,
			},
			n:          2,
			isErr:      false,
			isNotFound: false,
		},
		{
			name:       "no errs, many results",
			in:         []error{},
			n:          5,
			isErr:      false,
			isNotFound: false,
		},
		{
			name: "1 arbitrary err, 2 results",
			in: []error{
				errors.New("some err"),
			},
			n:          2,
			isErr:      false,
			isNotFound: false,
		},
	}

	for _, tst := range tests {
		tst := tst
		t.Run(tst.name, func(t *testing.T) {
			err, _ := optimistFanIn(tst.in, tst.n, "")

			if err != nil {
				if !tst.isErr {
					t.Fatal("got err, when none expected")
				}

				if _, ok := err.(typ.ErrNotFound); ok != tst.isNotFound {
					t.Fatalf("got err *%v* when not found err expected", err)
				}
			}
		})
	}
}

func TestMetricRefsPrefix(t *testing.T) {
	tests := []struct {
		metric string
		prefix string
		want   bool
	}{
		{"a.b.c.*", "a.b", true},
		{"a.*", "a.b", false},
		{"a.b", "a.b", true},
		{"a.bc", "a.b", false},
		{"a.b.c", "a", true},
		{"apple.x", "a", false},
		{"{a,x}.b.c", "a", false},
		{"a.b.c.d", "a.b", true},
		{"", "a", false},
		{"a", "a", true},
		{"a", "a.b.c", false},
	}

	for _, tt := range tests {
		got := metricRefsPrefix(tt.metric, tt.prefix)
		if got != tt.want {
			t.Errorf("metricRefsPrefix(%q, %q) = %v, want %v", tt.metric, tt.prefix, got, tt.want)
		}
	}
}

func TestCollectMatchedPrefixes(t *testing.T) {
	tests := []struct {
		name     string
		targets  []string
		prefixes []string
		want     []string // sorted expected matches; nil means no matches (empty or nil map)
	}{
		{
			name:     "no configured prefixes",
			targets:  []string{"a.b.c.*"},
			prefixes: nil,
			want:     nil,
		},
		{
			name:     "single target matches single prefix",
			targets:  []string{"a.b.c.*"},
			prefixes: []string{"a.b"},
			want:     []string{"a.b"},
		},
		{
			name:     "single target does not match prefix",
			targets:  []string{"x.y.z"},
			prefixes: []string{"a.b"},
			want:     nil,
		},
		{
			name:     "multiple targets, one matches",
			targets:  []string{"x.y.z", "a.b.c"},
			prefixes: []string{"a.b"},
			want:     []string{"a.b"},
		},
		{
			name:     "multiple targets match different prefixes",
			targets:  []string{"a.b.c", "x.y.z"},
			prefixes: []string{"a.b", "x.y"},
			want:     []string{"a.b", "x.y"},
		},
		{
			name:     "target with function expression",
			targets:  []string{"sum(a.b.c.*)"},
			prefixes: []string{"a.b"},
			want:     []string{"a.b"},
		},
		{
			name:     "unparseable target is skipped, valid target still counted",
			targets:  []string{"(((bad target", "a.b.c"},
			prefixes: []string{"a.b"},
			want:     []string{"a.b"},
		},
		{
			name:     "prefix only matched at dot boundary",
			targets:  []string{"a.bc.d"},
			prefixes: []string{"a.b"},
			want:     nil,
		},
		{
			name:     "exact match counts",
			targets:  []string{"a.b"},
			prefixes: []string{"a.b"},
			want:     []string{"a.b"},
		},
		{
			name:     "each prefix counted at most once across all targets",
			targets:  []string{"a.b.c", "a.b.d"},
			prefixes: []string{"a.b"},
			want:     []string{"a.b"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := collectMatchedPrefixes(tt.targets, tt.prefixes)
			if len(got) != len(tt.want) {
				t.Fatalf("collectMatchedPrefixes(%v, %v) = %v, want %v", tt.targets, tt.prefixes, got, tt.want)
			}
			for _, p := range tt.want {
				if _, ok := got[p]; !ok {
					t.Errorf("collectMatchedPrefixes(%v, %v): missing prefix %q in result %v", tt.targets, tt.prefixes, p, got)
				}
			}
		})
	}
}

func TestInitRequestPrefixes(t *testing.T) {
	lg := zap.NewNop()

	tests := []struct {
		name string
		in   []string
		want []string
	}{
		{"nil input", nil, nil},
		{"empty input", []string{}, nil},
		{"drops empty entries", []string{"a.b", "", "x"}, []string{"a.b", "x"}},
		{"drops glob entries", []string{"a.b", "a.*", "a.b{c,d}", "a?b", "x[1-2]"}, []string{"a.b"}},
		{"dedupes", []string{"a.b", "x", "a.b", "x"}, []string{"a.b", "x"}},
		{"preserves order", []string{"z", "a", "m"}, []string{"z", "a", "m"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := initRequestPrefixes(tt.in, lg)
			if len(got) != len(tt.want) {
				t.Fatalf("got %v, want %v", got, tt.want)
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Fatalf("got %v, want %v", got, tt.want)
				}
			}
		})
	}
}
