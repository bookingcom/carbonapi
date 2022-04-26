package functions

import (
	"go.uber.org/zap"
	"sort"
	"strings"

	"github.com/bookingcom/carbonapi/expr/functions/absolute"
	"github.com/bookingcom/carbonapi/expr/functions/alias"
	"github.com/bookingcom/carbonapi/expr/functions/aliasByMetric"
	"github.com/bookingcom/carbonapi/expr/functions/aliasByNode"
	"github.com/bookingcom/carbonapi/expr/functions/aliasSub"
	"github.com/bookingcom/carbonapi/expr/functions/applyByNode"
	"github.com/bookingcom/carbonapi/expr/functions/asPercent"
	"github.com/bookingcom/carbonapi/expr/functions/averageSeries"
	"github.com/bookingcom/carbonapi/expr/functions/averageSeriesWithWildcards"
	"github.com/bookingcom/carbonapi/expr/functions/below"
	"github.com/bookingcom/carbonapi/expr/functions/cactiStyle"
	"github.com/bookingcom/carbonapi/expr/functions/cairo"
	"github.com/bookingcom/carbonapi/expr/functions/changed"
	"github.com/bookingcom/carbonapi/expr/functions/consolidateBy"
	"github.com/bookingcom/carbonapi/expr/functions/constantLine"
	"github.com/bookingcom/carbonapi/expr/functions/countSeries"
	"github.com/bookingcom/carbonapi/expr/functions/cumulative"
	"github.com/bookingcom/carbonapi/expr/functions/delay"
	"github.com/bookingcom/carbonapi/expr/functions/derivative"
	"github.com/bookingcom/carbonapi/expr/functions/diffSeries"
	"github.com/bookingcom/carbonapi/expr/functions/divideSeries"
	"github.com/bookingcom/carbonapi/expr/functions/ewma"
	"github.com/bookingcom/carbonapi/expr/functions/exclude"
	"github.com/bookingcom/carbonapi/expr/functions/fallbackSeries"
	"github.com/bookingcom/carbonapi/expr/functions/fft"
	"github.com/bookingcom/carbonapi/expr/functions/filterSeries"
	"github.com/bookingcom/carbonapi/expr/functions/grep"
	"github.com/bookingcom/carbonapi/expr/functions/group"
	"github.com/bookingcom/carbonapi/expr/functions/groupByNode"
	"github.com/bookingcom/carbonapi/expr/functions/highest"
	"github.com/bookingcom/carbonapi/expr/functions/hitcount"
	"github.com/bookingcom/carbonapi/expr/functions/holtWintersAberration"
	"github.com/bookingcom/carbonapi/expr/functions/holtWintersConfidenceBands"
	"github.com/bookingcom/carbonapi/expr/functions/holtWintersForecast"
	"github.com/bookingcom/carbonapi/expr/functions/ifft"
	"github.com/bookingcom/carbonapi/expr/functions/integral"
	"github.com/bookingcom/carbonapi/expr/functions/integralByInterval"
	"github.com/bookingcom/carbonapi/expr/functions/invert"
	"github.com/bookingcom/carbonapi/expr/functions/isNotNull"
	"github.com/bookingcom/carbonapi/expr/functions/keepLastValue"
	"github.com/bookingcom/carbonapi/expr/functions/kolmogorovSmirnovTest2"
	"github.com/bookingcom/carbonapi/expr/functions/legendValue"
	"github.com/bookingcom/carbonapi/expr/functions/limit"
	"github.com/bookingcom/carbonapi/expr/functions/linearRegression"
	"github.com/bookingcom/carbonapi/expr/functions/logarithm"
	"github.com/bookingcom/carbonapi/expr/functions/lowPass"
	"github.com/bookingcom/carbonapi/expr/functions/lowest"
	"github.com/bookingcom/carbonapi/expr/functions/mapSeries"
	"github.com/bookingcom/carbonapi/expr/functions/medianSeries"
	"github.com/bookingcom/carbonapi/expr/functions/minMax"
	"github.com/bookingcom/carbonapi/expr/functions/mostDeviant"
	"github.com/bookingcom/carbonapi/expr/functions/moving"
	"github.com/bookingcom/carbonapi/expr/functions/movingMedian"
	"github.com/bookingcom/carbonapi/expr/functions/multiplySeries"
	"github.com/bookingcom/carbonapi/expr/functions/multiplySeriesWithWildcards"
	"github.com/bookingcom/carbonapi/expr/functions/nPercentile"
	"github.com/bookingcom/carbonapi/expr/functions/nonNegativeDerivative"
	"github.com/bookingcom/carbonapi/expr/functions/offset"
	"github.com/bookingcom/carbonapi/expr/functions/offsetToZero"
	"github.com/bookingcom/carbonapi/expr/functions/pearson"
	"github.com/bookingcom/carbonapi/expr/functions/pearsonClosest"
	"github.com/bookingcom/carbonapi/expr/functions/perSecond"
	"github.com/bookingcom/carbonapi/expr/functions/percentileOfSeries"
	"github.com/bookingcom/carbonapi/expr/functions/polyfit"
	"github.com/bookingcom/carbonapi/expr/functions/pow"
	"github.com/bookingcom/carbonapi/expr/functions/randomWalk"
	"github.com/bookingcom/carbonapi/expr/functions/rangeOfSeries"
	"github.com/bookingcom/carbonapi/expr/functions/reduce"
	"github.com/bookingcom/carbonapi/expr/functions/removeBelowSeries"
	"github.com/bookingcom/carbonapi/expr/functions/removeEmptySeries"
	"github.com/bookingcom/carbonapi/expr/functions/scale"
	"github.com/bookingcom/carbonapi/expr/functions/scaleToSeconds"
	"github.com/bookingcom/carbonapi/expr/functions/seriesList"
	"github.com/bookingcom/carbonapi/expr/functions/sortBy"
	"github.com/bookingcom/carbonapi/expr/functions/sortByName"
	"github.com/bookingcom/carbonapi/expr/functions/squareRoot"
	"github.com/bookingcom/carbonapi/expr/functions/stddevSeries"
	"github.com/bookingcom/carbonapi/expr/functions/stdev"
	"github.com/bookingcom/carbonapi/expr/functions/substr"
	"github.com/bookingcom/carbonapi/expr/functions/sum"
	"github.com/bookingcom/carbonapi/expr/functions/sumSeriesWithWildcards"
	"github.com/bookingcom/carbonapi/expr/functions/summarize"
	"github.com/bookingcom/carbonapi/expr/functions/timeFunction"
	"github.com/bookingcom/carbonapi/expr/functions/timeLag"
	"github.com/bookingcom/carbonapi/expr/functions/timeShift"
	"github.com/bookingcom/carbonapi/expr/functions/timeStack"
	"github.com/bookingcom/carbonapi/expr/functions/transformNull"
	"github.com/bookingcom/carbonapi/expr/functions/tukey"
	"github.com/bookingcom/carbonapi/expr/functions/weightedAverage"
	"github.com/bookingcom/carbonapi/expr/interfaces"
	"github.com/bookingcom/carbonapi/expr/metadata"
)

type initFunc struct {
	name  string
	order interfaces.Order
	f     func(configFile string) []interfaces.FunctionMetadata
}

func New(configs map[string]string, logger *zap.Logger) {
	funcs := make([]initFunc, 0, 87)

	funcs = append(funcs, initFunc{name: "absolute", order: absolute.GetOrder(), f: absolute.New})

	funcs = append(funcs, initFunc{name: "alias", order: alias.GetOrder(), f: alias.New})

	funcs = append(funcs, initFunc{name: "aliasByMetric", order: aliasByMetric.GetOrder(), f: aliasByMetric.New})

	funcs = append(funcs, initFunc{name: "aliasByNode", order: aliasByNode.GetOrder(), f: aliasByNode.New})

	funcs = append(funcs, initFunc{name: "aliasSub", order: aliasSub.GetOrder(), f: aliasSub.New})

	funcs = append(funcs, initFunc{name: "applyByNode", order: applyByNode.GetOrder(), f: applyByNode.New})

	funcs = append(funcs, initFunc{name: "asPercent", order: asPercent.GetOrder(), f: asPercent.New})

	funcs = append(funcs, initFunc{name: "averageSeries", order: averageSeries.GetOrder(), f: averageSeries.New})

	funcs = append(funcs, initFunc{name: "averageSeriesWithWildcards", order: averageSeriesWithWildcards.GetOrder(), f: averageSeriesWithWildcards.New})

	funcs = append(funcs, initFunc{name: "below", order: below.GetOrder(), f: below.New})

	funcs = append(funcs, initFunc{name: "cactiStyle", order: cactiStyle.GetOrder(), f: cactiStyle.New})

	funcs = append(funcs, initFunc{name: "cairo", order: cairo.GetOrder(), f: cairo.New})

	funcs = append(funcs, initFunc{name: "changed", order: changed.GetOrder(), f: changed.New})

	funcs = append(funcs, initFunc{name: "consolidateBy", order: consolidateBy.GetOrder(), f: consolidateBy.New})

	funcs = append(funcs, initFunc{name: "constantLine", order: constantLine.GetOrder(), f: constantLine.New})

	funcs = append(funcs, initFunc{name: "countSeries", order: countSeries.GetOrder(), f: countSeries.New})

	funcs = append(funcs, initFunc{name: "cumulative", order: cumulative.GetOrder(), f: cumulative.New})

	funcs = append(funcs, initFunc{name: "delay", order: delay.GetOrder(), f: delay.New})

	funcs = append(funcs, initFunc{name: "derivative", order: derivative.GetOrder(), f: derivative.New})

	funcs = append(funcs, initFunc{name: "diffSeries", order: diffSeries.GetOrder(), f: diffSeries.New})

	funcs = append(funcs, initFunc{name: "divideSeries", order: divideSeries.GetOrder(), f: divideSeries.New})

	funcs = append(funcs, initFunc{name: "ewma", order: ewma.GetOrder(), f: ewma.New})

	funcs = append(funcs, initFunc{name: "exclude", order: exclude.GetOrder(), f: exclude.New})

	funcs = append(funcs, initFunc{name: "fallbackSeries", order: fallbackSeries.GetOrder(), f: fallbackSeries.New})

	funcs = append(funcs, initFunc{name: "fft", order: fft.GetOrder(), f: fft.New})

	funcs = append(funcs, initFunc{name: "filterSeries", order: filterSeries.GetOrder(), f: filterSeries.New})

	funcs = append(funcs, initFunc{name: "grep", order: grep.GetOrder(), f: grep.New})

	funcs = append(funcs, initFunc{name: "group", order: group.GetOrder(), f: group.New})

	funcs = append(funcs, initFunc{name: "groupByNode", order: groupByNode.GetOrder(), f: groupByNode.New})

	funcs = append(funcs, initFunc{name: "highest", order: highest.GetOrder(), f: highest.New})

	funcs = append(funcs, initFunc{name: "hitcount", order: hitcount.GetOrder(), f: hitcount.New})

	funcs = append(funcs, initFunc{name: "holtWintersAberration", order: holtWintersAberration.GetOrder(), f: holtWintersAberration.New})

	funcs = append(funcs, initFunc{name: "holtWintersConfidenceBands", order: holtWintersConfidenceBands.GetOrder(), f: holtWintersConfidenceBands.New})

	funcs = append(funcs, initFunc{name: "holtWintersForecast", order: holtWintersForecast.GetOrder(), f: holtWintersForecast.New})

	funcs = append(funcs, initFunc{name: "ifft", order: ifft.GetOrder(), f: ifft.New})

	funcs = append(funcs, initFunc{name: "integral", order: integral.GetOrder(), f: integral.New})

	funcs = append(funcs, initFunc{name: "integralByInterval", order: integralByInterval.GetOrder(), f: integralByInterval.New})

	funcs = append(funcs, initFunc{name: "invert", order: invert.GetOrder(), f: invert.New})

	funcs = append(funcs, initFunc{name: "isNotNull", order: isNotNull.GetOrder(), f: isNotNull.New})

	funcs = append(funcs, initFunc{name: "keepLastValue", order: keepLastValue.GetOrder(), f: keepLastValue.New})

	funcs = append(funcs, initFunc{name: "kolmogorovSmirnovTest2", order: kolmogorovSmirnovTest2.GetOrder(), f: kolmogorovSmirnovTest2.New})

	funcs = append(funcs, initFunc{name: "legendValue", order: legendValue.GetOrder(), f: legendValue.New})

	funcs = append(funcs, initFunc{name: "limit", order: limit.GetOrder(), f: limit.New})

	funcs = append(funcs, initFunc{name: "linearRegression", order: linearRegression.GetOrder(), f: linearRegression.New})

	funcs = append(funcs, initFunc{name: "logarithm", order: logarithm.GetOrder(), f: logarithm.New})

	funcs = append(funcs, initFunc{name: "lowPass", order: lowPass.GetOrder(), f: lowPass.New})

	funcs = append(funcs, initFunc{name: "lowest", order: lowest.GetOrder(), f: lowest.New})

	funcs = append(funcs, initFunc{name: "mapSeries", order: mapSeries.GetOrder(), f: mapSeries.New})

	funcs = append(funcs, initFunc{name: "medianSeries", order: medianSeries.GetOrder(), f: medianSeries.New})

	funcs = append(funcs, initFunc{name: "minMax", order: minMax.GetOrder(), f: minMax.New})

	funcs = append(funcs, initFunc{name: "mostDeviant", order: mostDeviant.GetOrder(), f: mostDeviant.New})

	funcs = append(funcs, initFunc{name: "moving", order: moving.GetOrder(), f: moving.New})

	funcs = append(funcs, initFunc{name: "movingMedian", order: movingMedian.GetOrder(), f: movingMedian.New})

	funcs = append(funcs, initFunc{name: "multiplySeries", order: multiplySeries.GetOrder(), f: multiplySeries.New})

	funcs = append(funcs, initFunc{name: "multiplySeriesWithWildcards", order: multiplySeriesWithWildcards.GetOrder(), f: multiplySeriesWithWildcards.New})

	funcs = append(funcs, initFunc{name: "nPercentile", order: nPercentile.GetOrder(), f: nPercentile.New})

	funcs = append(funcs, initFunc{name: "nonNegativeDerivative", order: nonNegativeDerivative.GetOrder(), f: nonNegativeDerivative.New})

	funcs = append(funcs, initFunc{name: "offset", order: offset.GetOrder(), f: offset.New})

	funcs = append(funcs, initFunc{name: "offsetToZero", order: offsetToZero.GetOrder(), f: offsetToZero.New})

	funcs = append(funcs, initFunc{name: "pearson", order: pearson.GetOrder(), f: pearson.New})

	funcs = append(funcs, initFunc{name: "pearsonClosest", order: pearsonClosest.GetOrder(), f: pearsonClosest.New})

	funcs = append(funcs, initFunc{name: "perSecond", order: perSecond.GetOrder(), f: perSecond.New})

	funcs = append(funcs, initFunc{name: "percentileOfSeries", order: percentileOfSeries.GetOrder(), f: percentileOfSeries.New})

	funcs = append(funcs, initFunc{name: "polyfit", order: polyfit.GetOrder(), f: polyfit.New})

	funcs = append(funcs, initFunc{name: "pow", order: pow.GetOrder(), f: pow.New})

	funcs = append(funcs, initFunc{name: "randomWalk", order: randomWalk.GetOrder(), f: randomWalk.New})

	funcs = append(funcs, initFunc{name: "rangeOfSeries", order: rangeOfSeries.GetOrder(), f: rangeOfSeries.New})

	funcs = append(funcs, initFunc{name: "reduce", order: reduce.GetOrder(), f: reduce.New})

	funcs = append(funcs, initFunc{name: "removeBelowSeries", order: removeBelowSeries.GetOrder(), f: removeBelowSeries.New})

	funcs = append(funcs, initFunc{name: "removeEmptySeries", order: removeEmptySeries.GetOrder(), f: removeEmptySeries.New})

	funcs = append(funcs, initFunc{name: "scale", order: scale.GetOrder(), f: scale.New})

	funcs = append(funcs, initFunc{name: "scaleToSeconds", order: scaleToSeconds.GetOrder(), f: scaleToSeconds.New})

	funcs = append(funcs, initFunc{name: "seriesList", order: seriesList.GetOrder(), f: seriesList.New})

	funcs = append(funcs, initFunc{name: "sortBy", order: sortBy.GetOrder(), f: sortBy.New})

	funcs = append(funcs, initFunc{name: "sortByName", order: sortByName.GetOrder(), f: sortByName.New})

	funcs = append(funcs, initFunc{name: "squareRoot", order: squareRoot.GetOrder(), f: squareRoot.New})

	funcs = append(funcs, initFunc{name: "stddevSeries", order: stddevSeries.GetOrder(), f: stddevSeries.New})

	funcs = append(funcs, initFunc{name: "stdev", order: stdev.GetOrder(), f: stdev.New})

	funcs = append(funcs, initFunc{name: "substr", order: substr.GetOrder(), f: substr.New})

	funcs = append(funcs, initFunc{name: "sum", order: sum.GetOrder(), f: sum.New})

	funcs = append(funcs, initFunc{name: "sumSeriesWithWildcards", order: sumSeriesWithWildcards.GetOrder(), f: sumSeriesWithWildcards.New})

	funcs = append(funcs, initFunc{name: "summarize", order: summarize.GetOrder(), f: summarize.New})

	funcs = append(funcs, initFunc{name: "timeFunction", order: timeFunction.GetOrder(), f: timeFunction.New})

	funcs = append(funcs, initFunc{name: "timeLag", order: timeLag.GetOrder(), f: timeLag.New})

	funcs = append(funcs, initFunc{name: "timeShift", order: timeShift.GetOrder(), f: timeShift.New})

	funcs = append(funcs, initFunc{name: "timeStack", order: timeStack.GetOrder(), f: timeStack.New})

	funcs = append(funcs, initFunc{name: "transformNull", order: transformNull.GetOrder(), f: transformNull.New})

	funcs = append(funcs, initFunc{name: "tukey", order: tukey.GetOrder(), f: tukey.New})

	funcs = append(funcs, initFunc{name: "weightedAverage", order: weightedAverage.GetOrder(), f: weightedAverage.New})

	sort.Slice(funcs, func(i, j int) bool {
		if funcs[i].order == interfaces.Any && funcs[j].order == interfaces.Last {
			return true
		}
		if funcs[i].order == interfaces.Last && funcs[j].order == interfaces.Any {
			return false
		}
		return funcs[i].name > funcs[j].name
	})

	for _, f := range funcs {
		md := f.f(configs[strings.ToLower(f.name)])
		for _, m := range md {
			metadata.RegisterFunction(m.Name, m.F, logger)
		}
	}
}
