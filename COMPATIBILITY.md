# CarbonAPI compatibility with Graphite

- [CarbonAPI compatibility with Graphite](#carbonapi-compatibility-with-graphite)
  - [Default Settings](#default-settings)
    - [Default Line Colors](#default-line-colors)
  - [URI Parameters](#uri-parameters)
    - [/render/?...](#render)
    - [/metrics/find/?](#metricsfind)
  - [Functions diff compared to `graphite-web` v1.1.5](#functions-diff-compared-to-graphite-web-v115)
    - [Functions *present in graphite-web but absent in carbonapi*](#functions-present-in-graphite-web-but-absent-in-carbonapi)
    - [Functions *present in carbonapi but absent in graphite-web*](#functions-present-in-carbonapi-but-absent-in-graphite-web)
  - [Function short docs](#function-short-docs)

## Default Settings

### Default Line Colors

Default colors for png or svg rendering intentionally specified like it is in graphite-web 1.1.0

You can redefine that in config to be more more precise. In default config example they are defined in the same way as in [original graphite PR to make them right](https://github.com/graphite-project/graphite-web/pull/2239)

Reason behind that change is that on dark background it's much nicer to read old colors than new one

## URI Parameters

### /render/?...

* `target` : graphite series, seriesList or function (likely containing series or seriesList)
* `from`, `until` : time specifiers. Eg. "1d", "10min", "04:37_20150822", "now", "today", ... (**NOTE** does not handle timezones the same as graphite)
* `format` : support graphite values of { json, raw, pickle, csv, png, svg } adds { protobuf } and does not support { pdf }
* `jsonp` : (...)
* `noCache` : prevent query-response caching (which is 60s if enabled)
* `cacheTimeout` : override default result cache (60s)
* `rawdata` -or- `rawData` : true for `format=raw`

**Explicitly NOT supported**
* `_salt`
* `_ts`
* `_t`

_When `format=png`_ (default if not specified)
* `width`, `height` : number of pixels (default: width=330 , height=250)
* `pixelRatio` : (1.0)
* `margin` : (10)
* `logBase` : Y-scale should use. Recognizes "e" or a floating point ( >= 1 )
* `fgcolor` : foreground color
* `bgcolor` : background color
* `majorLine` : major line color
* `minorLine` : minor line color
* `fontName` : ("Sans")
* `fontSize` : (10.0)
* `fontBold` : (false)
* `fontItalic` : (false)
* `graphOnly` : (false)
* `hideLegend` : (false) (**NOTE** if not defined and >10 result metrics this becomes true)
* `hideGrid` : (false)
* `hideAxes` : (false)
* `hideYAxis` : (false)
* `hideXAxis` : (false)
* `yAxisSide` : ("left")
* `connectedLimit` : number of missing points to bridge when `linemode` is not one of { "slope", "staircase" } likely "connected" (4294967296)
* `lineMode` : ("slope")
* `areaMode` : ("none") also recognizes { "first", "all", "stacked" }
* `areaAlpha` : ( <not defined> ) float value for area alpha
* `pieMode` : ("average") also recognizes { "maximum", "minimum" } (**NOTE** pie graph support is explicitly unplanned)
* `lineWidth` : (1.2) float value for line width
* `dashed` : (false) dashed lines
* `rightWidth` : (1.2) ...
* `rightDashed` : (false)
* `rightColor` : ...
* `leftWidth` : (1.2)
* `leftDashed` : (false)
* `leftColor` : ...
* `title` : ("") graph title
* `vtitle` : ("") ...
* `vtitleRight` : ("") ...
* `colorList` : ("blue,green,red,purple,yellow,aqua,grey,magenta,pink,gold,rose")
* `majorGridLineColor` : ("rose")
* `minorGridLineColor` : ("grey")
* `uniqueLegend` : (false)
* `drawNullAsZero` : (false) (**NOTE** affects display only - does not translate missing values to zero in functions. For that use ...)
* `drawAsInfinite` : (false) ...
* `yMin` : <undefined>
* `yMax` : <undefined>
* `yStep` : <undefined>
* `xMin` : <undefined>
* `xMax` : <undefined>
* `xStep` : <undefined>
* `xFormat` : ("") ...
* `minorY` : (1) ...
* `yMinLeft` : <undefined>
* `yMinRight` : <undefined>
* `yMaxLeft` : <undefined>
* `yMaxRight` : <undefined>
* `yStepL` : <undefined>
* `ySTepR` : <undefined>
* `yLimitLeft` : <undefined>
* `yLimitRight` : <undefined>
* `yUnitSystem` : ("si") also recognizes { "binary" }
* `yDivisors` : (4,5,6) ...

### /metrics/find/?

* `format` : ("treejson") also recognizes { "json" (same as "treejson"), "completer", "raw" }
* `jsonp` : ...
* `query` : the metric or glob-pattern to find


## Functions diff compared to `graphite-web` v1.1.5

### Functions *present in graphite-web but absent in carbonapi*

- aggregate
- aggregateLine
- aggregateWithWildcards
- aliasByTags
- aliasQuery
- averageOutsidePercentile
- events
- exponentialMovingAverage
- filterSeries
- groupByTags
- highest
- holtWintersConfidenceArea
- identity
- interpolate
- lowest
- minMax
- movingWindow
- pct
- powSeries
- removeBetweenPercentile
- round
- seriesByTag
- setXFilesFactor
- sin
- sinFunction
- smartSummarize
- sortBy
- timeSlice
- unique
- useSeriesAbove
- verticalLine
- xFilesFactor

### Functions *present in carbonapi but absent in graphite-web*

- diffSeriesLists
- ewma
- exponentialWeightedMovingAverage
- fft
- ifft
- isNotNull
- kolmogorovSmirnovTest2
- ksTest2
- log
- lowPass
- lpf
- maxSeries
- minSeries
- multiplySeriesLists
- pearson
- pearsonClosest
- polyfit
- powSeriesLists
- removeZeroSeries
- stdev
- timeLagSeries
- timeLagSeriesLists
- tukeyAbove
- tukeyBelow

## Function short docs

| Graphite Function                                                         |
| :------------------------------------------------------------------------ |
| absolute(seriesList)                                                      |
| alias(seriesList, newName)                                                |
| aliasByMetric(seriesList)                                                 |
| aliasByNode(seriesList, *nodes)                                           |
| aliasSub(seriesList, search, replace)                                     |
| alpha(seriesList, alpha)                                                  |
| applyByNode(seriesList, nodeNum, templateFunction, newName=None)          |
| areaBetween(seriesList)                                                   |
| asPercent(seriesList, total=None, *nodes)                                 |
| averageAbove(seriesList, n)                                               |
| averageBelow(seriesList, n)                                               |
| averageSeries(*seriesLists), Short Alias: avg()                           |
| averageSeriesWithWildcards(seriesList, *position)                         |
| cactiStyle(seriesList, system=None)                                       |
| changed(seriesList)                                                       |
| color(seriesList, theColor)                                               |
| consolidateBy(seriesList, consolidationFunc)                              |
| constantLine(value)                                                       |
| countSeries(*seriesLists)                                                 |
| cumulative(seriesList)                                                    |
| currentAbove(seriesList, n)                                               |
| currentBelow(seriesList, n)                                               |
| dashed(*seriesList)                                                       |
| delay(seriesList, steps)                                                  |
| derivative(seriesList)                                                    |
| diffSeries(*seriesLists)                                                  |
| divideSeries(dividendSeriesList, divisorSeries)                           |
| divideSeriesLists(dividendSeriesList, divisorSeriesList)                  |
| diffSeriesLists(leftSeriesList, rightSeriesList)                          |
| multiplySeriesLists(leftSeriesList, rightSeriesList)                      |
| drawAsInfinite(seriesList)                                                |
| exclude(seriesList, pattern)                                              |
| exponentialWeightedMovingAverage(seriesList, alpha)                       |
| ewma(seriesList, alpha)                                                   |
| fallbackSeries( seriesList, fallback )                                    |
| [fft](https://en.wikipedia.org/wiki/Fast_Fourier_transform)(absSeriesList, phaseSeriesList) |
| grep(seriesList, pattern)                                                 |
| group(*seriesLists)                                                       |
| groupByNode(seriesList, nodeNum, callback)                                |
| groupByNodes(seriesList, callback, *nodes)                                |
| highestAverage(seriesList, n)                                             |
| highestCurrent(seriesList, n)                                             |
| highestMax(seriesList, n)                                                 |
| hitcount(seriesList, intervalString, alignToInterval=False)               |
| holtWintersAberration(seriesList, delta=3)                                |
| holtWintersConfidenceArea(seriesList, delta=3)                            |
| holtWintersConfidenceBands(seriesList, delta=3)                           |
| holtWintersForecast(seriesList)                                           |
| [ifft](https://en.wikipedia.org/wiki/Fast_Fourier_transform)(absSeriesList, phaseSeriesList) |
| integral(seriesList)                                                      |
| integralByInterval(seriesList, intervalString)                                                      |
| invert(seriesList)                                                        |
| isNonNull(seriesList)                                                     |
| keepLastValue(seriesList, limit=inf)                                      |
| [kolmogorovSmirnovTest2](https://en.wikipedia.org/wiki/Kolmogorov%E2%80%93Smirnov_test)(series, series, windowSize) alias ksTest2()   |
| legendValue(seriesList, *valueTypes)                                      |
| limit(seriesList, n)                                                      |
| lineWidth(seriesList, width)                                              |
| linearRegression(seriesList, startSourceAt=None, endSourceAt=None)        |
| logarithm(seriesList, base=10), alias log()                               |
| lowestAverage(seriesList, n)                                              |
| lowestCurrent(seriesList, n)                                              |
| [lowPass](https://en.wikipedia.org/wiki/Low-pass_filter)(seriesList, cutPercent) |
| mapSeries(seriesList, mapNode), Short form: map()                         |
| maxSeries(*seriesLists)                                                   |
| maximumAbove(seriesList, n)                                               |
| maximumBelow(seriesList, n)                                               |
| minSeries(*seriesLists)                                                   |
| minimumAbove(seriesList, n)                                               |
| minimumBelow(seriesList, n)                                               |
| mostDeviant(seriesList, n)                                                |
| movingAverage(seriesList, windowSize)                                     |
| movingMax(seriesList, windowSize)                                         |
| movingMedian(seriesList, windowSize)                                      |
| movingMin(seriesList, windowSize)                                         |
| movingSum(seriesList, windowSize)                                         |
| multiplySeries(*seriesLists)                                              |
| multiplySeriesWithWildcards(seriesList, *position)                        |
| nPercentile(seriesList, n)                                                |
| nonNegativeDerivative(seriesList, maxValue=None)                          |
| offset(seriesList, factor)                                                |
| offsetToZero(seriesList)                                                  |
| [pearson](https://en.wikipedia.org/wiki/Pearson_product-moment_correlation_coefficient)(series, series, n) |
| pearsonClosest(series, seriesList, windowSize, direction="abs")           |
| perSecond(seriesList, maxValue=None)                                      |
| percentileOfSeries(seriesList, n, interpolate=False)                      |
| [polyfit](https://en.wikipedia.org/wiki/Polynomial_regression)(seriesList, degree=1, offset='0d') |
| pow(seriesList, factor)                                                   |
| randomWalkFunction(name, step=60), Short Alias: randomWalk()              |
| rangeOfSeries(*seriesLists)                                               |
| reduceSeries(seriesLists, reduceFunction, reduceNode, *reduceMatchers)    |
| reduce()                                                                  |
| removeAbovePercentile(seriesList, n)                                      |
| removeAboveValue(seriesList, n)                                           |
| removeBelowPercentile(seriesList, n)                                      |
| removeBelowValue(seriesList, n)                                           |
| removeEmptySeries(seriesList)                                             |
| removeZeroSeries(seriesList)                                              |
| scale(seriesList, factor)                                                 |
| scaleToSeconds(seriesList, seconds)                                       |
| secondYAxis(seriesList)                                                   |
| sortByMaxima(seriesList)                                                  |
| sortByMinima(seriesList)                                                  |
| sortByName(seriesList)                                                    |
| sortByTotal(seriesList)                                                   |
| squareRoot(seriesList)                                                    |
| stacked(seriesLists, stackName='__DEFAULT__')                             |
| stddevSeries(*seriesLists)                                                |
| stdev(seriesList, points, windowTolerance=0.1)                            |
| substr(seriesList, start=0, stop=0)                                       |
| sumSeries(*seriesLists), Short form: sum()                                |
| sumSeriesWithWildcards(seriesList, *position)                             |
| summarize(seriesList, intervalString, func='sum', alignToFrom=False)      |
| threshold(value, label=None, color=None)                                  |
| timeFunction(name, step=60), Short Alias: time()                          |
| timeLagSeries(consumeMaxOffsetSeries, produceMaxOffsetSeries)             |
| timeLagSeriesLists(consumeMaxOffsetSeriesLists, produceMaxOffsetSeriesLists) |
| timeShift(seriesList, timeShift, resetEnd=True)                           |
| timeStack(seriesList, timeShiftUnit, timeShiftStart, timeShiftEnd)        |
| [tukeyAbove](https://en.wikipedia.org/wiki/Tukey%27s_range_test)(seriesList, basis, n, interval=0) |
| [tukeyBelow](https://en.wikipedia.org/wiki/Tukey%27s_range_test)(seriesList, basis, n, interval=0) |
| transformNull(seriesList, default=0)                                      |
| weightedAverage(seriesListAvg, seriesListWeight, *nodes)                  |
