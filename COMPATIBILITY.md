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
- integralByInterval
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
- weightedAverage
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
- tukeyAbove
- tukeyBelow
