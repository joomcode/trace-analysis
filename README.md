# trace-analysis
 
## Highlights
`trace-analysis` is a library for performance bottleneck detection and optimization efficiency prediction.

Given dataframe [OpenTracing](https://github.com/opentracing/specification/blob/master/specification.md)-compatible trace-analysis calculates latency distribution among all
encountered spans.

Also, trace-analysis allows to simulate optimization effects on historical traces, so you can estimate optimization potential 
before implementation. 

Optimization simulation is a key feature of this library but as it requires using sophisticated tree processing algorithms 
(described in [Optimization Analysis](#optimization-analysis) section) we have to deal with recursive data structures during Spark job execution.

## Introduction

The most common approach analyze Jaeger traces is to use Jaeger UI.
But this approach has many issues such as
- Traces may become very long, and it's difficult to detect latency dominators without special tooling.
- On higher percentiles (p95, p99) there may be many operations with high latency (database/cache queries, third party service requests, etc.).
  But it is not clear how to estimate total impact of each such operation on the latency percentile.
- Jaeger UI does not let us analyze subtraces. We have to open the whole trace and then look for operations of interest.
- There is no tooling to estimate optimizations effect before implementation.

This library solves all the issues mentioned above by analyzing big corpora of Jaeger traces using Spark capabilities.  

Usage of this library can be separated into two steps.
1. Hot spot analysis. On this stage we investigate which operations take most of the time.
2. Potential optimization analysis. On this stage we simulate potential optimizations and estimate their effect
   on historical traces.

## Hot Spot Analysis

First of all we should read the corpus of traces we want to analyze. For example, it may be all the traces about
`HTTP GET /dispatch` request.
 
For example, we can read test span dump from file `lib/src/test/resources/test_data.json` (spans create with [hotrod](https://github.com/jaegertracing/jaeger/blob/main/examples/hotrod/README.md)) 
into variable `spanDF`.
```scala
import org.apache.spark.sql.SparkSession
import com.joom.trace.analysis.spark.SparkUtils.spanSchema

val spark = SparkSession.builder()
  .master("local[1]")
  .appName("Test")
  .getOrCreate()


val spanDF = spark
  .read
  .schema(spanSchema)
  .json("lib/src/test/resources/test_data.json")
```

Then we have to define queries to particular operations inside this trace corpus. It may be root operation (`HTTP GET /dispatch`)
or we may want to see latency distribution inside heavy subtraces. Let's assume that we want to check 2 operations:
- Root operation `HTTP GET /dispatch`;
- Heavy subtrace `FindDriverIDs`.

So we create `TraceAnalysisQuery`
```scala
val query = HotSpotAnalysis.TraceAnalysisQuery(
   TraceSelector("HTTP GET /dispatch"),
   Seq(
      OperationAnalysisQuery("HTTP GET /dispatch")
      OperationAnalysisQuery("FindDriverIDs")
   )
)
```

The only thing left is to calculate span durations distribution
```scala
val durations = HotSpotAnalysis.getSpanDurations(spanDF, Seq(query))
```

Here `durations` is a dataframe with scheme
```
(
  "operation_name": name of the operation; 
  "duration": total operations duration [microseconds], 
  "count": total operation count
)
```

Further we can, for example, investigate the longest or most frequent operations.

## Optimization Analysis

Now, when we checked latency distribution among different spans we may want to optimize particular heavy operation.
For example `FindDriverIDs`. But it may take weeks and even months to refactor existing code base. So it will be very convenient
if we could estimate optimization impact before optimization implementation.

And comes `OptimizationAnalysis`!

Like before we need to load historical traces. We have to preprocess them with `SparkUtils.getTraceDataset` method and store in variable `traceDS`.
```scala
val traceDS = SparkUtils.getTraceDataset(spanDF)
```

Suppose we want to simulate effect of 50% latency decrease of `FindDriverIDs` operation.
```scala
val optimization = FractionOptimization("FindDriverIDs", 0.5)
```

Particularly we are interested in p50 and p90
```scala
val percentiles = Seq(Percentile("p50", 0.5), Percentile("p90", 0.9))
```

Now we create optimized durations
```scala
val optimizedDurations = OptimizationAnalysis.calculateOptimizedTracesDurations(
  traceDS,
  Seq(optimization),
  percentiles
)
```

Here `optimizedDurations` is a dataframe with schema.
```
(
   "optimization_name": name of the optimization,
   "percentile": percentile of the request latency
   "duration": latency [microseconds] 
)
```

Now we have estimation of our optimization potential!