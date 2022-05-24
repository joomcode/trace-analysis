package com.joom.trace.analysis

import com.joom.trace.analysis.Domain.{ExecutionGroup, Span, Trace}
import com.joom.trace.analysis.analysis.OptimizationAnalysis
import com.joom.trace.analysis.analysis.OptimizationAnalysis.{FractionOptimization, Optimization, Percentile}
import com.joom.trace.analysis.spark.{Storage, getTraceDataset, spanSchema}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}
import org.junit.Assert.assertEquals
import org.junit.Test

import java.time.Instant

class OptimizationAnalysisTest {
  val spark: SparkSession = createSparkSession()

  case class OptimizationData(name: String, percentile: String, duration: Long)

  @Test
  def testSeqOptimization(): Unit = {
    val traceStart = Instant.ofEpochMilli(0)

    def makeTrace(traceID: String, operation: String) = {
      val rootSpan = Span(
        traceID,
        "span",
        operation,
        traceStart,
        traceStart.plusMillis(100),
        Seq(),
      )

      Trace(
        traceID,
        rootSpan,
        Map(
          rootSpan.spanID -> rootSpan
        )
      )
    }

    val optimizationData = getOptimizationData(
      Seq(
        makeTrace("id1", "operation1"),
        makeTrace("id2", "operation2"),
      ),
      Seq(FractionOptimization("operation1", 0.5)),
      Seq(Percentile("p10", 0.1), Percentile("p90", 0.9))
    )

    assertEquals(100, optimizationData(("none", "p10")))
    assertEquals(100, optimizationData(("none", "p90")))

    assertEquals(50, optimizationData(("operation1", "p10")))
    assertEquals(100, optimizationData(("operation1", "p90")))
  }

  @Test
  def testParallelOptimization(): Unit = {
    val traceStart = Instant.ofEpochMilli(0)

    val traceID = "trace"

    val shortChildSpan = Span(
      traceID,
      "shortChild",
      "short",
      traceStart,
      traceStart.plusMillis(50),
      executionGroups = Seq()
    )

    val longChildSpan = Span(
      traceID,
      "longChild",
      "long",
      traceStart,
      traceStart.plusMillis(100),
      executionGroups = Seq()
    )

    val rootSpan = Span(
      traceID,
      "span",
      "parent",
      traceStart,
      traceStart.plusMillis(100),
      Seq(
        ExecutionGroup(Seq(shortChildSpan.spanID, longChildSpan.spanID))
      ),
    )

    val trace = Trace(
      traceID,
      rootSpan,
      Map(
        rootSpan.spanID -> rootSpan,
        shortChildSpan.spanID -> shortChildSpan,
        longChildSpan.spanID -> longChildSpan,
      )
    )

    val optimizationData = getOptimizationData(
      Seq(trace),
      Seq(FractionOptimization("short", 0.5), FractionOptimization("long", 0.5)),
      Seq(Percentile("p90", 0.9))
    )

    assertEquals(100, optimizationData(("none", "p90")))
    assertEquals(100, optimizationData(("short", "p90")))
    assertEquals(50, optimizationData(("long", "p90")))
  }

  @Test
  def testRealWorldSpanOptimizationAnalysis(): Unit = {
    val spansDF = loadTestData()(spark)

    val traces = getTraceDataset(spansDF)(spark)
      .filter(_.root.operationName == "HTTP GET /dispatch")

    val databaseOperation = "SQL SELECT"

    val optimizationData = getOptimizationData(
      traces,
      Seq(
        FractionOptimization(databaseOperation, 1)
      ),
      Seq(Percentile("p10", 0.1), Percentile("p90", 0.9))
    )

    def getOptimizationPercent(percentile: String) = {
      val baselineDuration = optimizationData(("none", percentile)).toFloat
      val optimizedDuration = optimizationData((databaseOperation, percentile)).toFloat

      (baselineDuration - optimizedDuration) / baselineDuration * 100
    }

    assertEquals(41, getOptimizationPercent("p10"), 1)
    assertEquals(42, getOptimizationPercent("p90"), 1)
  }

  private def getOptimizationData(traces: Seq[Trace], optimizations: Seq[Optimization], percentiles: Seq[Percentile]): Map[(String, String), Long] = {
    import spark.implicits._

    getOptimizationData(
      spark.sparkContext.parallelize(traces).toDS(),
      optimizations,
      percentiles
    )
  }

  private def getOptimizationData(traces: Dataset[Trace], optimizations: Seq[Optimization], percentiles: Seq[Percentile]): Map[(String, String), Long] = {
    OptimizationAnalysis.calculateOptimizedTracesDurations(
      traces,
      optimizations,
      percentiles
    )(spark)
      .collect()
      .map(r => OptimizationData(
        r.getAs("optimization_name"),
        r.getAs("percentile"),
        r.getAs[Long]("duration") / 1000
      ))
      .map(data => ((data.name, data.percentile), data.duration))
      .toMap
  }

  private def createSparkSession() = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    SparkSession.builder()
      .master("local[1]")
      .appName("Test")
      .getOrCreate()
  }

  private def loadTestData()(implicit spark: SparkSession): DataFrame = {
    spark
      .read
      .schema(spanSchema)
      .json("src/test/resources/test_data.json")
  }
}
