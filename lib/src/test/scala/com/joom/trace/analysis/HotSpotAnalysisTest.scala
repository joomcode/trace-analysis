package com.joom.trace.analysis

import com.joom.trace.analysis.analysis.HotSpotAnalysis
import com.joom.trace.analysis.analysis.HotSpotAnalysis.TraceSelector
import com.joom.trace.analysis.spark.SparkUtils.spanSchema
import com.joom.trace.analysis.spark.Storage
import com.joom.trace.analysis.util.TimeUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.junit.Assert.assertEquals
import org.junit.Test

import java.time.Instant

class HotSpotAnalysisTest {
  val spark: SparkSession = createSparkSession()

  case class HotSpotData(operation: String, duration: Long, count: Long)

  @Test
  def testSeqChildrenSpans(): Unit = {
    val traceStart = Instant.ofEpochMilli(0)

    val traceID = "traceId"

    val rootSpan = Storage.Span(
      traceID = traceID,
      spanID = "rootSpan",
      operationName = "rootOperation",
      startTime = traceStart,
      endTime = traceStart.plusMillis(200)
    )

    val childSpan1 = Storage.Span(
      traceID = traceID,
      spanID = "childSpan1",
      operationName = "childOperation1",
      startTime = traceStart,
      endTime = traceStart.plusMillis(100),
      references = Seq(Storage.Reference(traceID = traceID, spanID = "rootSpan"))
    )

    val childSpan2 = Storage.Span(
      traceID = traceID,
      spanID = "childSpan2",
      operationName = "childOperation2",
      startTime = traceStart.plusMillis(100),
      endTime = traceStart.plusMillis(200),
      references = Seq(Storage.Reference(traceID = traceID, spanID = "rootSpan"))
    )

    val df = createSpanDF(Seq(rootSpan, childSpan1, childSpan2))

    val operationData = getOperationData(df, "rootOperation")

    assertEquals(200, operationData("rootOperation").duration)
    assertEquals(1, operationData("rootOperation").count)

    assertEquals(100, operationData("childOperation1").duration)
    assertEquals(1, operationData("childOperation1").count)

    assertEquals(100, operationData("childOperation2").duration)
    assertEquals(1, operationData("childOperation2").count)
  }

  @Test
  def testParallelChildrenSpans(): Unit = {
    val traceStart = Instant.ofEpochMilli(0)

    val traceID = "traceId"

    val rootSpan = Storage.Span(
      traceID = traceID,
      spanID = "rootSpan",
      operationName = "rootOperation",
      startTime = traceStart,
      endTime = traceStart.plusMillis(200)
    )

    val childSpan1 = Storage.Span(
      traceID = traceID,
      spanID = "childSpan1",
      operationName = "childOperation",
      startTime = traceStart,
      endTime = traceStart.plusMillis(100),
      references = Seq(Storage.Reference(traceID = traceID, spanID = "rootSpan"))
    )

    val childSpan2 = Storage.Span(
      traceID = traceID,
      spanID = "childSpan2",
      operationName = "childOperation",
      startTime = traceStart,
      endTime = traceStart.plusMillis(100),
      references = Seq(Storage.Reference(traceID = traceID, spanID = "rootSpan"))
    )

    val df = createSpanDF(Seq(rootSpan, childSpan1, childSpan2))

    val operationData = getOperationData(df, "rootOperation")

    assertEquals(200, operationData("rootOperation").duration)
    assertEquals(1, operationData("rootOperation").count)

    assertEquals(200, operationData("childOperation").duration)
    assertEquals(2, operationData("childOperation").count)
  }

  @Test
  def testRealWorldSpanHotSpotAnalysis(): Unit = {
    val spansDF = loadTestData()(spark)
    val operationData = getOperationData(spansDF, "HTTP GET: /customer")

    val operations = operationData.keys.toList.sorted.seq
    assertEquals(Seq(
      "HTTP GET",
      "HTTP GET /customer",
      "HTTP GET: /customer",
      "SQL SELECT",
    ), operations)
  }

  private def getOperationData(df: DataFrame, rootOperation: String): Map[String, HotSpotData] = {
    val operationQuery = HotSpotAnalysis.OperationAnalysisQuery(rootOperation)
    val queries = Seq(HotSpotAnalysis.TraceAnalysisQuery(
      TraceSelector(
        operation = rootOperation
      ),
      Seq(operationQuery)
    ))

    val durations = HotSpotAnalysis.getSpanDurations(df, queries)(spark)

    durations(queries.head)(operationQuery)
      .collect()
      .map(r => HotSpotData(
        r.getAs[String]("operation_name"),
        r.getAs[Long]("duration") / 1000,
        r.getAs[Long]("count")
      ))
      .map(data => (data.operation, data))
      .toMap
  }

  private def createSpanDF(spans: Seq[Storage.Span]): DataFrame = {
    val rows = spans
      .map(s => Row(
        s.traceID,
        s.spanID,
        s.operationName,
        s.references.map(r => Row(r.traceID, r.spanID)),
        s.flags, s.startTime.toString,
        (TimeUtils.durationMicros(s.startTime, s.endTime).toFloat / 1e6).toString + "s",
        s.tags,
        s.process
      ))

    spark.createDataFrame(spark.sparkContext.parallelize(rows), spanSchema)
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
