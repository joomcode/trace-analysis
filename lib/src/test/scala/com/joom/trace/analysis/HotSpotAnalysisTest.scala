package com.joom.trace.analysis

import com.joom.trace.analysis.analysis.HotSpotAnalysis
import com.joom.trace.analysis.analysis.HotSpotAnalysis.TraceSelector
import com.joom.trace.analysis.spark.{Storage, spanSchema}
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
      traceId = traceID,
      spanId = "rootSpan",
      operationName = "rootOperation",
      startTime = traceStart.toString,
      duration = "0.2s"
    )

    val childSpan1 = Storage.Span(
      traceId = traceID,
      spanId = "childSpan1",
      operationName = "childOperation1",
      startTime = traceStart.toString,
      duration = "0.1s",
      references = Some(Seq(Storage.Reference(traceId = traceID, spanId = "rootSpan")))
    )

    val childSpan2 = Storage.Span(
      traceId = traceID,
      spanId = "childSpan2",
      operationName = "childOperation2",
      startTime = traceStart.plusMillis(100).toString,
      duration = "0.1s",
      references = Some(Seq(Storage.Reference(traceId = traceID, spanId = "rootSpan")))
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
      traceId = traceID,
      spanId = "rootSpan",
      operationName = "rootOperation",
      startTime = traceStart.toString,
      duration = "0.2s"
    )

    val childSpan1 = Storage.Span(
      traceId = traceID,
      spanId = "childSpan1",
      operationName = "childOperation",
      startTime = traceStart.toString,
      duration = "0.1s",
      references = Some(Seq(Storage.Reference(traceId = traceID, spanId = "rootSpan")))
    )

    val childSpan2 = Storage.Span(
      traceId = traceID,
      spanId = "childSpan2",
      operationName = "childOperation",
      startTime = traceStart.toString,
      duration = "0.1s",
      references = Some(Seq(Storage.Reference(traceId = traceID, spanId = "rootSpan")))
    )

    val df = createSpanDF(Seq(rootSpan, childSpan1, childSpan2))

    val operationData = getOperationData(df, "rootOperation")

    assertEquals(200, operationData("rootOperation").duration)
    assertEquals(1, operationData("rootOperation").count)

    assertEquals(100, operationData("childOperation").duration)
    assertEquals(2, operationData("childOperation").count)
  }

  @Test
  def testOperationUnion(): Unit = {
    val traceStart = Instant.ofEpochMilli(0)

    val traceID = "traceId"

    val rootSpan = Storage.Span(
      traceId = traceID,
      spanId = "rootSpan",
      operationName = "rootOperation",
      startTime = traceStart.toString,
      duration = "0.3s"
    )

    val childSpan1 = Storage.Span(
      traceId = traceID,
      spanId = "childSpan1",
      operationName = "childOperation",
      startTime = traceStart.toString,
      duration = "0.2s",
      references = Some(Seq(Storage.Reference(traceId = traceID, spanId = "rootSpan")))
    )

    val childSpan2 = Storage.Span(
      traceId = traceID,
      spanId = "childSpan2",
      operationName = "childOperation",
      startTime = traceStart.plusMillis(100).toString,
      duration = "0.2s",
      references = Some(Seq(Storage.Reference(traceId = traceID, spanId = "rootSpan")))
    )

    val childSpan3 = Storage.Span(
      traceId = traceID,
      spanId = "childSpan3",
      operationName = "childOperation",
      startTime = traceStart.plusMillis(500).toString,
      duration = "0.5s",
      references = Some(Seq(Storage.Reference(traceId = traceID, spanId = "rootSpan")))
    )

    val childSpan4 = Storage.Span(
      traceId = traceID,
      spanId = "childSpan4",
      operationName = "childOperation",
      startTime = traceStart.plusMillis(600).toString,
      duration = "0.1s",
      references = Some(Seq(Storage.Reference(traceId = traceID, spanId = "rootSpan")))
    )

    val df = createSpanDF(Seq(rootSpan, childSpan1, childSpan2, childSpan3, childSpan4))

    val operationData = getOperationData(df, "rootOperation")

    assertEquals(800, operationData("childOperation").duration)
    assertEquals(4, operationData("childOperation").count)
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
      .map(s => {
        val refs = s.references match {
          case Some(refs) => refs.map(r => Row(r.traceId, r.spanId))
          case None => Seq()
        }

        val flags = s.flags match {
          case Some(f) => f
          case None => 0
        }

        val tags = s.tags match {
          case Some(tags) => tags
          case None => Seq()
        }

        val process = s.process match {
          case Some(process) => process
          case None => null
        }

        Row(
          s.traceId,
          s.spanId,
          s.operationName,
          refs,
          flags, s.startTime,
          s.duration,
          tags,
          process
        )
      })

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
