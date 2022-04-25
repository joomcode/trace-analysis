package com.joom.trace_analysis.spark

import com.joom.trace_analysis.Domain.Trace
import com.joom.trace_analysis.util.TimeUtils
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import java.time.Instant

object SparkUtils {
  val tagType: ArrayType = ArrayType(
    StructType(Seq(
      StructField("key", StringType),
      StructField("vStr", StringType),
    ))
  )

  val spanSchema: StructType = StructType(
    Seq(
      StructField("traceId", StringType),
      StructField("spanId", StringType),
      StructField("operationName", StringType),
      StructField(
        "references", ArrayType(
          StructType(Seq(
            StructField("traceId", StringType),
            StructField("spanId", StringType),
          ))
        )
      ),
      StructField("flags", IntegerType),
      StructField("startTime", StringType),
      StructField("duration", StringType),
      StructField(
        "tags", ArrayType(tagType),
      ),
      StructField("process", StructType(Seq(
        StructField("serviceName", StringType),
        StructField("tags", ArrayType(tagType))
      )))
    )
  )


  def getTraceDataset(df: DataFrame, sampleFraction: Double = 1)(implicit spark: SparkSession): Dataset[Trace] = {
    import spark.implicits._

    df
      .groupByKey(_.getAs[String]("traceId"))
      .mapGroups((traceID, spanRows) => {
        val spans = spanRows.map(rowToStorageSpan)
        Storage.Trace(traceID, spans.toSeq)
      })
      .sample(sampleFraction)
      .map(TraceConverter.convertTrace)
  }

  private def rowToStorageSpan(r: Row): Storage.Span = {
    val referencesRows = r
      .getAs[Seq[Row]]("references")

    val references = if (referencesRows == null) {
      Seq[Storage.Reference]()
    } else {
      referencesRows
        .map(r => Storage.Reference(r.getAs("traceId"), r.getAs("spanId")))
    }

    def tagsFromRow(r: Row): Seq[Storage.Tag] = {
      if (r == null) {
        Seq()
      } else {
        val rows = r.getAs[Seq[Row]]("tags")
        if (rows == null) {
          Seq()
        } else {
          rows
            .map(r => Storage.Tag(r.getAs("key"), r.getAs("vStr")))
        }
      }
    }

    val processRow = r.getAs[Row]("process")

    val startTime = Instant.parse(r.getAs("startTime"))

    val durationStr = r.getAs[String]("duration")
    val durationMicros = if (durationStr == null) {
      0
    } else {
      (durationStr.substring(0, durationStr.length - 1).toFloat * 1e6).round
    }

    val endTime = TimeUtils.instantWithOffsetMicros(startTime, durationMicros)

    val process = if (processRow == null) {
      null
    } else {
      Storage.Process(processRow.getAs("serviceName"), tagsFromRow(processRow))
    }

    Storage.Span(
      r.getAs("traceId"),
      r.getAs("spanId"),
      r.getAs("operationName"),
      references,
      r.getAs("flags"),
      startTime,
      endTime,
      tagsFromRow(r),
      process,
    )
  }
}
