package com.joom.trace.analysis.analysis

import com.joom.trace.analysis.Domain.Trace.getSubTracesByOperationName
import com.joom.trace.analysis.Domain.{Continue, Span, Stop}
import com.joom.trace.analysis.spark.getTraceDataset
import com.joom.trace.analysis.util.TimeUtils
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.Instant
import scala.collection.mutable

object HotSpotAnalysis {
  /**
   * Query to get execution time distribution among all the operations
   *
   * @param traceSelector determines which traces match to analysis
   * @param operations particular operations which should be investigated inside selected traces;
   *                   all matching spans are considered to be root spans of corresponding subtraces
   * @param skipOperations spans which should not be taken into account when analyzing traces
   */
  case class TraceAnalysisQuery(
                                 traceSelector: TraceSelector,
                                 operations: Seq[OperationAnalysisQuery],
                                 skipOperations: Seq[String] = Seq(),
                                 )

  case class TraceSelector(
                            operation: String,
                            minTraceDurationMilli: Option[Long] = None,
                            maxTraceDurationMilli: Option[Long] = None
                          )

  /**
   * @param operation operationName of the span
   */
  case class OperationAnalysisQuery(operation: String)


  /**
   * @param df dataframe of Jaeger spans
   * @param queries trace selectors
   * @return dataframes of shape (operation_name: Span.operationName, duration: total duration of spans with this operation name [ms],
   *         count: amount of spans with this operation name) grouped by OperationAnalysisQuery and TraceAnalysisQuery
   */
  def getSpanDurations(df: DataFrame, queries: Seq[TraceAnalysisQuery])(implicit spark: SparkSession): Map[TraceAnalysisQuery, Map[OperationAnalysisQuery, DataFrame]] = {
    import spark.implicits._

    val traceDataset = getTraceDataset(df).cache()

    queries
      .map(traceQuery => {
        var rootDataset = traceDataset
          .flatMap(getSubTracesByOperationName(_, traceQuery.traceSelector.operation))

        traceQuery.traceSelector.minTraceDurationMilli match {
          case Some(durationMilli) => rootDataset = rootDataset.filter(_.durationMicros > 1000 * durationMilli)
          case None =>
        }

        traceQuery.traceSelector.maxTraceDurationMilli match {
          case Some(durationMilli) => rootDataset = rootDataset.filter(_.durationMicros < 1000 * durationMilli)
          case None =>
        }

        rootDataset.persist()

        val operationDFs = traceQuery.operations.map(operationQuery => {
          val operationDataset = rootDataset
            .flatMap(getSubTracesByOperationName(_, operationQuery.operation))

          val durationDF = operationDataset
            .flatMap(trace => {
              val spansByName = mutable.Map[String, mutable.Buffer[Span]]()

              trace.traverse(span => {
                if (traceQuery.skipOperations.contains(span.operationName)) {
                  Stop
                } else {
                  if (spansByName.contains(span.operationName)) {
                    spansByName(span.operationName).append(span)
                  } else {
                    spansByName.put(span.operationName, mutable.Buffer(span))
                  }
                  Continue
                }
              })

              spansByName
                .map(pair => (pair._1, getSpansUnionDuration(pair._2.toSeq), pair._2.length))
            })
            .toDF("operation_name", "duration", "count")
            .groupBy("operation_name")
            .agg(
              sum($"duration").as("duration"),
              sum($"count").as("count")
            )

          (operationQuery, durationDF)
        }).toMap

        rootDataset.unpersist()

        (traceQuery, operationDFs)
      }).toMap
  }

  private def getSpansUnionDuration(spans: Seq[Span]): Long = {
    if (spans.isEmpty) {
      return 0
    }

    val sortedSpans = spans.sortBy(_.startTime)
    val head = sortedSpans.head

    val periodBuffer = mutable.Buffer[(Instant, Instant)]()
    var currPeriod = (head.startTime, head.endTime)

    sortedSpans
      .tail
      .foreach(span => {
        if (span.startTime.isBefore(currPeriod._2)) {
          val endTime = if (span.endTime.isAfter(currPeriod._2)) {
            span.endTime
          } else {
            currPeriod._2
          }

          currPeriod = (currPeriod._1, endTime)
        } else {
          periodBuffer.append(currPeriod)
          currPeriod = (span.startTime, span.endTime)
        }
      })

    periodBuffer.append(currPeriod)

    periodBuffer
      .map(period => TimeUtils.durationMicros(period._1, period._2))
      .sum
  }
}
