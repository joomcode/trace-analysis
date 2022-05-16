package com.joom.trace.analysis.analysis

import com.joom.trace.analysis.Domain.Trace.getSubTracesByOperationName
import com.joom.trace.analysis.Domain.{Continue, Stop}
import com.joom.trace.analysis.spark.SparkUtils.getTraceDataset
import org.apache.spark.sql.functions.{count, lit, sum}
import org.apache.spark.sql.{DataFrame, SparkSession}

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
              val buffer = mutable.Buffer[(String, Long)]()

              trace.traverse(span => {
                if (traceQuery.skipOperations.contains(span.operationName)) {
                  Stop
                } else {
                  buffer.append((span.operationName, span.durationMicros))
                  Continue
                }
              })

              buffer
            })
            .toDF("operation_name", "duration")
            .groupBy("operation_name")
            .agg(
              sum($"duration").as("duration"),
              count(lit(1)).as("count")
            )

          (operationQuery, durationDF)
        }).toMap

        rootDataset.unpersist()

        (traceQuery, operationDFs)
      }).toMap
  }
}
