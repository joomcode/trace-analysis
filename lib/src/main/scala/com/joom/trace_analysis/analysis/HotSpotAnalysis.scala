package com.joom.trace_analysis.analysis

import com.joom.trace_analysis.Domain.{Continue, Stop}
import com.joom.trace_analysis.Domain.Trace.getSubTracesByOperationName
import com.joom.trace_analysis.spark.SparkUtils.getTraceDataset
import org.apache.spark.sql.functions.{count, lit, sum}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable

object HotSpotAnalysis {
  /**
   * Request to get execution time distribution among all the operations
   *
   * @param traceSelector determines which traces match to analysis
   * @param operations particular operations which should be investigated inside selected traces;
   *                   all matching spans are considered to be root spans of corresponding subtraces
   * @param skipOperations spans which should not be taken into account when analyzing traces
   */
  case class TraceAnalysisRequest(
                                   traceSelector: TraceSelector,
                                   operations: Seq[OperationAnalysisRequest],
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
  case class OperationAnalysisRequest(operation: String)


  /**
   * @param df dataframe of Jaeger spans
   * @param requests trace selectors
   * @return dataframes of shape (operation_name: Span.operationName, duration: total duration of spans with this operation name [ms],
   *         count: amount of spans with this operation name) grouped by OperationAnalysisRequest and TraceAnalysisRequest
   */
  def getSpanDurations(df: DataFrame, requests: Seq[TraceAnalysisRequest])(implicit spark: SparkSession): Map[TraceAnalysisRequest, Map[OperationAnalysisRequest, DataFrame]] = {
    import spark.implicits._

    val traceDataset = getTraceDataset(df).cache()

    requests
      .map(traceRequest => {
        var rootDataset = traceDataset
          .flatMap(getSubTracesByOperationName(_, traceRequest.traceSelector.operation))

        traceRequest.traceSelector.minTraceDurationMilli match {
          case Some(durationMilli) => rootDataset = rootDataset.filter(_.durationMicros > 1000 * durationMilli)
          case None =>
        }

        traceRequest.traceSelector.maxTraceDurationMilli match {
          case Some(durationMilli) => rootDataset = rootDataset.filter(_.durationMicros < 1000 * durationMilli)
          case None =>
        }

        rootDataset = rootDataset.cache()

        val operationDFs = traceRequest.operations.map(operationRequest => {
          val operationDataset = rootDataset
            .flatMap(getSubTracesByOperationName(_, operationRequest.operation))

          val durationDF = operationDataset
            .flatMap(trace => {
              val buffer = mutable.Buffer[(String, Long)]()

              trace.traverse(span => {
                if (traceRequest.skipOperations.contains(span.operationName)) {
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

          (operationRequest, durationDF)
        }).toMap

        (traceRequest, operationDFs)
      }).toMap
  }
}
