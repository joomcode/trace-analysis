package com.joom.trace.analysis.analysis

import com.joom.trace.analysis.Domain.Span
import com.joom.trace.analysis.Domain.Trace
import com.joom.trace.analysis.{Domain, SpanModifier}
import org.apache.spark.sql.functions.{explode, expr}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object OptimizationAnalysis {
  /**
   * Request to simulate effect of potential optimizations on historical traces
   *
   * @param traceRoot span operationName which is used to select traces
   * @param operation operation which should be considered a trace root
   * @param percentiles to calculate latency on
   * @param optimizations potential optimizations applicable to traces
   */
  case class Request(
                      traceRoot: String,
                      operation: String,
                      percentiles: Seq[Percentile],
                      optimizations: Seq[Optimization]
                    )


  /**
   * Named span modifier
   */
  trait Optimization extends SpanModifier {
    def name(): String
  }

  /**
   * Potential optimization which decreases duration of spans with given name by optimization fraction
   *
   * @param operationName - spans to be optimized
   * @param optimizationFraction - potential optimization [0, 1]; 0 corresponds to "no optimization" and 1 to full span elimination
   */
  case class FractionOptimization(operationName: String, optimizationFraction: Double) extends Optimization {
    override def matchSpan(span: Span): Boolean = {
      span.operationName == operationName
    }

    override def getEndTimeUpdateMicros(span: Domain.Span): Long = {
      -(span.durationMicros.toDouble * optimizationFraction).toLong
    }

    override def name(): String = operationName
  }

  case class Percentile(name: String, value: Double)


  /**
   * Calculates distribution of latency among given percentiles using given optimizations
   *
   * @param traceDataset historical traces
   * @param optimizations to apply
   * @param percentiles to calculate
   * @return latency distribution
   */
  def calculateOptimizedTracesDurations(traceDataset: Dataset[Trace], optimizations: Seq[Optimization], percentiles: Seq[Percentile])(implicit spark: SparkSession): DataFrame = {
    val durationsDF = calculateOptimizedDurations(traceDataset, optimizations)

    calculateDurationPercentiles(
      durationsDF,
      percentiles
    )
  }

  private def calculateOptimizedDurations(traceDataset: Dataset[Trace], optimizations: Seq[Optimization])(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    traceDataset
      .flatMap(trace => {
        val modifiedTracesDuration = optimizations.map(opt => {
          val modifiedTrace = SpanModifier.createModifiedTrace(trace, opt)
          if (modifiedTrace.durationMicros > trace.durationMicros) {
            throw new RuntimeException(s"invalid optimization of ${trace.id}, ${opt.name()}")
          }

          (opt.name(), modifiedTrace.durationMicros)
        })

        ("none", trace.durationMicros) +: modifiedTracesDuration
      })
      .toDF("optimization_name", "duration")
  }

  private def calculateDurationPercentiles(df: DataFrame, percentiles: Seq[Percentile])(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val aggArgs = percentiles
      .map(p => s"'${p.name}', approx_percentile(duration, ${p.value})")

    val percentilesMap = s"map(${aggArgs.mkString(",")})"

    df
      .groupBy("optimization_name")
      .agg(
        expr(percentilesMap).as("duration_percentiles")
      )
      .select(
        $"optimization_name",
        explode($"duration_percentiles")
      )
      .withColumnRenamed("key", "percentile")
      .withColumnRenamed("value", "duration")
  }
}
