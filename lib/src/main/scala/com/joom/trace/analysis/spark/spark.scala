package com.joom.trace.analysis

import com.joom.trace.analysis.Domain.Trace
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}

package object spark {
  val spanSchema: StructType = Encoders.product[Storage.Span].schema

  /**
   * @param df Dataframe with schema spanSchema
   * @param sampleFraction is a fraction of traces which should be taken into account [0; 1]
   * @return dataset of domain traces
   */
  def getTraceDataset(df: DataFrame, sampleFraction: Double = 1)(implicit spark: SparkSession): Dataset[Trace] = {
    import spark.implicits._

    df
      .as[Storage.Span]
      .groupByKey(_.traceId)
      .mapGroups((traceID, spans) => {
        Storage.Trace(traceID, spans.toSeq)
      })
      .sample(sampleFraction)
      .map(TraceConverter.convertTrace)
  }
}
