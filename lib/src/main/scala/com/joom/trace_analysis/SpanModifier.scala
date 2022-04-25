package com.joom.trace_analysis

import Domain.ExecutionGroup
import com.joom.trace_analysis.util.TimeUtils

import scala.collection.mutable

/**
 * SpanModifier enables to consistently change latency of spans
 */
trait SpanModifier {
  def matchSpan(span: Domain.Span): Boolean
  def getEndTimeUpdateMicros(span: Domain.Span): Long
}

object SpanModifier {
  def createModifiedTrace(sourceTrace: Domain.Trace, modifier: SpanModifier): Domain.Trace = {
    val (modifiedSpan, spanIndex) = createModifiedSpan(sourceTrace.root, modifier, sourceTrace.spanIndex)

    Domain.Trace(sourceTrace.id, modifiedSpan, spanIndex)
  }

  def createModifiedSpan(sourceSpan: Domain.Span, modifier: SpanModifier, spansByID: Map[String, Domain.Span]): (Domain.Span, Map[String, Domain.Span]) = {
    val newSpansByID = mutable.Map() ++ spansByID
    val visited = mutable.Set[String]()

    def doCreateModifiedSpan(span: Domain.Span, timeUpdateMicros: Long): Domain.Span = {
      if (!visited(span.spanID)) {
        visited.add(span.spanID)
      } else {
        return newSpansByID(span.spanID)
      }

      if (modifier.matchSpan(span)) {
        val totalUpdateMicros = modifier.getEndTimeUpdateMicros(span) + timeUpdateMicros

        // modifying non-leaf span transforms it to leaf one
        // as its duration does not depend on its children anymore
        val resultSpan = span.copy(
          startTime = TimeUtils.instantWithOffsetMicros(span.startTime, timeUpdateMicros),
          endTime = TimeUtils.instantWithOffsetMicros(span.endTime, totalUpdateMicros),
          executionGroups = Seq()
        )
        newSpansByID(resultSpan.spanID) = resultSpan

        return resultSpan
      }

      if (span.executionGroups.isEmpty) {
        val resultSpan = span.copy(
          startTime = TimeUtils.instantWithOffsetMicros(span.startTime, timeUpdateMicros),
          endTime = TimeUtils.instantWithOffsetMicros(span.endTime, timeUpdateMicros),
        )
        newSpansByID(resultSpan.spanID) = resultSpan

        return resultSpan
      }

      var localExecutionGroupOffset = timeUpdateMicros
      val updatedExecutionGroups = mutable.Buffer[ExecutionGroup]()
      for (eg <- span.executionGroups) {
        val spans = eg.spanIDs.map(id => doCreateModifiedSpan(spansByID(id), localExecutionGroupOffset))
        val updatedEG = ExecutionGroup(spans.map(_.spanID))
        updatedExecutionGroups.append(updatedEG)

        val localDiffMicros = TimeUtils.durationMicros(
          eg.spanIDs.map(id => spansByID(id).endTime).max,
          spans.map(_.endTime).max
        )

        localExecutionGroupOffset = localDiffMicros
      }

      val resultSpan = span.copy(
        startTime = TimeUtils.instantWithOffsetMicros(span.startTime, timeUpdateMicros),
        endTime = TimeUtils.instantWithOffsetMicros(span.endTime, localExecutionGroupOffset),
        executionGroups = updatedExecutionGroups
      )
      newSpansByID(resultSpan.spanID) = resultSpan

      resultSpan
    }

    (doCreateModifiedSpan(sourceSpan, 0), newSpansByID.toMap)
  }
}
