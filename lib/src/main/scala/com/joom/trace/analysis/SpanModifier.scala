package com.joom.trace.analysis

import Domain.{Continue, ExecutionGroup}
import com.joom.trace.analysis.util.TimeUtils

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

  /**
   * @param sourceSpan - root span modifier applied to
   * @param modifier - interface that changes duration of matching spans
   * @param spansByID - all sub spans of sourceSpan mapped by id
   * @return sourceSpan with modified duration and all (possibly modified) sub spans mapped by id
   */
  def createModifiedSpan(sourceSpan: Domain.Span, modifier: SpanModifier, spansByID: Map[String, Domain.Span]): (Domain.Span, Map[String, Domain.Span]) = {
    val newSpansByID = mutable.Map() ++ spansByID
    val visited = mutable.Set[String]()

    /**
     * @param span - span to process on current stage
     * @param timeUpdateMicros - time offset [microseconds] collected from previously processed spans
     * @return modified span
     */
    def doCreateModifiedSpan(span: Domain.Span, timeUpdateMicros: Long): Domain.Span = {
      if (!visited(span.spanID)) {
        visited.add(span.spanID)
      } else {
        return newSpansByID(span.spanID) // we have already visited this span; no need to process it again
      }

      if (modifier.matchSpan(span)) { // we have to transform span with modifier
        // total time update consists of previously collected update and current span duration modification
        val totalUpdateMicros = modifier.getEndTimeUpdateMicros(span) + timeUpdateMicros

        // modifying non-leaf span transforms it to leaf one
        // as its duration does not depend on its children anymore
        val resultSpan = span.copy(
          startTime = TimeUtils.instantWithOffsetMicros(span.startTime, timeUpdateMicros),
          endTime = TimeUtils.instantWithOffsetMicros(span.endTime, totalUpdateMicros),
          executionGroups = Seq()
        )
        newSpansByID(resultSpan.spanID) = resultSpan

        // remove all the children spans from updated index
        span.traverse(spansByID, span => {
          newSpansByID.remove(span.spanID)

          Continue
        })

        return resultSpan
      }

      // this is a leaf span; we just move its execution boundaries by timeUpdateMicros
      if (span.executionGroups.isEmpty) {
        val resultSpan = span.copy(
          startTime = TimeUtils.instantWithOffsetMicros(span.startTime, timeUpdateMicros),
          endTime = TimeUtils.instantWithOffsetMicros(span.endTime, timeUpdateMicros),
        )
        newSpansByID(resultSpan.spanID) = resultSpan

        return resultSpan
      }

      // now we have to recursively process this span children and collect time offset
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

      // this span time offset consists of previously collected offset and right boundary offset due to children optimization
      val resultSpan = span.copy(
        startTime = TimeUtils.instantWithOffsetMicros(span.startTime, timeUpdateMicros),
        endTime = TimeUtils.instantWithOffsetMicros(span.endTime, localExecutionGroupOffset),
        executionGroups = updatedExecutionGroups.toSeq
      )
      newSpansByID(resultSpan.spanID) = resultSpan

      resultSpan
    }

    (doCreateModifiedSpan(sourceSpan, 0), newSpansByID.toMap)
  }
}
