package com.joom.trace.analysis

import com.joom.trace.analysis.util.TimeUtils

import java.time.Instant
import scala.collection.mutable

object Domain {
  sealed trait TraverseDecision
  case object Continue extends TraverseDecision
  case object Stop extends TraverseDecision

  /**
   * Trace is a tree-like collection of spans containing all information about query processing
   *
   * @param id unique string
   * @param root root span
   * @param spanIndex map spanID -> Span
   */
  case class Trace(id: String, root: Span, spanIndex: Map[String, Span]) {
    def durationMicros: Long = root.durationMicros

    def traverse(callback: Span => TraverseDecision): Unit = traverseFromSpan(root.spanID, callback, inclusive = true)

    def traverseFromSpan(spanID: String, callback: Span => TraverseDecision, inclusive: Boolean = false): Unit = spanIndex(spanID).traverse(spanIndex, callback, inclusive)
  }

  object Trace {
    def getSubTrace(trace: Trace, newRoot: Span): Trace = {
      val traceSubMap = {
        val result = mutable.Map[String, Span]()
        trace.traverseFromSpan(newRoot.spanID, span => {
          result(span.spanID) = span
          Continue
        }, inclusive = true)

        result.toMap
      }

      Trace(trace.id, newRoot, traceSubMap)
    }

    def getSubTracesByOperationName(trace: Trace, operationName: String): Seq[Trace] = trace.spanIndex
      .values
      .filter(_.operationName == operationName)
      .map((trace, _))
      .map(pair => Trace.getSubTrace(pair._1, pair._2))
      .toSeq
  }

  /**
   * @param spanIDs ids of spans executed in parallel
   */
  case class ExecutionGroup(spanIDs: Seq[String])

  /**
   * @param key is a name of a tag
   * @param vStr is filled if tag has string value (currently the only one supported)
   */
  case class Tag(key: String, vStr: String)

  /**
   * Unit of trace execution
   *
   * @param traceID common among all spans of one trace
   * @param spanID unique among all spans in one trace
   * @param operationName arbitrary string
   * @param startTime span start (microsecond precision)
   * @param endTime span end (microsecond precision)
   * @param executionGroups groups of children spans. All spans inside one group are executed in parallel
   */
  case class Span(
                       traceID: String,
                       spanID: String,
                       operationName: String,
                       startTime: Instant,
                       endTime: Instant,
                       executionGroups: Seq[ExecutionGroup],
                       tags: Seq[Tag]
                     ) {
    def durationMicros: Long = TimeUtils.durationMicros(startTime, endTime)

    def traverse(spanIndex: Map[String, Span], callback: Span => TraverseDecision, inclusive: Boolean = false): Unit = {
      def doTraverse(span: Span): Unit = {
        val decision = callback(span)
        if (decision == Stop) {
          return
        }

        span.executionGroups.foreach(
          eg => eg.spanIDs
            .map(spanIndex)
            .foreach(doTraverse),
        )
      }

      if (inclusive) {
        doTraverse(this)
      } else {
        executionGroups
          .foreach(
            eg => eg
              .spanIDs
              .map(spanIndex)
              .foreach(doTraverse)
          )
      }
    }
  }
}
