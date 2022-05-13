package com.joom.trace.analysis.spark

import com.joom.trace.analysis.Domain

import java.time.Instant
import scala.collection.mutable

object TraceConverter {
  def convertTrace(storageTrace: Storage.Trace): Domain.Trace = {
    val spansByParent = getSpansByParent(storageTrace.spans)
    val spansByID = storageTrace.spans.map(span => (span.spanID, span)).toMap
    val executionGroups = getExecutionGroups(spansByParent)
    val rootID = getRootID(spansByID)

    // sometimes jaeger reports parent spans which are shorter than sum of all their children. Fixing it here
    val spanTimes = getSpanTimes(storageTrace.spans, spansByParent)

    val domainSpansByID = spansByID.map(pair => {
      val spanID = pair._1
      val span = pair._2

      (spanID, spanFromStorageSpan(span, executionGroups.getOrElse(spanID, Seq()), spanTimes))
    })

    Domain.Trace(storageTrace.id, domainSpansByID(rootID), domainSpansByID)
  }

  private def getSpanTimes(spans: Seq[Storage.Span], spansByParent: Map[String, Seq[Storage.Span]]): Map[String, (Instant, Instant)] = {
    val result = mutable.Map[String, (Instant, Instant)]()

    def doFillSpanTimes(currSpan: Storage.Span, children: Seq[Storage.Span]): Unit = {
      if (result.contains(currSpan.spanID)) {
        return
      }

      children.foreach(span => doFillSpanTimes(span, spansByParent.getOrElse(span.spanID, Seq())))

      result(currSpan.spanID) = if (children.nonEmpty) {
        val startTime = (children.map(_.startTime) :+ currSpan.startTime).min
        val endTime = (children.map(_.endTime) :+ currSpan.endTime).max

        (startTime, endTime)
      } else {
        (currSpan.startTime, currSpan.endTime)
      }
    }

    spans.foreach(span => doFillSpanTimes(span, spansByParent.getOrElse(span.spanID, Seq())))

    result.toMap
  }

  private def getRootID(spansByID: Map[String, Storage.Span]): String = {
    var currSpan = spansByID.take(1).head._2

    def getParentSpans(currSpan: Storage.Span) = currSpan.references.flatMap(ref => spansByID.get(ref.spanID))

    var parentSpans = getParentSpans(currSpan)

    while (parentSpans.nonEmpty) {
      parentSpans = getParentSpans(currSpan)
      if (parentSpans.nonEmpty) {
        currSpan = parentSpans.head
      }
    }

    currSpan.spanID
  }

  private def getExecutionGroups(spansByParent: Map[String, Seq[Storage.Span]]): Map[String, Seq[Domain.ExecutionGroup]] = {
    val result = mutable.Map[String, mutable.Buffer[mutable.Buffer[String]]]()
    spansByParent.foreach(pair => {
      val parentID = pair._1
      val children = pair._2

      var currGroup = mutable.Buffer[Storage.Span]()
      val spanExecutionGroups = mutable.Buffer[mutable.Buffer[String]]()

      children.foreach(child => {
        currGroup match {
          case mutable.Buffer(h, _*) =>
            if (child.startTime.compareTo(h.endTime) < 0) {
              currGroup.append(child)
            } else {
              spanExecutionGroups.append(currGroup.map(_.spanID))
              currGroup = mutable.Buffer(child)
            }
          case mutable.Buffer() => currGroup.append(child)
        }
      })

      if (currGroup.nonEmpty) {
        spanExecutionGroups.append(currGroup.map(_.spanID))
      }

      result(parentID) = spanExecutionGroups
    })

    result
      .map(pair => (pair._1, pair._2.map(ids => Domain.ExecutionGroup(ids))))
      .toMap
  }

  private def getSpansByParent(spans: Seq[Storage.Span]): Map[String, Seq[Storage.Span]] = {
    val spansByParent = mutable.Map[String, mutable.Buffer[Storage.Span]]()
    spans.foreach(s => {
      s.references.foreach(ref => {
        val parentSpanID = ref.spanID

        spansByParent.get(parentSpanID) match {
          case None => spansByParent.update(parentSpanID, mutable.Buffer(s))
          case Some(spans) => spans.append(s)
        }
      })
    })

    // sort all children spans by start time
    spansByParent
      .map(pair => (pair._1, pair._2.sortBy(_.startTime)))
      .toMap
  }

  private def spanFromStorageSpan(span: Storage.Span, executionGroups: Seq[Domain.ExecutionGroup], spanTimes: Map[String, (Instant, Instant)]): Domain.Span = {
    val (startTime, endTime) = spanTimes(span.spanID)

    Domain.Span(
      span.traceID,
      span.spanID,
      span.operationName,
      startTime,
      endTime,
      executionGroups
    )
  }
}
