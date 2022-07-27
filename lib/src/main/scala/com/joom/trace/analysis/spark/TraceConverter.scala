package com.joom.trace.analysis.spark

import com.joom.trace.analysis.Domain
import com.joom.trace.analysis.util.TimeUtils

import java.time.Instant
import scala.collection.mutable

object TraceConverter {
  private case class SpanTimes(startTime: Instant, endTime: Instant)

  def convertTrace(storageTrace: Storage.Trace): Domain.Trace = {
    val spansByParent = getSpansByParent(storageTrace.spans)
    val spansByID = storageTrace.spans.map(span => (span.spanId, span)).toMap
    val rootID = getRootID(spansByID)

    // sometimes jaeger reports parent spans which are shorter than sum of all their children. Fixing it here
    val spanTimes = getSpanTimes(storageTrace.spans, spansByParent)
    val executionGroups = getExecutionGroups(spansByParent, spanTimes)

    val domainSpansByID = spansByID.map(pair => {
      val spanID = pair._1
      val span = pair._2

      val domainSpan = spanFromStorageSpan(span, executionGroups.getOrElse(spanID, Seq()), spanTimes(spanID))

      (spanID, domainSpan)
    })

    Domain.Trace(storageTrace.id, domainSpansByID(rootID), domainSpansByID)
  }

  private def getSpanTimes(spans: Seq[Storage.Span], spansByParent: Map[String, Seq[Storage.Span]]): Map[String, SpanTimes] = {
    val result = mutable.Map[String, SpanTimes]()

    val spanTimesByID = spans.map(s => {
      val startTime = Instant.parse(s.startTime)

      val durationMicros = if (s.duration == null) {
        0
      } else {
        (s.duration.substring(0, s.duration.length - 1).toFloat * 1e6).round
      }

      val endTime = TimeUtils.instantWithOffsetMicros(startTime, durationMicros)

      (s.spanId, SpanTimes(startTime, endTime))
    }).toMap

    def doFillSpanTimes(currSpan: Storage.Span, children: Seq[Storage.Span]): Unit = {
      if (result.contains(currSpan.spanId)) {
        return
      }

      children.foreach(span => doFillSpanTimes(span, spansByParent.getOrElse(span.spanId, Seq())))

      val currSpanTime = spanTimesByID(currSpan.spanId)

      result(currSpan.spanId) = if (children.nonEmpty) {
        val childrenSpanTimes = children.map(s => spanTimesByID(s.spanId))

        val startTime = (childrenSpanTimes.map(_.startTime) :+ currSpanTime.startTime).min
        val endTime = (childrenSpanTimes.map(_.endTime) :+ currSpanTime.endTime).max

        SpanTimes(startTime, endTime)
      } else {
        SpanTimes(currSpanTime.startTime, currSpanTime.endTime)
      }
    }

    spans.foreach(span => doFillSpanTimes(span, spansByParent.getOrElse(span.spanId, Seq())))

    result.toMap
  }

  private def getRootID(spansByID: Map[String, Storage.Span]): String = {
    var currSpan = spansByID.take(1).head._2

    def getParentSpans(currSpan: Storage.Span) = currSpan.references match {
      case Some(refs) => refs .flatMap(ref => spansByID.get(ref.spanId))
      case None => Seq()
    }

    var parentSpans = getParentSpans(currSpan)

    while (parentSpans.nonEmpty) {
      parentSpans = getParentSpans(currSpan)
      if (parentSpans.nonEmpty) {
        currSpan = parentSpans.head
      }
    }

    currSpan.spanId
  }

  private def getExecutionGroups(spansByParent: Map[String, Seq[Storage.Span]], spanTimes: Map[String, SpanTimes]): Map[String, Seq[Domain.ExecutionGroup]] = {
    val result = mutable.Map[String, mutable.Buffer[mutable.Buffer[String]]]()
    spansByParent.foreach(pair => {
      val parentID = pair._1
      val children = pair._2

      var currGroup = mutable.Buffer[Storage.Span]()
      val spanExecutionGroups = mutable.Buffer[mutable.Buffer[String]]()

      children.foreach(child => {
        currGroup match {
          case mutable.Buffer(h, _*) =>
            val hTimes = spanTimes(h.spanId)
            val childTimes = spanTimes(child.spanId)

            if (childTimes.startTime.compareTo(hTimes.endTime) < 0) {
              currGroup.append(child)
            } else {
              spanExecutionGroups.append(currGroup.map(_.spanId))
              currGroup = mutable.Buffer(child)
            }
          case mutable.Buffer() => currGroup.append(child)
        }
      })

      if (currGroup.nonEmpty) {
        spanExecutionGroups.append(currGroup.map(_.spanId))
      }

      result(parentID) = spanExecutionGroups
    })

    result
      .map(pair => (pair._1, pair._2.map(ids => Domain.ExecutionGroup(ids.toSeq)).toSeq))
      .toMap
  }

  private def getSpansByParent(spans: Seq[Storage.Span]): Map[String, Seq[Storage.Span]] = {
    val spansByParent = mutable.Map[String, mutable.Buffer[Storage.Span]]()
    spans.foreach(s => {
      s.references match {
        case Some(refs) => refs.foreach(ref => {
          val parentSpanID = ref.spanId

          spansByParent.get(parentSpanID) match {
            case None => spansByParent.update(parentSpanID, mutable.Buffer(s))
            case Some(spans) => spans.append(s)
          }
        })
        case None =>
      }
    })

    // sort all children spans by start time
    spansByParent
      .map(pair => (pair._1, pair._2.sortBy(_.startTime).toSeq))
      .toMap
  }

  private def spanFromStorageSpan(span: Storage.Span, executionGroups: Seq[Domain.ExecutionGroup], spanTimes: SpanTimes): Domain.Span = {

    val tags = span.tags
      .getOrElse(Seq())
      .map(tag => Domain.Tag(tag.key, tag.vStr))

    Domain.Span(
      span.traceId,
      span.spanId,
      span.operationName,
      spanTimes.startTime,
      spanTimes.endTime,
      executionGroups,
      tags,
    )
  }
}
