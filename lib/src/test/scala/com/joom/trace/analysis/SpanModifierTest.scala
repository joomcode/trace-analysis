package com.joom.trace.analysis

import com.joom.trace.analysis.Domain.ExecutionGroup
import com.joom.trace.analysis.util.TimeUtils.instantWithOffsetMicros
import org.junit.Assert.assertEquals
import org.junit.Test

import java.time.Instant

class SpanModifierTest {
  @Test
  def testSpanModificationNoMatchingSpans(): Unit = {
    val allSpans = Seq(
      Domain.Span(
        traceID = "traceID",
        spanID = "SpanID",
        operationName = "Name",
        startTime = traceStart,
        endTime = instantWithOffsetMicros(traceStart, 100),
        executionGroups = Seq(),
        tags = Seq(),
      )
    )

    val modifiedSpan = createModifiedSpan(allSpans, new FixedOptimizer("Name1", 25))

    assertEquals(100, allSpans.head.durationMicros)
    assertEquals(100, modifiedSpan.durationMicros)
  }

  @Test
  def testSpanModificationTerminalSpan(): Unit = {
    val allSpans = Seq(
      fakeSpan("Name", traceStart, instantWithOffsetMicros(traceStart, 100))
    )

    val modifiedSpan = createModifiedSpan(allSpans, new FixedOptimizer("Name", 25))

    assertEquals(100, allSpans.head.durationMicros)
    assertEquals(75, modifiedSpan.durationMicros)
  }

  @Test
  def testSpanModificationTerminalSpanOnSecondLevel(): Unit = {
    val allSpans = Seq(
      fakeSpan("root", traceStart, instantWithOffsetMicros(traceStart, 100),
        Seq(
          ExecutionGroup(
            Seq("child")
          )
        )
      ),
      fakeSpan("child", traceStart, instantWithOffsetMicros(traceStart, 100))
    )

    val modifiedSpan = createModifiedSpan(allSpans, new FixedOptimizer("child", 25))

    assertEquals(100, allSpans.head.durationMicros)
    assertEquals(75, modifiedSpan.durationMicros)
  }

  @Test
  def testSpanModificationTerminalSpanParallelNoEffect(): Unit = {
    val allSpans = Seq(
      fakeSpan("root", traceStart, instantWithOffsetMicros(traceStart, 100),
        Seq(
          ExecutionGroup(Seq(
            "child1",
            "child2"
          ))
        )
      ),
      fakeSpan("child1", traceStart, instantWithOffsetMicros(traceStart, 100)),
      fakeSpan("child2", traceStart, instantWithOffsetMicros(traceStart, 75))
    )

    val modifiedSpan = createModifiedSpan(allSpans, new FixedOptimizer("child2", 25))

    assertEquals(100, allSpans.head.durationMicros)
    assertEquals(100, modifiedSpan.durationMicros)
  }

  @Test
  def testSpanModificationTerminalSpanParallelWithEffect(): Unit = {
    val allSpans = Seq(
      fakeSpan("root", traceStart, instantWithOffsetMicros(traceStart, 100),
        Seq(
          ExecutionGroup(Seq(
            "child1",
            "child2"
          ))
        )
      ),
      fakeSpan("child1", traceStart, instantWithOffsetMicros(traceStart, 100)),
      fakeSpan("child2", traceStart, instantWithOffsetMicros(traceStart, 75))
    )

    val modifiedSpan = createModifiedSpan(allSpans, new FixedOptimizer("child1", 50))

    assertEquals(100, allSpans.head.durationMicros)
    assertEquals(75, modifiedSpan.durationMicros)
  }

  @Test
  def testSpanModificationTerminalSpanConsecutive(): Unit = {
    val allSpans = Seq(
      fakeSpan("root", traceStart, instantWithOffsetMicros(traceStart, 200),
        Seq(
          ExecutionGroup(Seq(
            "child1"
          )),
          ExecutionGroup(Seq(
            "child2"
          ))
        )
      ),
      fakeSpan("child1", traceStart, instantWithOffsetMicros(traceStart, 100)),
      fakeSpan("child2", instantWithOffsetMicros(traceStart, 100), instantWithOffsetMicros(traceStart, 200))
    )

    val modifiedSpan = createModifiedSpan(allSpans, new FixedOptimizer("child1", -100))

    assertEquals(200, allSpans.head.durationMicros)
    assertEquals(300, modifiedSpan.durationMicros)
  }

  @Test
  def testSpanModificationNonTerminalSpan(): Unit = {
    val allSpans = Seq(
      fakeSpan(
        "root", traceStart, instantWithOffsetMicros(traceStart, 400),
        Seq(
          ExecutionGroup(Seq("child1")),
          ExecutionGroup(Seq("child2"))
        )
      ),
      fakeSpan(
        "child1", traceStart, instantWithOffsetMicros(traceStart, 200),
        Seq(
          ExecutionGroup(Seq("child11")),
          ExecutionGroup(Seq("child12"))
        )
      ),
      fakeSpan(
        "child2", instantWithOffsetMicros(traceStart, 200), instantWithOffsetMicros(traceStart, 400),
        Seq(
          ExecutionGroup(Seq("child21")),
          ExecutionGroup(Seq("child22"))
        )
      ),
      fakeSpan("child11", traceStart, instantWithOffsetMicros(traceStart, 100)),
      fakeSpan("child12", instantWithOffsetMicros(traceStart, 100), instantWithOffsetMicros(traceStart, 200)),
      fakeSpan("child21", instantWithOffsetMicros(traceStart, 200), instantWithOffsetMicros(traceStart, 300)),
      fakeSpan("child22", instantWithOffsetMicros(traceStart, 300), instantWithOffsetMicros(traceStart, 400))
    )

    val modifiedSpan = createModifiedSpan(allSpans, new FixedOptimizer("child1", -100))

    assertEquals(400, allSpans.head.durationMicros)
    assertEquals(500, modifiedSpan.durationMicros)
  }

  private class FixedOptimizer(var optimizations: Map[String, Long] = Map()) extends SpanModifier {
    def this(name: String, optimization: Long) = {
      this(Map(name -> optimization))
    }

    override def matchSpan(span: Domain.Span): Boolean = {
      optimizations.contains(span.operationName)
    }

    override def getEndTimeUpdateMicros(span: Domain.Span): Long = {
      optimizations.get(span.operationName) match {
        case Some(opt) => -opt
        case None => 0
      }
    }
  }

  private val traceStart = Instant.ofEpochMilli(0)

  private def fakeSpan(name: String, start: Instant, end: Instant, executionGroups: Seq[ExecutionGroup] = Seq()): Domain.Span = {
    Domain.Span(
      traceID = "traceID",
      spanID = name,
      operationName = name,
      startTime = start,
      endTime = end,
      executionGroups = executionGroups,
      tags = Seq(),
    )
  }

  private def createModifiedSpan(allSpans: Seq[Domain.Span], modifier: SpanModifier): Domain.Span = {
    SpanModifier.createModifiedSpan(allSpans.head, modifier, allSpans.map(s => (s.spanID, s)).toMap)._1
  }
}
