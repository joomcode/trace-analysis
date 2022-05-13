package com.joom.trace.analysis.util

import java.time.temporal.ChronoUnit
import java.time.{Duration, Instant}

object TimeUtils {
  def instantWithOffsetMicros(instant: Instant, offsetMicros: Long): Instant = {
    instant.plus(offsetMicros, ChronoUnit.MICROS)
  }

  def durationMicros(start: Instant, end: Instant): Long = {
    Duration.between(start, end).toNanos / 1000
  }
}
