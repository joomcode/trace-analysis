package com.joom.trace.analysis.spark

import java.time.Instant

object Storage {
  case class Tag(key: String, value: String)

  case class Reference(traceID: String, spanID: String)

  case class Process(serviceName: String, tags: Seq[Tag])

  case class Span(
                   traceID: String,
                   spanID: String,
                   operationName: String,
                   references: Seq[Reference] = Seq(),
                   flags: Int = 0,
                   startTime: Instant,
                   endTime: Instant,
                   tags: Seq[Tag] = Seq(),
                   process: Process = null,
                 )

  case class Trace(id: String, spans: Seq[Span])
}
