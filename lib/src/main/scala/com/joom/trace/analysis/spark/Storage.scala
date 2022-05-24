package com.joom.trace.analysis.spark

object Storage {
  case class Tag(key: String, vStr: String)

  case class Reference(traceId: String, spanId: String)

  case class Process(serviceName: String, tags: Option[Seq[Tag]])

  case class Span(
                   traceId: String,
                   spanId: String,
                   operationName: String,
                   references: Option[Seq[Reference]] = None,
                   flags: Option[Int] = Some(0),
                   startTime: String,
                   duration: String,
                   tags: Option[Seq[Tag]] = None,
                   process: Option[Process] = None,
                 )

  case class Trace(id: String, spans: Seq[Span])
}
