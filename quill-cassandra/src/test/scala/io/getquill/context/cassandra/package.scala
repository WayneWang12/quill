package io.getquill.context

import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.duration.Duration

import io.getquill._
import io.getquill.Literal

package object cassandra {

  lazy val mirrorContext = new CassandraMirrorContext(Literal) with CassandraTestEntities

  def await[T](f: Future[T]): T = Await.result(f, Duration.Inf)
}
