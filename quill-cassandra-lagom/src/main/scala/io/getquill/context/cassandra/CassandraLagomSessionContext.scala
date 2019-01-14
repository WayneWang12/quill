package io.getquill.context.cassandra

import com.datastax.driver.core._
import com.lightbend.lagom.scaladsl.persistence.cassandra.CassandraSession
import io.getquill.NamingStrategy

import scala.concurrent.{ Await, ExecutionContext, Future }
import scala.util.Try

abstract class CassandraLagomSessionContext[N <: NamingStrategy](
  val naming: N,
  session:    CassandraSession
) extends AbstractCassandraSessionContext[N] {

  override type PrepareRow = BoundStatement
  override type ResultRow = Row

  override type RunActionReturningResult[T] = Unit
  override type RunBatchActionReturningResult[T] = Unit

  protected def prepareAsync(cql: String)(implicit executionContext: ExecutionContext): Future[BoundStatement] = {
    session.prepare(cql).map(_.bind())
  }

  def close() = {
  }

  def probe(cql: String) = {
    import scala.concurrent.ExecutionContext.Implicits.global
    import scala.concurrent.duration._
    Try {
      Await.result(prepareAsync(cql), 1.minute)
      ()
    }
  }

}
