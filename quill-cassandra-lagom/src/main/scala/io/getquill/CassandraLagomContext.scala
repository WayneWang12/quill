package io.getquill

import akka.Done
import com.datastax.driver.core.{BoundStatement, Statement}
import com.lightbend.lagom.scaladsl.persistence.cassandra.CassandraSession
import io.getquill.context.cassandra.CassandraSessionContext
import io.getquill.util.ContextLogger

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

class CassandraLagomContext[N <: NamingStrategy](
                                                  val naming: N,
                                                  session: CassandraSession
                                                )
  extends CassandraSessionContext[N] {

  protected val logger: ContextLogger = ContextLogger(this.getClass)

  override type Result[T] = Future[T]
  override type RunQuerySingleResult[T] = Option[T]
  override type RunQueryResult[T] = Seq[T]
  override type RunActionResult = Done
  override type RunBatchActionResult = Done

  override protected def prepare(cql: String): BoundStatement = {
    Await.result(prepareAsync(cql)(ExecutionContext.Implicits.global), 1.minute)
  }

  def prepareAsync(cql: String)(implicit executionContext: ExecutionContext): Future[BoundStatement] = {
    session.prepare(cql).map(_.bind())
  }

  override def close(): Unit = ???

  override def queryAll[T](preparedRow: Future[Statement], extractor: Extractor[T])(implicit executionContext: ExecutionContext): Future[Seq[T]] = {
    preparedRow.flatMap(session.selectAll).map(_.map(extractor))
  }

  override def extractOneFromAll[T](result: Future[Seq[T]])(implicit executionContext: ExecutionContext): Future[Option[T]] = {
    result.map(_.headOption)
  }

  override def runAction(preparedRow: Future[Statement])(implicit executionContext: ExecutionContext): Future[Done] = {
    preparedRow.flatMap(session.executeWrite)
  }

  override def runBatchAction(groups: List[BatchGroup])(implicit executionContext: ExecutionContext): Future[Done] = {
    Future.sequence {
      groups.flatMap {
        case BatchGroup(cql, prepare) =>
          prepare.map(executeAction(cql, _))
      }
    }.map(_ => Done)
  }
}

