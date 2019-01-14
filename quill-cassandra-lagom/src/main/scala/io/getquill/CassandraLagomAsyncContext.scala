package io.getquill

import akka.Done
import com.lightbend.lagom.scaladsl.persistence.cassandra.CassandraSession
import io.getquill.context.cassandra.CassandraLagomSessionContext
import io.getquill.monad.ScalaFutureIOMonad
import io.getquill.util.ContextLogger

import scala.concurrent.{ ExecutionContext, Future }

class CassandraLagomAsyncContext[N <: NamingStrategy](
  naming:  N,
  session: CassandraSession
)
  extends CassandraLagomSessionContext[N](naming, session)
  with ScalaFutureIOMonad {

  private val logger = ContextLogger(classOf[CassandraLagomAsyncContext[_]])

  override type Result[T] = Future[T]
  override type RunQueryResult[T] = Seq[T]
  override type RunQuerySingleResult[T] = Option[T]
  override type RunActionResult = Done
  override type RunBatchActionResult = Done

  override def performIO[T](io: IO[T, _], transactional: Boolean = false)(implicit ec: ExecutionContext): Result[T] = {
    session.underlying()
    if (transactional) logger.underlying.warn("Cassandra doesn't support transactions, ignoring `io.transactional`")
    super.performIO(io)
  }

  def executeQuery[T](cql: String, prepare: Prepare = identityPrepare, extractor: Extractor[T] = identityExtractor)(implicit ec: ExecutionContext): Future[Seq[T]] =
    this.prepareAsync(cql).map(prepare).flatMap {
      case (params, bs) =>
        logger.logQuery(cql, params)
        session.selectAll(bs)
          .map(_.map(extractor))
    }

  def executeQuerySingle[T](cql: String, prepare: Prepare = identityPrepare, extractor: Extractor[T] = identityExtractor)(implicit ec: ExecutionContext): Future[Option[T]] = {
    this.prepareAsync(cql).map(prepare).flatMap {
      case (params, bs) =>
        logger.logQuery(cql, params)
        session.selectOne(bs)
          .map(_.map(extractor))
    }
  }

  def executeAction[T](cql: String, prepare: Prepare = identityPrepare)(implicit ec: ExecutionContext): Future[Done] = {
    this.prepareAsync(cql).map(prepare).flatMap {
      case (params, bs) =>
        logger.logQuery(cql, params)
        session.executeWrite(bs)
    }
  }

  def executeBatchAction(groups: List[BatchGroup])(implicit ec: ExecutionContext): Future[Done] =
    Future.sequence {
      groups.flatMap {
        case BatchGroup(cql, prepare) =>
          prepare.map(executeAction(cql, _))
      }
    }.map(_ => Done)
}
