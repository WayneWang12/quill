package io.getquill

import akka.stream.scaladsl.Source
import akka.{Done, NotUsed}
import com.lightbend.lagom.scaladsl.persistence.cassandra.CassandraSession
import io.getquill.context.cassandra.{AbstractCassandraSessionContext, CassandraLagomSessionContext}
import io.getquill.util.ContextLogger

import scala.concurrent.{ExecutionContext, Future}

class CassandraLagomStreamContext[N <: NamingStrategy](
  naming:  N,
  session: CassandraSession
)
  extends CassandraLagomSessionContext[N](naming, session) {

  private val logger = ContextLogger(classOf[CassandraLagomStreamContext[_]])

  override type Result[T] = Source[T, NotUsed]
  override type RunQueryResult[T] = T
  override type RunQuerySingleResult[T] = Option[T]
  override type RunActionResult = Done
  override type RunBatchActionResult = Done

  override def executeQuery[T](cql: String, prepare: Prepare = identityPrepare, extractor: Extractor[T] = identityExtractor)(implicit ec: ExecutionContext): Source[T, NotUsed] = {
    Source.fromFutureSource {
      this.prepareAsync(cql).map(prepare).map {
        case (params, bs) =>
          logger.logQuery(cql, params)
          session.select(bs).map(extractor)
      }
    }.mapMaterializedValue(_ => NotUsed)
  }

  override def executeQuerySingle[T](cql: String, prepare: Prepare = identityPrepare, extractor: Extractor[T] = identityExtractor)(implicit ec: ExecutionContext): Future[Option[T]] = {
    this.prepareAsync(cql).map(prepare).flatMap {
      case (params, bs) =>
        logger.logQuery(cql, params)
        session.selectOne(bs).map(_.map(extractor))
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
