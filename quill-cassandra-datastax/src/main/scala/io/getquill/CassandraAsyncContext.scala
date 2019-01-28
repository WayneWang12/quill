package io.getquill

import com.datastax.driver.core.{Cluster, Statement}
import com.typesafe.config.Config
import io.getquill.context.cassandra.util.FutureConversions._
import io.getquill.monad.ScalaFutureIOMonad
import io.getquill.util.{ContextLogger, LoadConfig}

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}

class CassandraAsyncContext[N <: NamingStrategy](
                                                  naming: N,
                                                  cluster: Cluster,
                                                  keyspace: String,
                                                  preparedStatementCacheSize: Long
                                                )
  extends CassandraDatastaxSessionContext[N](naming, cluster, keyspace, preparedStatementCacheSize)
    with ScalaFutureIOMonad {

  def this(naming: N, config: CassandraContextConfig) = this(naming, config.cluster, config.keyspace, config.preparedStatementCacheSize)

  def this(naming: N, config: Config) = this(naming, CassandraContextConfig(config))

  def this(naming: N, configPrefix: String) = this(naming, LoadConfig(configPrefix))

  protected val logger = ContextLogger(classOf[CassandraAsyncContext[_]])

  override type Result[T] = Future[T]
  override type RunQueryResult[T] = List[T]
  override type RunQuerySingleResult[T] = T
  override type RunActionResult = Unit
  override type RunBatchActionResult = Unit

  override def performIO[T](io: IO[T, _], transactional: Boolean = false)(implicit ec: ExecutionContext): Result[T] = {
    if (transactional) logger.underlying.warn("Cassandra doesn't support transactions, ignoring `io.transactional`")
    super.performIO(io)
  }

  override def queryAll[T](statement: Future[Statement], extractor: Extractor[T])(implicit executionContext: ExecutionContext): Future[List[T]] = {
    statement.flatMap(st => session.executeAsync(st))
      .map(_.all.asScala.toList.map(extractor))
  }

  override def extractOneFromAll[T](result: Future[List[T]])(implicit executionContext: ExecutionContext): Future[T] = {
    result.map(handleSingleResult)
  }

  override def runAction(statement: Future[Statement])(implicit executionContext: ExecutionContext): Future[Unit] = {
    statement.flatMap(st => session.executeAsync(st)).map(_ => ())
  }

  override def runBatchAction(groups: List[BatchGroup])(implicit executionContext: ExecutionContext): Future[Unit] = {
    Future.sequence {
      groups.flatMap {
        case BatchGroup(cql, prepare) =>
          prepare.map(executeAction(cql, _))
      }
    }.map(_ => ())
  }
}
