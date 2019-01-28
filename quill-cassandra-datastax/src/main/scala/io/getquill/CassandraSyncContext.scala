package io.getquill

import com.datastax.driver.core.{Cluster, Statement}
import com.typesafe.config.Config
import io.getquill.monad.SyncIOMonad
import io.getquill.util.{ContextLogger, LoadConfig}

import scala.collection.JavaConverters._
import scala.concurrent.{Await, ExecutionContext, Future}

class CassandraSyncContext[N <: NamingStrategy](
                                                 naming: N,
                                                 cluster: Cluster,
                                                 keyspace: String,
                                                 preparedStatementCacheSize: Long
                                               )
  extends CassandraDatastaxSessionContext[N](naming, cluster, keyspace, preparedStatementCacheSize)
    with SyncIOMonad {

  def this(naming: N, config: CassandraContextConfig) = this(naming, config.cluster, config.keyspace, config.preparedStatementCacheSize)

  def this(naming: N, config: Config) = this(naming, CassandraContextConfig(config))

  def this(naming: N, configPrefix: String) = this(naming, LoadConfig(configPrefix))

  private val logger = ContextLogger(classOf[CassandraSyncContext[_]])

  override type Result[T] = T
  override type RunQueryResult[T] = List[T]
  override type RunQuerySingleResult[T] = T
  override type RunActionResult = Unit
  override type RunBatchActionResult = Unit

  override def performIO[T](io: IO[T, _], transactional: Boolean = false): Result[T] = {
    if (transactional) logger.underlying.warn("Cassandra doesn't support transactions, ignoring `io.transactional`")
    super.performIO(io)
  }

  import scala.concurrent.duration._

  private def await[T](future: Future[T]) = {
    Await.result(future, 1.minute)
  }

  private val ec = scala.concurrent.ExecutionContext.Implicits.global

  override def queryAll[T](preparedRow: Future[Statement], extractor: Extractor[T])(implicit executionContext: ExecutionContext = ec): List[T] = {
    val bs = await(preparedRow)
    session.execute(bs)
      .all.asScala.toList.map(extractor)
  }

  override def extractOneFromAll[T](result: List[T])(implicit executionContext: ExecutionContext = ec): T = {
    handleSingleResult(result)
  }

  override def runAction(preparedRow: Future[Statement])(implicit executionContext: ExecutionContext = ec): Unit = {
    val bs = await(preparedRow)
    session.execute(bs)
    ()
  }

  override def runBatchAction(groups: List[BatchGroup])(implicit executionContext: ExecutionContext = ec): Unit = {
    groups.foreach {
      case BatchGroup(cql, prepare) =>
        prepare.foreach(executeAction(cql, _))
    }
  }
}
