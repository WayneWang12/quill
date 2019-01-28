package io.getquill.context.cassandra

import com.datastax.driver.core._
import io.getquill.NamingStrategy
import io.getquill.context.Context
import io.getquill.context.cassandra.encoding.{CassandraTypes, Decoders, Encoders, UdtEncoding}
import io.getquill.util.ContextLogger
import io.getquill.util.Messages.fail

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

abstract class CassandraSessionContext[N <: NamingStrategy]
  extends Context[CqlIdiom, N]
    with CassandraContext[N]
    with Encoders
    with Decoders
    with CassandraTypes
    with UdtEncoding {

  val idiom = CqlIdiom

  override type PrepareRow = BoundStatement
  override type ResultRow = Row

  override type RunActionReturningResult[T] = Unit
  override type RunBatchActionReturningResult[T] = Unit

  protected def prepare(cql: String): BoundStatement

  protected def prepareAsync(cql: String)(implicit executionContext: ExecutionContext): Future[BoundStatement]

  def probe(cql: String): Try[_] =
    Try {
      prepare(cql)
      ()
    }

  def queryAll[T](preparedRow: Future[Statement], extractor: Extractor[T])(implicit executionContext: ExecutionContext): Result[RunQueryResult[T]]

  def extractOneFromAll[T](result: Result[RunQueryResult[T]])(implicit executionContext: ExecutionContext): Result[RunQuerySingleResult[T]]

  def runAction(preparedRow: Future[Statement])(implicit executionContext: ExecutionContext): Result[RunActionResult]

  def runBatchAction(groups: List[BatchGroup])(implicit executionContext: ExecutionContext): Result[RunBatchActionResult]

  private val logger: ContextLogger = ContextLogger(this.getClass)

  private def prepareAndGetRow(cql: String, prepare: Prepare)(implicit executionContext: ExecutionContext): Future[BoundStatement] = {
    val prepareResult = this.prepareAsync(cql).map(prepare)
    val preparedRow = prepareResult.map {
      case (params, bs) =>
        logger.logQuery(cql, params)
        bs
    }
    preparedRow
  }

  def executeQuery[T](cql: String, prepare: Prepare = identityPrepare, extractor: Extractor[T] = identityExtractor): Result[RunQueryResult[T]] = {
    queryAll(prepareAndGetRow(cql, prepare), extractor)
  }

  def executeQuerySingle[T](cql: String, prepare: Prepare = identityPrepare, extractor: Extractor[T] = identityExtractor): Result[RunQuerySingleResult[T]] = {
    extractOneFromAll(executeQuery(cql, prepare, extractor))
  }

  def executeAction[T](cql: String, prepare: Prepare = identityPrepare): Result[RunActionResult] = {
    runAction(prepareAndGetRow(cql, prepare))
  }

  def executeBatchAction(groups: List[BatchGroup]): Result[RunBatchActionResult] = {
    runBatchAction(groups)
  }

  def executeActionReturning[O](sql: String, prepare: Prepare = identityPrepare, extractor: Extractor[O], returningColumn: String): Unit =
    fail("Cassandra doesn't support `returning`.")

  def executeBatchActionReturning[T](groups: List[BatchGroupReturning], extractor: Extractor[T]): Unit =
    fail("Cassandra doesn't support `returning`.")
}
