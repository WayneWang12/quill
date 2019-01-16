package io.getquill.context.cassandra.lagom

import io.getquill.context.cassandra.QueryResultTypeCassandraSpec

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

class QueryResultTypeCassandraMonixSpec extends QueryResultTypeCassandraSpec {

  val context = testLagomAsyncDB
  import context._

  def result[T](t: Future[T]) = await(t)

  override def beforeAll = {
    result(context.run(deleteAll))
    result(context.run(liftQuery(entries).foreach(e => insert(e))))
    ()
  }

  "query" in {
    result(context.run(selectAll)) mustEqual entries
  }

//  "stream" in {
//    result(context.stream(selectAll)) mustEqual entries
//  }

  "querySingle" - {
    "size" in {
      result(context.run(entitySize)) mustEqual Option(3)
    }
    "parametrized size" in {
      result(context.run(parametrizedSize(lift(10000)))) mustEqual Option(0)
    }
  }
}
