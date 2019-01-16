package io.getquill.context.cassandra.lagom.stream

import io.getquill.context.cassandra.QueryResultTypeCassandraSpec

class QueryResultTypeCassandraStreamSpec extends QueryResultTypeCassandraSpec {

  val context = testStreamDB

  def result[T](t: Observable[T]) =
    await(t.foldLeftL(List.empty[T])(_ :+ _).runToFuture)

  override def beforeAll = {
    result(context.run(deleteAll))
    result(context.run(liftQuery(entries).foreach(e => insert(e))))
    ()
  }

  "query" in {
    result(context.run(selectAll)) mustEqual entries
  }

  "querySingle" - {
    "size" in {
      result(context.run(entitySize)) mustEqual List(3)
    }
    "parametrized size" in {
      result(context.run(parametrizedSize(lift(10000)))) mustEqual List(0)
    }
  }
}
