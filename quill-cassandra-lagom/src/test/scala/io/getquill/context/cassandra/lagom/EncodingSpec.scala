package io.getquill.context.cassandra.lagom

import io.getquill.context.cassandra.EncodingSpecHelper

class EncodingSpec extends EncodingSpecHelper {
  import testLagomAsyncDB._
  import scala.concurrent.ExecutionContext.Implicits.global
  "encodes and decodes types" - {
    "stream" in {
      val result =
        for {
          _ <- testLagomAsyncDB.run(query[EncodingTestEntity].delete)
          _ <- testLagomAsyncDB.run(liftQuery(insertValues).foreach(e => query[EncodingTestEntity].insert(e)))
          result <- testLagomAsyncDB.run(query[EncodingTestEntity])
        } yield {
          result
        }
      verify(await(result).toList)
    }
  }

  "encodes collections" - {
    "stream" in {
      val q = quote {
        (list: Query[Int]) =>
          query[EncodingTestEntity].filter(t => list.contains(t.id))
      }
      val result =
        for {
          _ <- testLagomAsyncDB.run(query[EncodingTestEntity].delete)
          _ <- testLagomAsyncDB.run(liftQuery(insertValues).foreach(e => query[EncodingTestEntity].insert(e)))
          result <- testLagomAsyncDB.run(q(liftQuery(insertValues.map(_.id))))
        } yield {
          result
        }
      verify(await(result).toList)
    }
  }
}
