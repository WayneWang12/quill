package io.getquill.context.cassandra.lagom.stream

import io.getquill.context.cassandra.EncodingSpecHelper

class EncodingSpec extends EncodingSpecHelper {
  import io.getquill.context.cassandra.lagom.server.application.materializer
  import io.getquill.context.cassandra.lagom.server.application.executionContext
  import testStreamDB._
  "encodes and decodes types" - {
    "stream" in {
      val result =
        for {
          _ <- testStreamDB.run(query[EncodingTestEntity].delete).runForeach(_ => ())
          _ <- testStreamDB.run(liftQuery(insertValues).foreach(e => query[EncodingTestEntity].insert(e))).runForeach(_ => ())
          result <- testStreamDB.run(query[EncodingTestEntity]).runFold(List.empty[EncodingTestEntity])(_ :+ _)
        } yield {
          result
        }
      verify(await(result))
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
          _ <- testStreamDB.run(query[EncodingTestEntity].delete).runForeach(_ => ())
          _ <- testStreamDB.run(liftQuery(insertValues).foreach(e => query[EncodingTestEntity].insert(e))).runForeach(_ => ())
          result <- testStreamDB.run(q(liftQuery(insertValues.map(_.id)))).runFold(List.empty[EncodingTestEntity])(_ :+ _)
        } yield {
          result
        }
      verify(await(result))
    }
  }
}
