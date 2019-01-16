package io.getquill.context.cassandra.lagom.stream

import io.getquill._

class DecodeNullSpec extends Spec {
  import io.getquill.context.cassandra.lagom.server.application.materializer
  import io.getquill.context.cassandra.lagom.server.application.executionContext
  import testStreamDB._

  "no default values when reading null" - {
    "stream" in {
      val writeEntities = quote(querySchema[DecodeNullTestWriteEntity]("DecodeNullTestEntity"))

      val result =
        for {
          _ <- testStreamDB.run(writeEntities.delete).runForeach(_ => ())
          _ <- testStreamDB.run(writeEntities.insert(lift(insertValue))).runForeach(_ => ())
          result <- testStreamDB.run(query[DecodeNullTestEntity]).runForeach(t => t)
        } yield {
          result
        }
      intercept[IllegalStateException] {
        await {
          result
        }
      }
    }
  }

  case class DecodeNullTestEntity(id: Int, value: Int)

  case class DecodeNullTestWriteEntity(id: Int, value: Option[Int])

  val insertValue = DecodeNullTestWriteEntity(0, None)
}
