package io.getquill.context.cassandra.lagom

import io.getquill._

class DecodeNullSpec extends Spec {
  import testLagomAsyncDB._
  import scala.concurrent.ExecutionContext.Implicits.global

  "no default values when reading null" - {
    "stream" in {
      val writeEntities = quote(querySchema[DecodeNullTestWriteEntity]("DecodeNullTestEntity"))

      val f =
        for {
          _ <- testLagomAsyncDB.run(writeEntities.delete)
          _ <- testLagomAsyncDB.run(writeEntities.insert(lift(insertValue)))
          result <- testLagomAsyncDB.run(query[DecodeNullTestEntity])
        } yield {
          result
        }
      intercept[IllegalStateException] {
        await {
          f
        }
      }
    }
  }

  case class DecodeNullTestEntity(id: Int, value: Int)

  case class DecodeNullTestWriteEntity(id: Int, value: Option[Int])

  val insertValue = DecodeNullTestWriteEntity(0, None)
}
