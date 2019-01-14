package io.getquill.context.cassandra.lagom.utils

import akka.NotUsed
import com.lightbend.lagom.scaladsl.api.{ Descriptor, Service, ServiceCall }

import scala.concurrent.Future

class DummyService extends Service {

  def hello(): ServiceCall[NotUsed, NotUsed] = ServiceCall { _ =>
    Future.successful(NotUsed)
  }

  override def descriptor: Descriptor = {
    import Service._
    named("dummy").withCalls(
      pathCall("/dummy", hello _)
    )
  }
}
