package io.getquill.context.cassandra

import com.lightbend.lagom.scaladsl.api.ServiceLocator.NoServiceLocator
import com.lightbend.lagom.scaladsl.broker.kafka.LagomKafkaComponents
import com.lightbend.lagom.scaladsl.persistence.cassandra.CassandraPersistenceComponents
import com.lightbend.lagom.scaladsl.playjson.{ EmptyJsonSerializerRegistry, JsonSerializerRegistry }
import com.lightbend.lagom.scaladsl.server.{ LagomApplication, LagomServer }
import com.lightbend.lagom.scaladsl.testkit.ServiceTest
import io.getquill.context.cassandra.lagom.utils.DummyService
import io.getquill.{ Literal, _ }
import play.api.libs.ws.ahc.AhcWSComponents

import scala.concurrent.duration.Duration
import scala.concurrent.{ Await, Future }
import scala.io.Source

package object lagom {

  lazy val server = ServiceTest.startServer(ServiceTest.defaultSetup.withCassandra(true)) { ctx =>
    new LagomApplication(ctx) with AhcWSComponents with CassandraPersistenceComponents with LagomKafkaComponents {

      override def serviceLocator = NoServiceLocator

      override def lagomServer: LagomServer = serverFor[DummyService](new DummyService)

      environment
        .resource("cql/cassandra-schema.cql")
        .map(url => Source.fromURL(url).mkString.split(';')).foreach { cqls =>
          cqls.foreach { cql =>
            if (!cql.trim.isEmpty) {
              await(cassandraSession.executeCreateTable(cql))
            }
          }
        }

      override def jsonSerializerRegistry: JsonSerializerRegistry = EmptyJsonSerializerRegistry
    }
  }

  lazy val testLagomAsyncDB = new CassandraLagomAsyncContext(Literal, server.application.cassandraSession) with CassandraTestEntities

  def await[T](f: Future[T]): T = Await.result(f, Duration.Inf)
}
