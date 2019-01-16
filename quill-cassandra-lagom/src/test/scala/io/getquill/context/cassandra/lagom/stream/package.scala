package io.getquill.context.cassandra.lagom

import io.getquill.{CassandraLagomStreamContext, Literal}
import io.getquill.context.cassandra.CassandraTestEntities
import io.getquill.context.cassandra.lagom

package object stream {
  lazy val testStreamDB = new CassandraLagomStreamContext(Literal, lagom.server.application.cassandraSession) with CassandraTestEntities
}
