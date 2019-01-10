package io.getquill.context.cassandra.util

import com.google.common.util.concurrent.{ FutureCallback, Futures, ListenableFuture, MoreExecutors }

import scala.concurrent.{ Future, Promise }
import scala.language.implicitConversions

object FutureConversions {

  implicit def toScalaFuture[T](fut: ListenableFuture[T]): Future[T] = {
    val p = Promise[T]()
    Futures.addCallback(
      fut,
      new FutureCallback[T] {
        def onSuccess(r: T) = {
          p.success(r)
          ()
        }
        def onFailure(t: Throwable) = {
          p.failure(t)
          ()
        }
      },
      MoreExecutors.directExecutor()
    )
    p.future
  }
}
