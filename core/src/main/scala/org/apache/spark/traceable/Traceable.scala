// scalastyle:off
package org.apache.spark.traceable

import scala.reflect.ClassTag
import hu.sztaki.ilab.traceable
import hu.sztaki.ilab.traceable.Wrapper.Attachment
import hu.sztaki.ilab.traceable.{Dependency, Event, Trace}
import org.apache.spark.util.collection.CompactBuffer

import scala.collection.immutable.HashMap

case class afterMapPartition() extends Event()
case class beforeCoGroupAggregate(numberOfRDDs: Int, numberOfDependencies: Int = -1)
extends Event()

class Traceable[T: ClassTag](newPayload: T, traceOption: Option[_])
extends traceable.Traceable[T](newPayload, traceOption) {
  protected type CoGroup = CompactBuffer[Any]
  protected type CoGroupValue = (Any, Int)
  protected type CoGroupCombiner = Array[CoGroup]

  def this(newPayload: T) = {
    this(newPayload, Some(Trace()))
  }

  def this(p: T, t: Trace) = this(p, Some(t))

  def this(p: T, traces: TraversableOnce[Trace]) = {
    this(p, if (traces.nonEmpty) Some(new Dependency(traces)) else None)
  }

  def this(payload: T, original: Traceable[_]) = this(payload, original.dependencies)

  def this(payload: T, original: traceable.Traceable[_]) = this(payload, original.dependencies)

  def this(p: T, collection: Iterator[traceable.Traceable[traceable.Traceable[T]]]) =
    this(p, traceable.Traceable.createDependencies[T](collection))

  override def interceptPoke(event: Event): PartialFunction[Any, Traceable.this.type] = {
    case afterMapPartition() =>
      log.info("Poked by an [afterMapPartition].")
      this(event).asInstanceOf[this.type]
    case beforeCoGroupAggregate(r, d) =>
      log.info("Poked by a beforeCoGroupAggregate event.")
      val initialCombiner = Array.fill(r)(new CoGroup())
      ^ match {
        case (a: Any, i: Int) =>
          initialCombiner(i) += a
        case cgv =>
          initialCombiner(d) += cgv
      }
      new Traceable[CoGroupCombiner](initialCombiner, ˇ).asInstanceOf[this.type]
  }
}
