
package org.apache.spark.traceable

import hu.sztaki.ilab.traceable

import scala.reflect.ClassTag

class Wrapper[T: ClassTag] extends traceable.Wrapper[T] {

}

object Wrapper {

}