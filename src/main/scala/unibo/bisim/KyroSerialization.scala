package unibo.bisim

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

trait KryoSerialization {

  /**
   * Registers internal case classes for Kryo serialization.
   */
  def useKryo(conf: SparkConf): Unit = {
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryo.referenceTracking", "false")
    conf.registerKryoClasses(Array(
      classOf[Transition],
      classOf[TransitionRDD],
      classOf[TransitionCompressed],
      classOf[TransitionCompressedRDD],
      classOf[(StateId, TransitionCompressed)],
      classOf[StateSignatureRDD],
      classOf[Signature],
      classOf[(String, Int)],
      classOf[RDD[(StateId, (LabelId, Signature))]],
      classOf[ArrayBuffer[(LabelId, Signature)]],
      classOf[RDD[(StateId, ArrayBuffer[(LabelId, Signature)])]]
    ))
  }
}