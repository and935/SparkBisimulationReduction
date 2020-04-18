package unibo

import org.apache.spark.rdd.RDD

package object bisim {
  type StateId = Int
  type Label = String
  type LabelId = Long
  type Transition = (StateId, Label, StateId)
  type TransitionRDD = RDD[Transition]
  type TransitionCompressed = (StateId, LabelId, StateId)
  type TransitionCompressedRDD = RDD[TransitionCompressed]
  type Signature = Array[Byte]
  type StateSignatureRDD = RDD[(StateId, Signature)]
}
