package unibo.bisim

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

/**
 * Lts represented with RDD of transitions in the form (src, labelId, dest) where labelId is a Long
 * for save memory space.
 * The RDD Label contains all the label String associated with a labelId.
 */
final case class LtsBisimulation(transitionRDD: TransitionCompressedRDD,
                                 stateSignatureRDD: StateSignatureRDD,
                                 label: RDD[(String, Long)],
                                 numberTransitions: Int,
                                 numberStates: Int
                                )

object Lts {

  /**
   * Given an RDD of transitions (srcInt, StringLabel, dstInt) takes all the distinct label
   * and associates at each of them an unique Long value
   * Return the RDD Label (StringLabel, LongLabelId)
   */
  def processLabel(transitions: TransitionRDD): RDD[(String, Long)] = {
    transitions
      .map(_._2)
      .distinct()
      .zipWithIndex()
  }

  /**
   * Given an RDD of transition (src, labelString, dst) replace each labelString with labelId in the
   * argument labelMap.
   * Return the new RDD (src, labelId, dst)
   */
  def compressTransitionsFromMap(transitionRDD: TransitionRDD,
                                 labelMap: scala.collection.Map[String, LabelId]): TransitionCompressedRDD = {
    transitionRDD.map {
      case (src, lab, dst) => //(Int, String, Int)
        (src, labelMap(lab), dst) //(Int, Long, Int)
    }
  }

  /**
   * Given an RDD of state, calculate the initial signature for each state and
   * return the RDD (state, InitialSignature)
   */
  def initializeStateSignature(stateRDD: RDD[StateId]): StateSignatureRDD = {
    val initialHashSignature = LtsUtils.hashMd5(ArrayBuffer.empty)
    stateRDD.map(x => (x, initialHashSignature))
  }

  /**
   * Return the RDD of states that are deadlock
   */
  def findDeadLocks(compressedTransitionRDD: TransitionCompressedRDD,
                    stateSignatureRDD: StateSignatureRDD): RDD[StateId] = {
    val srcRDD = compressedTransitionRDD.map(_._1)
    val stateRDD = stateSignatureRDD.keys
    stateRDD.subtract(srcRDD)
  }

  /**
   * Assign an index to each signature of stateSignatureRDD
   */
  def distinctAndZipWithIndexBlockId(stateSignatureRDD: StateSignatureRDD,
                                     numPartition: Int): RDD[(String, LabelId)] = {
    val blockIDRDD = LtsUtils.countBlockOccurrences(stateSignatureRDD, numPartition)
    blockIDRDD.keys.zipWithIndex()
  }

  /**
   * Convert the signature from type Array[Byte] to String
   */
  def convertSignature(stateSignatureRDD: StateSignatureRDD): RDD[(String, StateId)] = {
    val stateSignatureStrRDD = stateSignatureRDD.mapValues(x => x.map(_.toChar).mkString)
    stateSignatureStrRDD.map(_.swap)
  }

  /**
   *  Replace the signature of Array[Byte] with an progressive integer
   */
  def joinAndCompress(signatureStrStateRDD: RDD[(String, StateId)],
                      blockIdWithIndexRDD: RDD[(String, Long)]): RDD[(StateId, LabelId)] = {
    signatureStrStateRDD.join(blockIdWithIndexRDD)
      .map {
        case (_, (state, signatureId)) =>
          (state, signatureId)
      }
  }

  /**
   *  Save in a file a set of ArrayBuffer of states. Each ArrayBuffer contains the states
   *  that are in the same block and so are bisimilar.
   */
  def saveBlockStates(sparkContext: SparkContext,
                      stateSignatureRDD: StateSignatureRDD,
                      numPartitionCoalesced: Int,
                      path: String
                     ): Unit = {

    val SignatureStrStateRDD = stateSignatureRDD
      .mapValues(x => x.map(_.toChar).mkString)
      .map(_.swap)
      .coalesce(numPartitionCoalesced)

    def mergeValues(accumulator: ArrayBuffer[StateId],
                    next: StateId): ArrayBuffer[StateId] =
      accumulator += next

    def mergeCombiners(leftOp: ArrayBuffer[StateId],
                       leftRight: ArrayBuffer[StateId]) = {
     leftOp ++=  leftRight
    }

    SignatureStrStateRDD
      .aggregateByKey(ArrayBuffer.empty[StateId])(mergeValues, mergeCombiners)
      .values
      .coalesce(1, shuffle =true)
      .saveAsTextFile(path)
  }

  /**
   *
   */
  def joinAndReplace(stateSignatureIdRDD: RDD[(StateId, Long)],
                     transitionCompressedRDD: TransitionCompressedRDD): TransitionCompressedRDD = {

    val reverseTransitionCompressedKeyValueRDD = transitionCompressedRDD
      .map {
        case (src, lab, dst) => (dst, (lab, src))
      }
    reverseTransitionCompressedKeyValueRDD
      .join(stateSignatureIdRDD)
      .map {
        case (_, ((lab, src), idBlockDst)) => (src, (lab, idBlockDst.toInt))
      }.join(stateSignatureIdRDD)
      .map {
        case (_, ((lab, idBlockDst), idBlockSrc)) => (idBlockSrc.toInt, lab, idBlockDst)
      }.distinct()
  }

  /**
   *
   */
  def decompressTransition(context: SparkContext,
                           transitionCompressedRDD: TransitionCompressedRDD,
                           labelRDD: RDD[(String, LabelId)]): TransitionRDD = {

    val labelBroadCastForWrite = context.broadcast(labelRDD.map(_.swap).collectAsMap().updated(-1L, "dead"))

    transitionCompressedRDD.map {
      case (src, labId, dst) => (
        src,
        labelBroadCastForWrite.value(labId),
        dst
      )
    }
  }

  /**
   *
   */
  def saveReducedLts(sparkContext: SparkContext,
                     lts: LtsBisimulation,
                     stateSignatureRDD: StateSignatureRDD,
                     numPartition: Int,
                     path: String
                    ): Unit = {
    //RDD (blockIdString, IdLong)
    val blockIdWithIndexRDD = Lts.distinctAndZipWithIndexBlockId(stateSignatureRDD, numPartition)
    //convert signature into String and swap key value for join after
    val signatureStrStateRDD = Lts.convertSignature(stateSignatureRDD)
    //
    val compressedStateSignatureRDD = joinAndCompress(signatureStrStateRDD, blockIdWithIndexRDD)
    //
    val reducedCompressedTransitionRDD = joinAndReplace(
      compressedStateSignatureRDD,
      lts.transitionRDD)
    //
    val reducedTransitionRDD = decompressTransition(sparkContext, reducedCompressedTransitionRDD, lts.label)
    // saveOnFileText
    reducedTransitionRDD.coalesce(1, shuffle = true).saveAsTextFile(path)
  }
}


