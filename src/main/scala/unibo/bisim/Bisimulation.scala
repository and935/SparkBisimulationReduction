package unibo.bisim

import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer

object Bisimulation extends Serializable {

  /**
   * Given the RDD of transition (dst, (src, labId, dst)) and the RDD (state, Signature)
   * replace each state dst with his correspondent Signature and
   * returns the new RDD (src, (labId, idDst))
   */
  def signZip(preProcessedTransitionRDD: RDD[(StateId, TransitionCompressed)],
              stateSignatureRDD: StateSignatureRDD
             ): RDD[(StateId, (LabelId, Signature))] = {
    preProcessedTransitionRDD.zipPartitions(stateSignatureRDD) (
      (iteratorTransition, iteratorStateSignature) => {
        val mapStateSignature = iteratorStateSignature.toMap
        for {
          (dstKey, (src, lab, dst)) <- iteratorTransition
        } yield (src, (lab, mapStateSignature(dstKey)))
      }
    )
  }

  /**
   * Given the accumulator for the signature acc and a tuple (LabelId, Signature)
   * add the tuple, if is not present yet, in the accumulator preserving the order
   */
  def mergeValue(acc: ArrayBuffer[(LabelId, Signature)],
                 next: (LabelId, Signature)): ArrayBuffer[(LabelId, Signature)] = {
    LtsUtils.addOccInOrderToArray(acc, next)
    acc
  }

  /**
   * Given two ArrayBuffer of (LabelId, Signature) sorted merge them
   * in an unique ArrayBuffer (LabelId, Signature) preserving the order,
   * eliminating the duplicates
   */
  def mergeComb(left: ArrayBuffer[(LabelId, Signature)],
                right: ArrayBuffer[(LabelId, Signature)]): Unit = {
    left.foreach(el =>
      LtsUtils.addOccInOrderToArray(right, el)
    )
  }

  /**
   * Given two ArrayBuffer of (LabelId, Signature) sorted select the small one and
   * add each element of it in the greater ArrayBuffer preserving the order.
   * In case of duplicates, maintain only one occurrence.
   * Return the ArrayBuffer greater modified
   */
  def mergeCombiners(leftOp: ArrayBuffer[(LabelId, Signature)],
                     rightOp: ArrayBuffer[(LabelId, Signature)]
                    ): ArrayBuffer[(LabelId, Signature)] = {
    if (leftOp.length < rightOp.length) {
      mergeComb(leftOp, rightOp)
      rightOp
    } else {
      mergeComb(rightOp, leftOp)
      leftOp
    }
  }

  /**
   * Given the RDD of transitions with the previous signature associated to each dst state
   * calculates the new Signature for each state.
   * Return the new RDD (state, Signature)
   */
  def aggregate(transitionsWithId: RDD[(StateId, (LabelId, Signature))],
                numPartition: Int): StateSignatureRDD = {
    //initialize the accumulator for a signature with an empty array
    val signature = ArrayBuffer.empty[(LabelId, Signature)]

    //compute the aggregation
    val stateSignature = transitionsWithId
      .aggregateByKey(signature, numPartition)(mergeValue, mergeCombiners)

    //calculates the Hash md5 for each signature
    stateSignature.mapValues(LtsUtils.hashMd5)
  }

  /**
   * Check if there was an increment in the number of block among two iteration
   */
  def checkConditionTermination(oldBlockCount: Long, newBlockCount: Long): Boolean =
    oldBlockCount == newBlockCount

  /**
   * Given the RDD (state, Signature) return the RDD (sate, Signature)
   * partitioned by an HashPartitioner for the integer numberPartition and
   * persisted in memory at the level storageLevelStateSignature
   */
  def preProcessStateSignature(stateSignatureRDD: StateSignatureRDD,
                               storageLevelStateSignature: StorageLevel,
                               numberPartition: Int): StateSignatureRDD = {
    stateSignatureRDD
      .partitionBy(new HashPartitioner(numberPartition))
      .persist(storageLevelStateSignature)
  }

  /**
   * Given the RDD of transition (src, labId, dst) return the RDD (dst, (src, labId, dst))
   * partitioned by an HashPartitioner for the integer numberPartition and
   * persisted in memory at level storageLevelTransition
   */
  def preProcessCompressedTransitionRDD(compressedTransitionRDD: TransitionCompressedRDD,
                                        storageLevelTransition: StorageLevel,
                                        numberPartition: Int): RDD[(StateId, TransitionCompressed)] = {
    val transitionKeyDestRDD = compressedTransitionRDD.keyBy {
      case (_, _, destId) => destId
    }
    transitionKeyDestRDD
      .partitionBy(new HashPartitioner(numberPartition))
      .persist(storageLevelTransition)
  }

  /**
   * Core of the algorithm
   * Calculates the new signature for each state and
   * refines the blocks until there isn't any possible refinement
   */
  @scala.annotation.tailrec
  def refinement(preProcessedTransitionRDD: RDD[(StateId, TransitionCompressed)],
                 stateSignatureRDD: StateSignatureRDD,
                 oldBlockCount: Long,
                 newBlockCount: Long,
                 numPartition: Int,
                 numPartitionCoalesced: Int
                ): StateSignatureRDD = {
    //Check if there was an increment in the number of block in the previous iteration
    //If not, terminate the execution.
    if (checkConditionTermination(oldBlockCount, newBlockCount))
      return stateSignatureRDD
    //(srcId, labId, signatureDstId)
    val transitionSignRDD = signZip(preProcessedTransitionRDD, stateSignatureRDD)
      .partitionBy(new HashPartitioner(numPartition))

    stateSignatureRDD.unpersist()
    //RDD (state, newSignature)
    val newStateSignatureRDD = aggregate(transitionSignRDD, numPartition)
      .persist(StorageLevel.MEMORY_AND_DISK_SER)


    val blockIdRDD = LtsUtils.countBlockOccurrences(newStateSignatureRDD, numPartitionCoalesced)
    //number of blocks for the new signatures
    val newBlockCountUpdated = LtsUtils.countBlock(blockIdRDD)

    //reiterate
    refinement(
      preProcessedTransitionRDD,
      newStateSignatureRDD,
      newBlockCount,
      newBlockCountUpdated,
      numPartition,
      numPartitionCoalesced
    )
  }

  def run(lts: LtsBisimulation,
          storageLevelTransition: StorageLevel,
          storageLevelStates: StorageLevel,
          numPartition: Int,
          numPartitionCoalesced: Int
         ): StateSignatureRDD = {
    //RDD of compressed transition in the form (dstId, (srcId, labId, dstId))
    var preProcessedTransitionRDD: Option[RDD[(StateId, TransitionCompressed)]] = None
    //RDD (state, Signature)
    var stateSignatureRDD: Option[StateSignatureRDD] = None
    //Number of block before refinement
    val oldBlockCount = 0
    //Number of block after refinement
    var newBlockCount = 0

    /**
     * Initialize preProcessedTransitionRDD and stateSignatureRDD with
     * the data contained in lts
     */
    def init(): Unit = {

      //initialize preProcessedTransitionRDD with the compressed transition of lts
      preProcessedTransitionRDD = Some(preProcessCompressedTransitionRDD(
        lts.transitionRDD,
        storageLevelTransition,
        numPartition
      ))

      //initialize stateSignatureRDD with the stateSignatureRDD of lts
      stateSignatureRDD = Some(preProcessStateSignature(
        lts.stateSignatureRDD,
        storageLevelStates,
        numPartition
      ))
      //initialize the number of blocks
      newBlockCount = 1
    }
    //initialize all data structure
    init()

    val reverseTransitionRDD = preProcessedTransitionRDD.get

    //refinement Blocks until fixed Point
    val stateSignatureResultRDD = refinement(
      reverseTransitionRDD,
      stateSignatureRDD.get,
      oldBlockCount,
      newBlockCount,
      numPartition,
      numPartitionCoalesced
    )

    reverseTransitionRDD.unpersist()
    stateSignatureResultRDD
  }
}
