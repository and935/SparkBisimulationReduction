package unibo.bisim

import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object LtsUtils {

  /**
   * Compare two signature sign1 and sign2 and return
   * 0 if they are equals
   * 1 if sign1 < sign2
   * -1 if sign1 > sign2
   */
  def compare(sign1: Signature, sign2: Signature): Int = {
    @scala.annotation.tailrec
    def compareIndex(index: Int, sign1: Signature, sign2: Signature): Int = {
      //array1 sameElements array2
      if (index == sign1.length) return 0
      //
      if (sign1(index) < sign2(index)) 1 //array1 lesser array2
      else if (sign1(index) > sign2(index)) -1 //array1 greater array2
      else compareIndex(index + 1, sign1, sign2) //continue to compare
    }

    compareIndex(0, sign1, sign2)
  }

  /**
   * Given a tuple labelSignature (labelId, Signature) and an ArrayBuffer ordered of (labelId, Signature)
   * insert in the correct position of the ArrayBuffer the tuple.
   * If the tuple is a duplicate in accOrdered then doesn't insert it.
   * accumulatorOrdered is sorted before on the labelId and then on the Signature.
   * For compare the signatures uses the function compare(sign1, sign2)
   */
  def addOccInOrderToArray(accOrdered: ArrayBuffer[(LabelId, Signature)],
                           labelSignature: (LabelId, Signature)): Unit = {

    @scala.annotation.tailrec
    def addOccInOrderToArray(index: Int,
                             accumulatorOrdered: ArrayBuffer[(LabelId, Signature)],
                             tuple: (LabelId, Signature)): Unit = {
      //if the argument tuple is greater of each tuple in accumulatorOrdered
      if (index >= accumulatorOrdered.length) {
        //add the tuple to tail
        accumulatorOrdered += tuple
      } else {
        val value = accumulatorOrdered(index)
        //labelId of tuple greater of labelId of current value
        if (value._1 < tuple._1) {
          //continue to search the correct position
          addOccInOrderToArray(index + 1, accumulatorOrdered, tuple)
        } //labelId equals of labelId of current value
        else if (value._1 == tuple._1) {
          //compare the correspondent signature
          compare(value._2, tuple._2) match {
            //case signature of the current value greater of signature of the tuple
            case -1 => accumulatorOrdered.insert(index, tuple)
            //case signature of value equals of signature of the tuple
            case 0 =>
            //case signature of value lesser of signature of the tuple
            case 1 => addOccInOrderToArray(index + 1, accumulatorOrdered, tuple)
          }
        } else {
          //found correct position
          accumulatorOrdered.insert(index, tuple)
        }
      }
    }

    addOccInOrderToArray(0, accOrdered, labelSignature)
  }

  /**
   * Given an ArrayBuffer of (Int, Signature) calculates the md5 for it
   * and return the hash Array[Byte] (Signature)
   */
  def hashMd5(value: ArrayBuffer[(LabelId, Signature)]): Signature = {
    import upickle.default._
    org.apache.commons.codec.digest.DigestUtils.md5(writeBinary(value))
  }

  /**
   * Given an RDD of (StateId, Signature) in each partition
   * calculates all the distinct values for Signature and return
   * a new RDD with the signature of type String and the number of times that
   * the signature occurred in the partition
   */
  def distinctPartitionHashMap(statesHashSignature: RDD[(StateId, Signature)]): RDD[(String, Int)] = {
    //for each partition
    val rddWithDistinctValues = statesHashSignature.mapPartitions(values => {
      //initialize the hashMap for the signatures
      val hashSignatureStored = mutable.HashMap.empty[String, Int]
      //for each value in the partition
      values.foreach(value => {
        //create String Signature from Array[Byte] Signature
        val stringValue = value._2.map(_.toChar).mkString
        //get the number of occurrences for the String Signature
        //and initialize to zero if it is not present
        val hashValue = hashSignatureStored.getOrElse(stringValue, 0)
        //increment the number of occurrences
        hashSignatureStored.update(stringValue, hashValue + 1)
      })
      hashSignatureStored.toIterator
    })
    rddWithDistinctValues
  }

  /**
   * Given an RDD (signatureString, numberOfOccurrences) counts
   * the number of different signatureString
   */
  def countBlock(signatureNumberOcc: RDD[(String, Int)]): Long =
    signatureNumberOcc.count()

  /**
   * Given an RDD of (StateId, Signature) coalesces the partitions
   * and return a new RDD with the signature of type String and the number of times that
   * the signature is present in stateSignatureRDD
   */
  def countBlockOccurrences(stateSignatureRDD: StateSignatureRDD,
                            numberPartitionCoalesced: Int): RDD[(String, Int)] = {
    //repartition in a lesser number of partition
    val stateSignatureCoalescedRDD = stateSignatureRDD.coalesce(numberPartitionCoalesced)
    //count number of Signature in each partition
    val blockCountOnPartition = distinctPartitionHashMap(stateSignatureCoalescedRDD)
    blockCountOnPartition.reduceByKey(_ + _)
  }
}
