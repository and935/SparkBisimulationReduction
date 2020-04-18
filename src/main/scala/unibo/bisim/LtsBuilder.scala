package unibo.bisim

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * Builds an Lts from the transitions contained in an AUT file format (src, label, dst),
 * saving the resulting lts (transitions, label) in an LtsBisimulation instance
 * in preparation for next steps.
 */
object LtsBuilder {

  /**
   * Parse the number of transitions contained in the String
   * "des (_, numberTransitions, _)", the first line in an AUT file.
   */
  private def parseNumberTransitions(str: String): Int = str
    .split(",")(1)
    .toInt

  /**
   * Parse the number of states contained in the String
   * "des (_, _, numberStates)", the first line in an AUT file.
   */
  private def parseNumberStates(string: String): Int = string
    .split(",")(2)
    .split("\\)")(0)
    .toInt

  /**
   * Parse a transition from a String in the form "(src, "label", dst)"
   * and returns a triple (src, label, dst) that represents the transition.
   */
  private def parseTransition(string: String): Transition = {
    val Array(src, lab, dst) = string
      .substring(1, string.length - 1) //remove the parenthesis at the begin and at the end of the String
      .split("(,\"|\",)") //split the string when encounters the sequences ' ", '
    (
      src.toInt,
      lab,
      dst.toInt
    )
  }

  /**
   * Given an RDD of transitions (src, labelString, dst) and an RDD of (label, IdLabel)
   * replace each labelString with idLabel and return a new RDD (src, IdLabel, dst)
   */
  def compressTransition(context: SparkContext,
                         transitionRDD: TransitionRDD,
                         labelRDD: RDD[(String, Long)]): TransitionCompressedRDD = {
    //broadcast to each node the RDD label
    val labelBroadCast = context.broadcast(labelRDD.collectAsMap())
    //replace each labelString with idLabel
    val compressedTransitionRDD = Lts.compressTransitionsFromMap(transitionRDD, labelBroadCast.value)
    labelBroadCast.unpersist()
    compressedTransitionRDD
  }

  /**
   * Process each transition line
   */
  def processTextRdd(textRdd: RDD[String]): TransitionRDD = {
    textRdd.map(parseTransition)
  }

  /**
   * Given a number of states build an RDD(state, Signature)
   * from the state -1 until numberState with the initial signature
   */
  def buildStatesWithInitialSignature(context: SparkContext,
                                      numberStates: Int): StateSignatureRDD = {
    val stateRDD = context.parallelize(-1 until numberStates)
    Lts.initializeStateSignature(stateRDD)
  }

  /**
   * Given an RDD of compressed transition add a transition (x, -1, -1)
   * for each x in deadLockRDD and return the new RDD of compressed transition
   */
  def addTransitionForDeadLock(compressedTransitionRDD: TransitionCompressedRDD,
                               deadLockRDD: RDD[StateId]): TransitionCompressedRDD = {
    compressedTransitionRDD.union(deadLockRDD.map(x => (x, -1, -1)))
  }

  /**
   * Given a sparkContext and a name of AUT file, process the text and
   * returns the correspondent LtsBisimulation
   */
  def fromFile(sparkContext: SparkContext, fileName: String, numPartition: Int): LtsBisimulation = {

    var numPar = numPartition
    //RDD of Strings for the file AUT
    val textRdd: RDD[String] = sparkContext.textFile(fileName, numPartition)
    //first line "des(initialState, numberTransitions, numberStates)"
    val des = textRdd.first()
    //number of states
    val numberStates = parseNumberStates(des)

    //number of transitions
    val numberTransitions = parseNumberTransitions(des)
    //all lines except the first
    val textTransitions = textRdd.filter(!_.contains("des"))
    //transitions RDD(src, label: String, dst)
    val transitionRDD = processTextRdd(textTransitions)
    //distinct label with unique Long associated RDD[(String, Long)]
    val labelRDD = Lts.processLabel(transitionRDD)
    //transitions RDD(src, idLabel : Long, dest)
    var compressedTransitionRDD = compressTransition(sparkContext, transitionRDD, labelRDD)
    //states with initial signature [md5(ArrayBuffer.empty)] RDD(state, initialSignature)
    val stateSignatureRDD = buildStatesWithInitialSignature(sparkContext, numberStates)
    //states deadlock
    val deadLockRDD = Lts.findDeadLocks(compressedTransitionRDD, stateSignatureRDD)
    //transition compressed with transition for deadlock states
    compressedTransitionRDD = addTransitionForDeadLock(compressedTransitionRDD, deadLockRDD)
    //creates the lts
    LtsBisimulation(compressedTransitionRDD,
      stateSignatureRDD,
      labelRDD,
      numberTransitions,
      numberStates)
  }
}
