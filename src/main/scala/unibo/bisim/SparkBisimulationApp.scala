package unibo.bisim

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.kohsuke.args4j.{CmdLineParser, Option => ArgOption}


object SparkBisimulationApp extends SparkApp {

  class Options {
    @ArgOption(name = "--input", usage = "Input file AUT containing the lts", required = true)
    var input: String = _

    @ArgOption(name = "--output", usage = "Output file for the lts reduced", required = true)
    var output: String = _

    @ArgOption(name = "--reduce", usage = "Flag for reduce the entire lts in an AUT file", required = false)
    var reduce: Boolean = false

    @ArgOption(name = "--num-partition", usage = "number of partition for transition RDD", required = true)
    var numPartition: Int = 0

    @ArgOption(name = "--num-partitionCoalesce", usage = "number of partition for stateSignatureRDD", required = false)
    var numPartitionCoalesced: Int = numPartition/2
  }

  def run(args: Array[String], spark: SparkSession): Unit = {

    val options = new Options()
    new CmdLineParser(options).parseArgument(args: _*)

    val sc = spark.sparkContext

    val lts = LtsBuilder.fromFile(
      sc,
      options.input,
      options.numPartition
    )

    val (keyDestTransition, stateSignatureReducedRDD) = Bisimulation.run(
      lts,
      StorageLevel.MEMORY_AND_DISK_SER,
      StorageLevel.MEMORY_AND_DISK_SER,
      options.numPartition,
      options.numPartitionCoalesced
    )

    if (options.reduce) {
      FileUtils.deleteDirectory(new File(options.output))

      Lts.saveReducedLts(
        spark.sparkContext,
        keyDestTransition.values,
        stateSignatureReducedRDD,
        lts.label,
        options.numPartitionCoalesced,
        options.output
      )
    } else {
      keyDestTransition.unpersist()
      val pathOut = s"${options.output}__ArrayBlock"

      FileUtils.deleteDirectory(new File(pathOut))
      Lts.saveBlockStates(
        spark.sparkContext,
        stateSignatureReducedRDD,
        options.numPartitionCoalesced,
        pathOut
      )
    }
  }
}
