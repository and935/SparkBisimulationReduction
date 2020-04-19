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

    @ArgOption(name = "--output", usage = "Output file AUT for the lts reduced", required = false)
    var output: String = _

    @ArgOption(name = "--num-partition", usage = "number of partition for transition RDD", required = true)
    var numPartition: Int = 0

    @ArgOption(name = "--num-partitionCoalesce", usage = "number of partition for stateSignatureRDD", required = true)
    var numPartitionCoalesced: Int = 0
  }

  def run(args: Array[String], spark: SparkSession): Unit = {

    val options = new Options()
    new CmdLineParser(options).parseArgument(args: _*)

    val lts = LtsBuilder.fromFile(
      spark.sparkContext,
      options.input,
      options.numPartition
    )

    val stateSignatureReducedRDD = Bisimulation.run(
      lts,
      StorageLevel.MEMORY_AND_DISK_SER,
      StorageLevel.MEMORY_AND_DISK_SER,
      options.numPartition,
      options.numPartitionCoalesced
    )

    if (options.output != null) {
      FileUtils.deleteDirectory(new File(options.output))
      Lts.saveReducedLts(
        spark.sparkContext,
        lts,
        stateSignatureReducedRDD,
        options.numPartitionCoalesced,
        options.output
      )
    } else {

      val pathOut = options.input + "BlockStates"

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
