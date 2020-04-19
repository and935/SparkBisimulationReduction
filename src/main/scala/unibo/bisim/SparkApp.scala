package unibo.bisim

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * Common trait for bootstrapping a Spark app.
 */
trait SparkApp extends KryoSerialization {
  def run(args: Array[String], spark: SparkSession): Unit

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.set("spark.executor.extraJavaOptions", "-XX:+UseG1GC")
    useKryo(conf)

    val spark = SparkSession
      .builder
      .appName("Spark Bisimulation Reduction")
      .config(conf)
      .getOrCreate()

    setupLogging(spark)

    run(args, spark)
    spark.stop()
  }

  def setupLogging(spark: SparkSession): Unit =
    spark.sparkContext.setLogLevel("WARN")
}
