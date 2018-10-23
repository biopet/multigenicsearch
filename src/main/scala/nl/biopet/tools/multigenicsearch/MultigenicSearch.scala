/*
 * Copyright (c) 2018 Biopet
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of
 * this software and associated documentation files (the "Software"), to deal in
 * the Software without restriction, including without limitation the rights to
 * use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
 * the Software, and to permit persons to whom the Software is furnished to do so,
 * subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
 * FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 * COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
 * IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package nl.biopet.tools.multigenicsearch

import java.io.File

import htsjdk.variant.vcf.VCFFileReader
import nl.biopet.utils.ngs.intervals.{BedRecord, BedRecordList}
import nl.biopet.utils.tool.ToolCommand
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{Dataset, SparkSession, types}
import org.apache.spark.sql.functions._

import scala.collection.JavaConversions._

object MultigenicSearch extends ToolCommand[Args] {
  def emptyArgs = Args()
  def argsParser = new ArgsParser(this)

  override def urlToolName: String = "tool-template"
  def main(args: Array[String]): Unit = {
    val cmdArgs = cmdArrayToArgs(args)

    logger.info("Start")

    implicit val spark: SparkSession = SparkSession.builder().getOrCreate()
    implicit val sc: SparkContext = spark.sparkContext
    import spark.implicits._

    val reader = new VCFFileReader(cmdArgs.inputFile, true)
    val header = reader.getFileHeader
    reader.close()
    val sampleNumber = header.getNGenotypeSamples

    val scatterRegions = sc.parallelize(
      BedRecordList.fromReference(cmdArgs.reference).scatter(1000000))
    val variants = readVcf(scatterRegions, cmdArgs.inputFile, sampleNumber)

    val indexOnly = variants
      .select("index", "variant.samples")
      .withColumnRenamed("samples", "variantSamples")
      .as[SingleSamples]

    val duoCombinations = initCombinations(indexOnly)
    val triCombinations = addCombination(indexOnly, duoCombinations)
    val quatCombinations = addCombination(indexOnly, triCombinations)
    val fiveCombinations = addCombination(indexOnly, quatCombinations)
    val sixCombinations = addCombination(indexOnly, fiveCombinations)
    val sevenCombinations = addCombination(indexOnly, sixCombinations)

    val bla2 = duoCombinations.collect()
    val bla3 = triCombinations.collect()
    val bla4 = quatCombinations.collect()
    val bla5 = fiveCombinations.collect()
    val bla6 = sixCombinations.collect()
    val bla7 = sevenCombinations.collect()

    spark.stop()
    logger.info("Done")
  }

  def readVcf(scatterRegions: RDD[List[BedRecord]],
              inputFile: File,
              sampleNumber: Int)(
      implicit spark: SparkSession): Dataset[SingleVariantWithIndex] = {
    import spark.implicits._
    scatterRegions
      .flatMap(x => x)
      .mapPartitions { it =>
        val reader = new VCFFileReader(inputFile, true)

        it.flatMap(r => reader.query(r.chr, r.start + 1, r.end))
          .map(r =>
            SingleVariant(r.getContig, r.getStart, (0 until sampleNumber).map {
              g =>
                val genotype = r.getGenotype(g)
                genotype.getAlleles.exists(a => a.isNonReference && a.isCalled)
            }.toList))
      }
      .zipWithUniqueId()
      .map {
        case (variant, idx) =>
          SingleVariantWithIndex(idx, variant)
      }
      .toDS()
  }

  def initCombinations(variants: Dataset[SingleSamples])(
      implicit spark: SparkSession): Dataset[Combination] = {
    import spark.implicits._

    val singleSamples1 = variants
      .withColumnRenamed("index", "index1")
      .withColumnRenamed("variantSamples", "samples1")
      .as("singleSamples1")
    val singleSamples2 = variants
      .withColumnRenamed("index", "index2")
      .withColumnRenamed("variantSamples", "samples2")
      .as("singleSamples2")

    singleSamples1
      .join(singleSamples2, $"index1" < $"index2")
      .withColumn("indexes", array($"index1", $"index2"))
      .withColumn("samples", array($"samples1", $"samples2"))
      .select("indexes", "samples")
      .as[Combination]
  }

  def addCombination(variants: Dataset[SingleSamples],
                     current: Dataset[Combination])(
      implicit spark: SparkSession): Dataset[Combination] = {
    import spark.implicits._

    val addIndex: UserDefinedFunction = udf(
      (array: Seq[Long], value: Long) => array :+ value)
    val addSamples: UserDefinedFunction = udf(
      (array: Seq[Seq[Boolean]], value: Seq[Boolean]) => array :+ value)

    current
      .join(variants, sort_array($"indexes")(size($"indexes") - 1) < $"index")
      .withColumnRenamed("indexes", "indexes_old")
      .withColumnRenamed("samples", "samples_old")
      .withColumn("indexes", addIndex($"indexes_old", $"index"))
      .withColumn("samples", addSamples($"samples_old", $"variantSamples"))
      .select("indexes", "samples")
      .as[Combination]
  }

  def descriptionText: String =
    """
      |
    """.stripMargin

  def manualText: String =
    """
      |
    """.stripMargin

  def exampleText: String =
    """
      |
    """.stripMargin
}
