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
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.JavaConversions._
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

object MultigenicSearch extends ToolCommand[Args] {
  def emptyArgs = Args()
  def argsParser = new ArgsParser(this)

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

    val regions = (cmdArgs.regions match {
      case Some(i) =>
        BedRecordList.fromFile(i).validateContigs(cmdArgs.reference)
      case _ => BedRecordList.fromReference(cmdArgs.reference)
    }).combineOverlap
      .scatter(cmdArgs.binSize)

    val regionsRdd = sc.parallelize(regions, regions.size)
    val variants = readVcf(regionsRdd, cmdArgs.inputFile, sampleNumber)

    val indexOnly = variants
      .select("index", "variant.samples")
      .withColumnRenamed("samples", "variantSamples")
      .as[SingleSamples]

    val maxAllowedNonVariantSites: Map[Int, Int] =
      (2 to cmdArgs.maxCombinationSize)
        .map(i => i -> (i - (i * cmdArgs.multigenicFraction).ceil.toInt))
        .toMap
    val maxNonVariants = maxAllowedNonVariantSites.values.max

    val digenic = filterMaxNonVariants(initCombinations(indexOnly),
                                       maxNonVariants,
                                       sampleNumber,
                                       cmdArgs.sampleFraction)

    val combinations = (3 to cmdArgs.maxCombinationSize)
      .foldLeft(Map(2 -> digenic)) { (a, multigenicSize) =>
        val c = addCombination(indexOnly, a(multigenicSize - 1))
        a + (multigenicSize -> filterMaxNonVariants(c,
                                                    maxNonVariants,
                                                    sampleNumber,
                                                    cmdArgs.sampleFraction))
      }

    val futures = (2 to cmdArgs.maxCombinationSize).map { i =>
      writeResult(variants,
                  combinations(i),
                  maxAllowedNonVariantSites(i),
                  sampleNumber,
                  cmdArgs.sampleFraction,
                  new File(cmdArgs.outputDir, s"multigenic_$i"))
    }

    Await.result(Future.sequence(futures), Duration.Inf)

    spark.stop()
    logger.info("Done")
  }

  /**
    * This method will filter combination has to much non-variants
    * @param combinations Input dataset
    * @param maxNonVariants Max non-variants per sample
    * @param sampleNumber Total number of samples
    * @param sampleFraction Fraction of samples that should pass the test
    * @return Filtered dataset
    */
  def filterMaxNonVariants(combinations: Dataset[Combination],
                           maxNonVariants: Int,
                           sampleNumber: Int,
                           sampleFraction: Double): Dataset[Combination] = {
    require(maxNonVariants >= 0)
    require(sampleNumber >= 1)
    require(sampleFraction >= 0.0 && sampleFraction <= 1.0)
    combinations.filter { x =>
      val nonVariants =
        (0 until sampleNumber).map(s => x.samples.map(_(s)).count(_ == false))
      val nonVaraintsCount = nonVariants.count(_ <= maxNonVariants)
      nonVaraintsCount.toDouble / sampleNumber.toDouble >= sampleFraction
    }
  }

  /**
    * This method will read a vcf file in partitions
    * @param scatterRegions Regions to read
    * @param inputFile Input vcf file
    * @param sampleNumber Total number of samples
    * @param spark implicit spark session
    * @return Output dataset
    */
  def readVcf(scatterRegions: RDD[List[BedRecord]],
              inputFile: File,
              sampleNumber: Int)(
      implicit spark: SparkSession): Dataset[SingleVariantWithIndex] = {
    require(sampleNumber >= 1)

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

  /**
    * This method will make the first combinations
    * @param variants Input dataset
    * @param spark Implicit spark session
    * @return Output dataset
    */
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

  /**
    *
    * @param variants Variants to add
    * @param current Current combinations
    * @param spark Implicit spark session
    * @return Output dataset
    */
  def addCombination(variants: Dataset[SingleSamples],
                     current: Dataset[Combination])(
      implicit spark: SparkSession): Dataset[Combination] = {
    import spark.implicits._

    val addIndex: UserDefinedFunction = udf(
      (array: Seq[Long], value: Long) => array :+ value)
    val addSamples: UserDefinedFunction = udf(
      (array: Seq[Seq[Boolean]], value: Seq[Boolean]) => array :+ value)

    current
      .join(variants, $"indexes" (size($"indexes") - 1) < $"index")
      .withColumnRenamed("indexes", "indexes_old")
      .withColumnRenamed("samples", "samples_old")
      .withColumn("indexes", addIndex($"indexes_old", $"index"))
      .withColumn("samples", addSamples($"samples_old", $"variantSamples"))
      .select("indexes", "samples")
      .as[Combination]
  }

  /**
    * Writing result as partitioned json file
    * @param variants All variants
    * @param combinations Combinations to write
    * @param maxNonVariants Max allowed non-variant calls per sample
    * @param sampleNumber Tot number of samples
    * @param sampleFraction Fraction of samples that should pass
    * @param outputDir Output dir to write files, should not yet exist
    * @return
    */
  def writeResult(variants: Dataset[SingleVariantWithIndex],
                  combinations: Dataset[Combination],
                  maxNonVariants: Int,
                  sampleNumber: Int,
                  sampleFraction: Double,
                  outputDir: File): Future[Unit] = {
    val filterCombinations = filterMaxNonVariants(combinations,
                                                  maxNonVariants,
                                                  sampleNumber,
                                                  sampleFraction)

    Future {
      filterCombinations.write.json(outputDir.getAbsolutePath)
    }
  }

  def descriptionText: String =
    """
      |This tool will try to find multigenic events with different sizes.
      |It's not required for all samples to have all variants in a multigenic event.
    """.stripMargin

  def manualText: String =
    """
      |The fractions are controled by multigenicFraction and sampleFraction.
      |With maxCombinationSize you can set the maximum variants per multigenic event.
      |With binsize the number of spark partitions can be controlled, the lower this number the more partitions will be created
    """.stripMargin

  def exampleText: String =
    s"""
      |Default run:
      |${example("-i",
                 "<input vcf file>",
                 "-o",
                 "<output directory>",
                 "-R",
                 "<reference fasta>")}
      |
      |Run with more settings:
      |${example(
         "-i",
         "<input vcf file>",
         "-o",
         "<output directory>",
         "-R",
         "<reference fasta>",
         "--maxCombinationSize",
         "5",
         "--multigenicFraction",
         "0.6",
         "--sampleFraction",
         "0.8"
       )}
      |
    """.stripMargin
}
