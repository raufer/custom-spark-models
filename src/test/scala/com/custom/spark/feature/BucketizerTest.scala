package com.custom.spark.feature

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.ml.linalg.{SparseVector, Vector, Vectors}
import org.scalatest.FlatSpec
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row



class BucketizerTest extends FlatSpec {

  val spark = SparkSession
    .builder()
    .appName("TestApp")
    .config("spark.master", "local")
    .getOrCreate()

  import spark.implicits._

  "Bucketizer.fit" should "return a model with the right parameters fit" in {

    val df = Seq(
      (1.0, 0.0),
      (5.0, 2.0),
      (0.0, 0.0),
      (7.0, 2.0),
      (4.0, 1.0),
      (8.0, 3.0),
      (10.0, 3.0)
    ).toDF("input", "expected")

    val nBins = 4

    val bucketizer = new Bucketizer()
      .setInputCol("input")
      .setOutputCol("bin")
      .setNumberBins(nBins)

    val model = bucketizer.fit(df)

    val targetBins = Array((0.0, 0), (2.5, 1), (5.0, 2), (7.5, 3), (10.0, 4))

    assert(model.bins.toArray.sameElements(targetBins))

  }

  "Bucketizer.transform" should "transform the input column to the right bins" in {

    val df = Seq(
      (1.0, 0),
      (5.0, 2),
      (0.0, 0),
      (7.0, 2),
      (4.0, 1),
      (8.0, 3),
      (10.0, 4)
    ).toDF("input", "expected")

    val nBins = 4

    val bucketizer = new Bucketizer()
      .setInputCol("input")
      .setOutputCol("bin")
      .setNumberBins(nBins)

    val model = bucketizer.fit(df)

    model.transform(df).select("bin", "expected").collect().foreach {
      case Row(bin: Int, expected: Int) =>
        assert(bin == expected)
    }
  }

  "BucketizerModel" should "allow for model persistence" in {

    val df = Seq(
      (1.0, 0),
      (5.0, 2),
      (0.0, 0),
      (7.0, 2),
      (4.0, 1),
      (8.0, 3),
      (10.0, 4)
    ).toDF("input", "expected")

    val nBins = 4

    val bucketizer = new Bucketizer()
      .setInputCol("input")
      .setOutputCol("bin")
      .setNumberBins(nBins)

    val model = bucketizer.fit(df)

    model.save(".temp/")

    val loadedModel = BucketizerModel.load(".temp/")

    assert(model.bins.sameElements(loadedModel.bins))

    val features = model.transform(df).select("bin").collect()
    val featuresAfter = loadedModel.transform(df).select("bin").collect()

    features.zip(featuresAfter).foreach {
      case (Row(bin: Int),  Row(binAfter: Int)) =>
        assert(bin == binAfter)
    }

    FileUtils.deleteQuietly(new File(".temp/"))

  }
}
