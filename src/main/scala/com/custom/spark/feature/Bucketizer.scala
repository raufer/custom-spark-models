package com.custom.spark.feature

import java.lang.{Double => JDouble, Integer => JInt}
import java.util.{NoSuchElementException, Map => JMap}

import scala.collection.JavaConverters._
import com.custom.spark.persistence
import com.custom.spark
import org.apache.hadoop.fs.Path
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.param._
import org.apache.spark.ml.util._
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.collection.immutable.SortedMap



/**
  * Params for [[Bucketizer]] and [[BucketizerModel]].
  */
trait BucketizerParams extends Params {

  val inputCol= new Param[String](this, "inputCol", "The input column")

  val outputCol = new Param[String](this, "outputCol", "The output column")

  val numberBins: IntParam = new IntParam(this, "numberBins", "Number of fixed bins to divide the continuous range", ParamValidators.gt(0))

  def getNBins: Int = $(numberBins)


  /** Validates and transforms the input schema. */
  protected def validateAndTransformSchema(schema: StructType): StructType = {
    require(isDefined(inputCol), s"Bucketizer requires input column parameter: $inputCol")
    require(isDefined(outputCol), s"Bucketizer requires output column parameter: $outputCol")

    val field = schema.fields(schema.fieldIndex($(inputCol)))

    if (field.dataType!= DoubleType) {
      throw new Exception(
        s"Input type ${field.dataType} did not match input type DoubleType")
    }

    // Add the return field
    schema.add(StructField($(outputCol), IntegerType, false))
  }
}

class Bucketizer(override val uid: String)
  extends Estimator[BucketizerModel] with BucketizerParams {

  def this() = this(Identifiable.randomUID("Bucketizer"))

  def setInputCol(value: String) = set(inputCol, value)

  def setOutputCol(value: String) = set(outputCol, value)

  def setNumberBins(value: Int): this.type = set(numberBins, value)

  private def minMax(a: Array[Double]): (Double, Double) = {
    a.foldLeft((a(0), a(0))){case (acc, x) => (math.min(acc._1, x), math.max(acc._2, x))}
  }

  override def copy(extra: ParamMap): Bucketizer = {
    defaultCopy(extra)
  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  override def fit(dataset: Dataset[_]): BucketizerModel = {
    transformSchema(dataset.schema, logging = true)

    val numBins = $(numberBins)

    val input = dataset.select($(inputCol)).rdd.map(_.getAs[Double](0)).collect()

    val (min, max) = minMax(input)
    val bins = SortedMap[Double, Int]((min to max by ((max - min) / numBins)).zipWithIndex:_*)

    val model = new BucketizerModel(uid, bins)
    copyValues(model)
  }
}


class BucketizerModel(override val uid: String, val bins: SortedMap[Double, Int])
  extends Model[BucketizerModel] with BucketizerParams with MLWritable {

  import BucketizerModel._

  /** Java-friendly version of [[bins]] */
  def javaBins: JMap[JDouble, JInt] = {
    bins.map{ case (k, v) => double2Double(k) -> int2Integer(v) }.asJava
  }

  /** Returns the corresponding bin on which the input falls */
  val getBin = (a: Double, bins: SortedMap[Double, Int]) => bins.to(a).last._2

  override def copy(extra: ParamMap): BucketizerModel = {
    defaultCopy(extra)
  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  private var broadcastBins: Option[Broadcast[SortedMap[Double, Int]]] = None

  override def transform(dataset: Dataset[_]): DataFrame = {
    transformSchema(dataset.schema, logging = true)
    if (broadcastBins.isEmpty) {
      val dict = bins
      broadcastBins = Some(dataset.sparkSession.sparkContext.broadcast(dict))
    }

    val binsBr = broadcastBins.get

    val vectorizer = udf { (input: Double) => getBin(input, binsBr.value)}

    dataset.withColumn($(outputCol), vectorizer(col($(inputCol))))
  }

  override def write: MLWriter = new BucketizerModelWriter(this)
}


object BucketizerModel extends MLReadable[BucketizerModel] {

  class BucketizerModelWriter(instance: BucketizerModel) extends MLWriter {

    private case class Data(bins: Map[Double, Int])

    override protected def saveImpl(path: String): Unit = {
      spark.persistence.DefaultParamsWriter.saveMetadata(instance, path, sc)
      val data = Data(instance.bins)
      val dataPath = new Path(path, "data").toString
      sparkSession.createDataFrame(Seq(data)).repartition(1).write.parquet(dataPath)
    }
  }

  private class BucketizerModelReader extends MLReader[BucketizerModel] {

    private val className = classOf[BucketizerModel].getName

    override def load(path: String): BucketizerModel = {
      val metadata = spark.persistence.DefaultParamsReader.loadMetadata(path, sc, className)
      val dataPath = new Path(path, "data").toString
      val data = sparkSession.read.parquet(dataPath)
        .select("bins")
        .head()

      val bins = SortedMap(data.getAs[Seq[(Double, Int)]](0).toMap.toArray:_*)

      val model = new BucketizerModel(metadata.uid, bins)
      spark.persistence.DefaultParamsReader.getAndSetParams(model, metadata)
      model
    }
  }

  override def read: MLReader[BucketizerModel] = new BucketizerModelReader

  override def load(path: String): BucketizerModel = super.load(path)
}
