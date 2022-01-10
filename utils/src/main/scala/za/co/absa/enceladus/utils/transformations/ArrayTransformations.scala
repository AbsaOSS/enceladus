/*
 * Copyright 2018 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package za.co.absa.enceladus.utils.transformations

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.api.java.UDF1
import za.co.absa.enceladus.utils.schema.SchemaUtils
import org.apache.spark.sql.types._
import org.apache.spark.sql.api.java.UDF2
import org.slf4j.LoggerFactory
import org.apache.spark.storage.StorageLevel
import za.co.absa.enceladus.utils.implicits.OptionImplicits.OptionEnhancements
import za.co.absa.enceladus.utils.udf.UDFNames

object ArrayTransformations {

  private val logger = LoggerFactory.getLogger(this.getClass)
  private[enceladus] val arraySizeCols = new scala.collection.mutable.HashMap[String, String]()

  private val zipWithOrderUDF1 = new UDF1[Seq[Row], Option[Seq[(Int, Row)]]] {
    override def call(colValue: Seq[Row]): Option[Seq[(Int, Row)]] = {
      Option(colValue).map(_.zipWithIndex.map(_.swap))
    }
  }

  private val handleNullAndEmptyUDF2 = new UDF2[Int, Seq[Row], Option[Seq[Row]]] {
    override def call(size: Int, arrayColValue: Seq[Row]): Option[Seq[Row]] = {
      size match {
        case -1 => None
        case  0 => Option(Seq())
        case  _ => Option(arrayColValue)
      }
    }
  }

  //array safe col
  def arrCol(any: String): Column = {
    val toks = any.replaceAll("\\[(\\d+)]", "\\.$1").split('.')
    toks.tail.foldLeft(col(toks.head)){
      case (acc, tok) =>
        if (tok.matches("\\d+")) {
          acc(tok.toInt)
        } else {
          acc(tok)
        }
    }
  }

  def nestedWithColumn(ds: DataFrame)(columnName: String, column: Column): DataFrame = {
    val toks = columnName.split("\\.").toList

    def helper(tokens: List[String], pathAcc: Seq[String]): Column = {
      val currPath = (pathAcc :+ tokens.head).mkString(".")
      val topType = SchemaUtils.getFieldType(currPath, ds.schema)

      // got a match
      if (currPath == columnName) {
        column as tokens.head
      } // some other attribute
      else if (!columnName.startsWith(currPath)) {
        arrCol(currPath)
      } // partial match, keep going
      else if (topType.isEmpty) {
        struct(helper(tokens.tail, pathAcc ++ List(tokens.head))) as tokens.head
      } else {
        topType.get match {
          case s: StructType =>
            val cols = s.fields.map(_.name)
            val fields = if (tokens.size > 1 && !cols.contains(tokens(1))) {
              cols :+ tokens(1)
            } else {
              cols
            }
            struct(fields.map(field => helper((List(field) ++ tokens.tail).distinct, pathAcc :+ tokens.head) as field): _*) as tokens.head
          case _: ArrayType => throw new IllegalStateException("Cannot reconstruct array columns. Please use this within arrayTransform.")
          case _: DataType  => arrCol(currPath) as tokens.head
        }
      }
    }

    ds.withColumn(toks.head, helper(toks, Seq()))
  }

  private def getArraySchema(field: String, schema: StructType): ArrayType = {
    val arrType = SchemaUtils.getFieldType(field, schema)
    if (arrType.isEmpty || !arrType.get.isInstanceOf[ArrayType]) {
      throw new IllegalStateException(s"Column $field either does not exist or is not of type ArrayType")
    } else {
      arrType.get.asInstanceOf[ArrayType]
    }
  }

  /**
   * Reverse zip with index for array type fields producing (index, original_element)
   *
   *  @param ds Dataset to be transformed
   *  @param arrCol The dot-separated path of the array column
   *  @return Dataset with the original path being an array of form (array_index, element)
   */
  def zipWithOrder(ds: DataFrame, arrCol: String)(implicit spark: SparkSession): DataFrame = {
    val udfName = UDFNames.uniqueUDFName("arrayZipWithIndex", arrCol)

    val elType = getArraySchema(arrCol, ds.schema)
    val newType = ArrayType.apply(StructType(Seq(
      StructField("_1", IntegerType, nullable = false), StructField("_2", elType.elementType, elType.containsNull)
    )))

    logger.info(s"Registering $udfName UDF for Array Zip With Index")
    logger.info(s"Calling UDF $udfName($arrCol))")

    spark.udf.register(udfName, zipWithOrderUDF1, newType)
    val res = nestedWithColumn(ds)(arrCol, expr(s"$udfName($arrCol)"))
    res
  }

  /**
    * Map transformation for array columns, preserving the original order of the array elements
    *
    *  @param ds Dataset to be transformed
    *  @param arrayCol The dot-separated path of the array column
    *  @param fn Function which takes a dataset and returns dataset. The array elements in the dataset have same path as the original array
    *            (one element per record). Fn has to preserve the element path.
    *  @return Dataset with the array of transformed elements (at the original path) and preserving the original order of the elements.
    */
  def arrayTransform(ds: DataFrame, arrayCol: String)(fn: DataFrame => DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    import za.co.absa.enceladus.utils.implicits.DataFrameImplicits.DataFrameEnhancements

    def colNotFoundException: Exception = new IllegalStateException(s"The field $arrayCol not found in the transformed schema.")

    val groupField = arrayCol.replace(".", "_") + "_arrayTransformId"
    val indField = s"${groupField}_index"
    val sizeField = s"${groupField}_size"

    logger.info(s"ArrayTransform: Storing array size col for $arrayCol as $sizeField")
    this.arraySizeCols.put(arrayCol, sizeField)

    val tokens = arrayCol.split('.')

    val id = ds.withColumn(groupField, monotonically_increasing_id())
               .withColumn(sizeField, size(col(arrayCol))).persist(StorageLevel.MEMORY_ONLY_SER)
    val zipped = zipWithOrder(id, arrayCol)
    val exploded = zipped.withColumn(arrayCol, explode_outer(arrCol(arrayCol)))
    val tmp = exploded.conditionally(tokens.length > 1)(nestedWithColumn(_)(arrayCol, col(s"`$arrayCol`")).drop(col(s"`$arrayCol`")))
    val flattened = nestedWithColumn(nestedWithColumn(tmp)(indField, arrCol(s"$arrayCol._1")))(arrayCol, arrCol(s"$arrayCol._2"))
    val transformed = fn(flattened)
    val withInd = nestedWithColumn(transformed)(arrayCol, struct(arrCol(indField) as "_1", arrCol(arrayCol) as "_2")).drop(indField)
    val list = withInd.groupBy(arrCol(groupField)).agg(collect_list(col(arrayCol)) as arrayCol)
    val tmp2 = list.conditionally(tokens.length > 1)(nestedWithColumn(_)(arrayCol, col(s"`$arrayCol`")).drop(col(s"`$arrayCol`")))

    val origArraySchema = SchemaUtils.getFieldType(arrayCol, ds.schema).getOrThrow(colNotFoundException).asInstanceOf[ArrayType]
    val arrayChildSchema = SchemaUtils.getFieldType(arrayCol, transformed.schema).getOrThrow(colNotFoundException)
    val arraySchema = ArrayType.apply(arrayChildSchema, origArraySchema.containsNull)

    val udfName = UDFNames.uniqueUDFName("handleNullAndEmpty", groupField)

    spark.udf.register(udfName, handleNullAndEmptyUDF2, arraySchema)


    val res1 = id
      .as("orig")
      .join(tmp2.as("trns"), col(s"orig.$groupField") === col(s"trns.$groupField"))
      .select($"orig.*", col(s"trns.${tokens.head}") as s"${tokens.head}_RENAME_TEMP")
    val res2 = nestedWithColumn(res1)(arrayCol, arrCol(s"${tokens.head}_RENAME_TEMP.${tokens.tail.mkString(".")}"))
      .drop(s"${tokens.head}_RENAME_TEMP")
    val res3 = nestedWithColumn(res2)(arrayCol, sort_array(arrCol(arrayCol)))
    val res4 = nestedWithColumn(res3)(arrayCol, arrCol(s"$arrayCol._2"))
    val res5 = nestedWithColumn(res4)(arrayCol, expr(s"$udfName($sizeField, $arrayCol)")).drop(sizeField).drop(arrCol(groupField))

    res5
  }

  def nestedDrop(df: DataFrame, colName: String): DataFrame = {
    val toks = colName.split("\\.")
    if (toks.size == 1) df.drop(colName) else {
      if (SchemaUtils.getFirstArrayPath(colName, df.schema) != "") {
        throw new IllegalStateException(s"Array Type fields in the path of $colName - dropping arrays children is not supported")
      }
      val parentPath = toks.init.mkString(".")
      logger.info(s"Nested Drop: parent path $parentPath")
      val parentType = SchemaUtils.getFieldType(parentPath, df.schema)
      logger.info(s"Nested Drop: parent type $parentType")
      val parentCols = parentType
        .getOrThrow{new IllegalStateException(s"Field $colName does not exist in \n ${df.printSchema()}")}
        .asInstanceOf[StructType]
        .fields
      val replace = struct(parentCols.filter(_.name != toks.last).map(x => arrCol(s"$parentPath.${x.name}") as x.name): _*)
      this.nestedWithColumn(df)(parentPath, replace)
    }
  }

  def flattenArrays(df: DataFrame, colName: String)(implicit spark: SparkSession): DataFrame = {
    val dataType = SchemaUtils.getFieldType(colName, df.schema)
      .getOrThrow(new Error(s"Field $colName does not exist in ${df.schema.printTreeString()}"))
    if (!dataType.isInstanceOf[ArrayType]) {
      logger.info(s"Field $colName is not an ArrayType, returning the original dataset!")
      df
    } else {
      val arrType = dataType.asInstanceOf[ArrayType]
      if (!arrType.elementType.isInstanceOf[ArrayType]) {
        logger.info(s"Field $colName is not a nested array, returning the original dataset!")
        df
      } else {
        val udfName = UDFNames.uniqueUDFName("flattenArray", colName)

        spark.udf.register(udfName, new UDF1[Seq[Seq[Row]], Option[Seq[Row]]] {
          def call(arrayColValue: Seq[Seq[Row]]): Option[Seq[Row]] = {
            Option(arrayColValue).map(array => array.filter(Option(_).isDefined).flatten)
          }
        }, arrType.elementType)

        nestedWithColumn(df)(colName, callUDF(udfName, col(colName)))
      }
    }

  }

  def handleArrays(targetColumn: String, df: DataFrame)(fn: DataFrame => DataFrame)(implicit spark: SparkSession): DataFrame = {
    logger.info(s"handleArrays: Finding first array for $targetColumn")
    val firstArr = SchemaUtils.getFirstArrayPath(targetColumn, df.schema)
    logger.info(s"handleArrays: First array field $firstArr")
    firstArr match {
      case "" => fn(df)
      case _ =>
        val res = ArrayTransformations.arrayTransform(df, firstArr) {
          dfArr => handleArrays(targetColumn, dfArr)(fn)
        }
        res
    }
  }
}
