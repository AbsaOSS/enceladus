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
import org.apache.spark.sql.types._
import org.apache.spark.sql.api.java.UDF2
import org.slf4j.LoggerFactory
import org.apache.spark.storage.StorageLevel
import za.co.absa.enceladus.utils.implicits.OptionImplicits.OptionEnhancements
import za.co.absa.enceladus.utils.udf.UDFNames
import za.co.absa.spark.commons.implicits.StructTypeImplicits.StructTypeEnhancementsArrays
import za.co.absa.spark.commons.sql.functions.col_of_path
import za.co.absa.spark.commons.utils.SchemaUtils

object ArrayTransformations {

  private implicit class TransformationsChaining(val ds: DataFrame) extends AnyVal{
    def nestedWithColumn(columnName: String, column: Column): DataFrame = {
      ArrayTransformations.nestedWithColumn(ds)(columnName: String, column: Column)
    }
    def nestedWithColumnConditionally(condition: Boolean)(columnName: String): DataFrame = {
      if (condition) {
        val arrayCol = col(s"`$columnName`")
        nestedWithColumn(columnName, arrayCol).drop(arrayCol)
      } else {
        ds
      }
    }

    def zipWithOrder(arrCol: String)(implicit spark: SparkSession): DataFrame = {
      ArrayTransformations.zipWithOrder(ds, arrCol)
    }
  }

  private val logger = LoggerFactory.getLogger(this.getClass)
  private[enceladus] val arraySizeCols = new scala.collection.mutable.HashMap[String, String]()

  private val zipWithOrderUDF1 = new UDF1[Seq[Row], Seq[(Int, Row)]] {
    override def call(t1: Seq[Row]): Seq[(Int, Row)] = {
      // scalastyle:off null
      if (t1 == null) {
        null
      } else {
        t1.zipWithIndex.map(_.swap)
      }
      // scalastyle:on null
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

  def nestedWithColumn(ds: DataFrame)(columnName: String, column: Column): DataFrame = {
    val toks = SchemaUtils.splitPath(columnName)

    def helper(tokens: List[String], pathAcc: Seq[String]): Column = {
      val currPath = (pathAcc :+ tokens.head).mkString(".")
      val topType = ds.schema.getFieldType(currPath)

      // got a match
      if (currPath == columnName) {
        column as tokens.head
      } // some other attribute
      else if (!columnName.startsWith(currPath)) {
        col_of_path(currPath)
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
          case _: DataType  => col_of_path(currPath) as tokens.head
        }
      }
    }

    ds.withColumn(toks.head, helper(toks, Seq()))
  }

  private def getArraySchema(field: String, schema: StructType): ArrayType = {
    val arrType = schema.getFieldType(field)
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
    nestedWithColumn(ds)(arrCol, expr(s"$udfName($arrCol)"))
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

    val groupField = arrayCol.replace(".", "_") + "_arrayTransformId"
    val indField = s"${groupField}_index"
    val sizeField = s"${groupField}_size"

    logger.info(s"ArrayTransform: Storing array size col for $arrayCol as $sizeField")
    this.arraySizeCols.put(arrayCol, sizeField)

    val tokens = SchemaUtils.splitPath(arrayCol)

    val dsWithId = ds.withColumn(groupField, monotonically_increasing_id())
                     .withColumn(sizeField, size(col(arrayCol))).persist(StorageLevel.MEMORY_ONLY_SER) //id
    val flattened = dsWithId.zipWithOrder(arrayCol) //zipped
                            .withColumn(arrayCol, explode_outer(col_of_path(arrayCol))) //exploded
                            .nestedWithColumnConditionally(tokens.length > 1)(arrayCol)
                            .nestedWithColumn(indField, col_of_path(s"$arrayCol._1"))
                            .nestedWithColumn(arrayCol, col_of_path(s"$arrayCol._2"))
    val transformed = fn(flattened)
    val joinDs = transformed.nestedWithColumn(arrayCol, struct(col_of_path(indField) as "_1", col_of_path(arrayCol) as "_2"))
                            .drop(indField) //withInd
                            .groupBy(col_of_path(groupField)).agg(collect_list(col(arrayCol)) as arrayCol) //list
                            .nestedWithColumnConditionally(tokens.length > 1)(arrayCol)

    val origArraySchema = ds.schema
                            .getFieldType(arrayCol)
                            .getOrThrow(new IllegalStateException(s"The field $arrayCol not found in the transformed schema."))
                            .asInstanceOf[ArrayType]
    val arrayChildSchema = transformed.schema
                                      .getFieldType(arrayCol)
                                      .getOrThrow(new IllegalStateException(s"The field $arrayCol not found in the transformed schema."))
    val arraySchema = ArrayType.apply(arrayChildSchema, origArraySchema.containsNull)

    val udfName = UDFNames.uniqueUDFName("handleNullAndEmpty", groupField)

    spark.udf.register(udfName, handleNullAndEmptyUDF2, arraySchema)

    dsWithId.as("orig")
            .join(joinDs.as("trns"), col(s"orig.$groupField") === col(s"trns.$groupField"))
            .select($"orig.*", col(s"trns.${tokens.head}") as s"${tokens.head}_RENAME_TEMP")
            .nestedWithColumn(arrayCol, col_of_path(s"${tokens.head}_RENAME_TEMP.${tokens.tail.mkString(".")}"))
            .drop(s"${tokens.head}_RENAME_TEMP")
            .nestedWithColumn(arrayCol, sort_array(col_of_path(arrayCol)))
            .nestedWithColumn(arrayCol, col_of_path(s"$arrayCol._2"))
            .nestedWithColumn(arrayCol, expr(s"$udfName($sizeField, $arrayCol)")).drop(sizeField).drop(col_of_path(groupField))
  }

  def nestedDrop(df: DataFrame, colName: String): DataFrame = {
    val toks = SchemaUtils.splitPath(colName)
    if (toks.size == 1) df.drop(colName) else {
      if (df.schema.getFirstArrayPath(colName) != "") {
        throw new IllegalStateException(s"Array Type fields in the path of $colName - dropping arrays children is not supported")
      }
      val parentPath = toks.init.mkString(".")
      logger.info(s"Nested Drop: parent path $parentPath")
      val parentType = df.schema.getFieldType(parentPath)
      logger.info(s"Nested Drop: parent type $parentType")
      val parentCols = parentType
        .getOrThrow{new IllegalStateException(s"Field $colName does not exist in \n ${df.printSchema()}")}
        .asInstanceOf[StructType]
        .fields
      val replace = struct(parentCols.filter(_.name != toks.last).map(x => col_of_path(s"$parentPath.${x.name}") as x.name): _*)
      nestedWithColumn(df)(parentPath, replace)
    }
  }

  def flattenArrays(df: DataFrame, colName: String)(implicit spark: SparkSession): DataFrame = {
    val dataType = df.schema.getFieldType(colName)
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

        nestedWithColumn(df)(colName, call_udf(udfName, col(colName)))
      }
    }

  }

  def handleArrays(targetColumn: String, df: DataFrame)(fn: DataFrame => DataFrame)(implicit spark: SparkSession): DataFrame = {
    logger.info(s"handleArrays: Finding first array for $targetColumn")
    val firstArr = df.schema.getFirstArrayPath(targetColumn)
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
