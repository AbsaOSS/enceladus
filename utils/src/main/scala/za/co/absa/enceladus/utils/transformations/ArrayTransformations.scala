/*
 * Copyright 2018-2019 ABSA Group Limited
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

import scala.collection.mutable.ListBuffer

object ArrayTransformations {

  val logger = LoggerFactory.getLogger(this.getClass)
  private[enceladus] val arraySizeCols = new scala.collection.mutable.HashMap[String, String]()

  private val zipWithOrderUDF1 = new UDF1[Seq[Row], Seq[(Int, Row)]] {
    override def call(t1: Seq[Row]) = {
      if (t1 == null) null
      else t1.zipWithIndex.map(_.swap)
    }
  }

  //array safe col
  def arrCol(any: String): Column = {
    val toks = any.replaceAll("\\[(\\d+)\\]", "\\.$1").split("\\.")
    toks.tail.foldLeft(col(toks.head))({
      case (acc, tok) => {
        if (tok.matches("\\d+")) {
          acc(tok.toInt)
        } else acc(tok)
      }
    })
  }

  def nestedWithColumn(ds: Dataset[Row])(columnName: String, column: Column): Dataset[Row] = {
    val toks = columnName.split("\\.").toList

    def helper(tokens: List[String], pathAcc: Seq[String]): Column = {
      val currPath = (pathAcc :+ tokens.head).mkString(".")
      val topType = SchemaUtils.getFieldType(currPath, ds.schema)

      // got a match
      if (currPath == columnName) {
        column as tokens.head
      } // some other attribute
      else if (!columnName.startsWith(currPath)) arrCol(currPath)
      // partial match, keep going
      else {
        if (topType.isEmpty) struct(helper(tokens.tail, pathAcc ++ List(tokens.head))) as tokens.head
        else topType.get match {
          case s: StructType => {
            val cols = s.fields.map(_.name)
            val fields = if (tokens.size > 1 && !cols.contains(tokens(1))) (cols :+ tokens(1)) else cols
            struct(fields.map(field => helper((List(field) ++ tokens.tail).distinct, pathAcc :+ tokens.head) as field): _*) as tokens.head
          }
          case _: ArrayType => throw new IllegalStateException("Cannot reconstruct array columns. Please use this within arrayTransform.")
          case _: DataType  => arrCol(currPath) as tokens.head
        }
      }
    }

    ds.withColumn(toks.head, helper(toks, Seq()))
  }

  private def getArraySchema(field: String, schema: StructType): ArrayType = {
    val arrType = SchemaUtils.getFieldType(field, schema)
    if (arrType.isEmpty || !arrType.get.isInstanceOf[ArrayType]) throw new IllegalStateException(s"Column $field either does not exist or is not of type ArrayType")
    else arrType.get.asInstanceOf[ArrayType]
  }

  /**
   * Reverse zip with index for array type fields producing (index, original_element)
   *
   *  @param ds Dataset to be transformed
   *  @param arrCol The dot-separated path of the array column
   *  @return Dataset with the original path being an array of form (array_index, element)
   */
  def zipWithOrder(ds: Dataset[Row], arrCol: String)(implicit spark: SparkSession): Dataset[Row] = {
    val udfName = "arrayZipWithIndex_" + arrCol.replace(".", "_")

    val elType = getArraySchema(arrCol, ds.schema)
    val newType = ArrayType.apply(StructType(Seq(StructField("_1", IntegerType, false), StructField("_2", elType.elementType, elType.containsNull))))

    logger.info(s"Registering $udfName UDF for Array Zip With Index")
    logger.info(s"Calling UDF ${udfName}(${arrCol}))")

    val zipUdf = spark.udf.register(udfName, zipWithOrderUDF1, newType)
    val res = nestedWithColumn(ds)(arrCol, expr(s"${udfName}(${arrCol})"))
    res
  }

  /**
   * Map transformation for array columns, preserving the original order of the array elements
   *
   *  @param ds Dataset to be transformed
   *  @param arrayCol The dot-separated path of the array column
   *  @param fn Function which takes a dataset and returns dataset. The array elements in the dataset have same path as the original array (one element per record).
   *  		Fn has to preserve the element path.
   *  @return Dataset with the array of transformed elements (at the original path) and preserving the original order of the elements.
   */
  def arrayTransform(ds: Dataset[Row], arrayCol: String)(fn: ((Dataset[Row]) => Dataset[Row]))(implicit spark: SparkSession) = {

    import spark.implicits._

    val groupField = arrayCol.replace(".", "_") + "_arrayTransformId"
    val indField = s"${groupField}_index"
    val sizeField = s"${groupField}_size"

    logger.info(s"ArrayTransform: Storing array size col for $arrayCol as $sizeField")
    this.arraySizeCols.put(arrayCol, sizeField)

    val tokens = arrayCol.split("\\.")

    val id = ds.withColumn(groupField, monotonically_increasing_id()).withColumn(sizeField, size(col(arrayCol))).persist(StorageLevel.MEMORY_ONLY_SER)
    val zipped = zipWithOrder(id, arrayCol)
    val exploded = zipped.withColumn(arrayCol, explode_outer(arrCol(arrayCol)))
    val tmp = if (tokens.length > 1) nestedWithColumn(exploded)(arrayCol, col(s"`$arrayCol`")).drop(col(s"`$arrayCol`")) else exploded
    val flattened = nestedWithColumn(nestedWithColumn(tmp)(indField, arrCol(s"$arrayCol._1")))(arrayCol, arrCol(s"$arrayCol._2"))
    val transformed = fn(flattened)
    val withInd = nestedWithColumn(transformed)(arrayCol, struct(arrCol(indField) as "_1", arrCol(arrayCol) as "_2")).drop(indField)
    val list = withInd.groupBy(arrCol(groupField)).agg(collect_list(col(arrayCol)) as arrayCol)
    val tmp2 = if (tokens.length > 1) nestedWithColumn(list)(arrayCol, col(s"`$arrayCol`")).drop(col(s"`$arrayCol`")) else list

    val origArraySchema = SchemaUtils.getFieldType(arrayCol, ds.schema).getOrElse(throw new IllegalStateException(s"The field $arrayCol not found in the transformed schema.")).asInstanceOf[ArrayType]
    val arrayChildSchema = SchemaUtils.getFieldType(arrayCol, transformed.schema).getOrElse(throw new IllegalStateException(s"The field $arrayCol not found in the transformed schema."))
    val arraySchema = ArrayType.apply(arrayChildSchema, origArraySchema.containsNull)

    spark.udf.register(s"${groupField}_handleNullAndEmpty", new UDF2[Int, Seq[Row], Seq[Row]] {
      override def call(t1: Int, t2: Seq[Row]) = {
        if (t1 == -1) null
        else if (t1 == 0) Seq()
        else t2
      }

    }, arraySchema)

    val res1 = id.as("orig").join(tmp2.as("trns"), col(s"orig.$groupField") === col(s"trns.$groupField")).select($"orig.*", col(s"trns.${tokens.head}") as s"${tokens.head}_RENAME_TEMP")
    val res1a = nestedWithColumn(res1)(arrayCol, arrCol(s"${tokens.head}_RENAME_TEMP.${tokens.tail.mkString(".")}")).drop(s"${tokens.head}_RENAME_TEMP")
    val res2 = nestedWithColumn(res1a)(arrayCol, sort_array(arrCol(arrayCol)))
    val res3 = nestedWithColumn(res2)(arrayCol, arrCol(s"$arrayCol._2"))
    val res4 = nestedWithColumn(res3)(arrayCol, expr(s"${groupField}_handleNullAndEmpty($sizeField, $arrayCol)")).drop(sizeField).drop(arrCol(groupField))

    res4
  }

  def nestedDrop(df: Dataset[Row], colName: String) = {
    val toks = colName.split("\\.")
    if (toks.size == 1) df.drop(colName) else {
      if (SchemaUtils.getFirstArrayPath(colName, df.schema) != "") throw new IllegalStateException(s"Array Type fields in the path of $colName - dropping arrays children is not supported")
      val parentPath = toks.init.mkString(".")
      logger.info(s"Nested Drop: parent path $parentPath")
      val parentType = SchemaUtils.getFieldType(parentPath, df.schema)
      logger.info(s"Nested Drop: parent type $parentType")
      val parentCols = if (parentType.isEmpty) throw new IllegalStateException(s"Field $colName does not exist in \n ${df.printSchema()}") else parentType.get.asInstanceOf[StructType].fields
      val replace = struct(parentCols.filter(_.name != toks.last).map(x => arrCol(s"${parentPath}.${x.name}") as x.name): _*)
      this.nestedWithColumn(df)(parentPath, replace)
    }
  }

  def flattenArrays(df: Dataset[Row], colName: String)(implicit spark: SparkSession) = {
    val typ = SchemaUtils.getFieldType(colName, df.schema).getOrElse(throw new Error(s"Field $colName does not exist in ${df.schema.printTreeString()}"))
    if (!typ.isInstanceOf[ArrayType]) {
      logger.info(s"Field $colName is not an ArrayType, returning the original dataset!")
      df
    } else {
      val arrType = typ.asInstanceOf[ArrayType]
      if (!arrType.elementType.isInstanceOf[ArrayType]) {
        logger.info(s"Field ${colName} is not a nested array, returning the original dataset!")
        df
      } else {
        val udfName = colName.replace('.', '_') + System.currentTimeMillis()

        spark.udf.register(udfName, new UDF1[Seq[Seq[Row]], Seq[Row]] {
          def call(t1: Seq[Seq[Row]]): Seq[Row] = if (t1 == null) null.asInstanceOf[Seq[Row]] else t1.filter(_ != null).flatten
        }, arrType.elementType)

        nestedWithColumn(df)(colName, callUDF(udfName, col(colName)))
      }
    }

  }

  def handleArrays(targetColumn: String, df: Dataset[Row])(fn: (Dataset[Row] => Dataset[Row]))(implicit spark: SparkSession): Dataset[Row] = {
    logger.info(s"handleArrays: Finding first array for $targetColumn")
    val firstArr = SchemaUtils.getFirstArrayPath(targetColumn, df.schema)
    logger.info(s"handleArrays: First array field $firstArr")
    firstArr match {
      case "" => fn(df)
      case _ => {
        val res = ArrayTransformations.arrayTransform(df, firstArr) {
          case dfArr => handleArrays(targetColumn, dfArr)(fn)
        }
        res
      }
    }
  }

  /**
    * Map transformation for columns that can be inside nested structs, arrays and it's combinations
    *
    *  @param df Dataframe to be transformed
    *  @param inputColumnName A column name for which to apply the transformation, e.g. `company.employee.firstName`
    *  @param outputColumnName The output column name. The path is optional, e.g. you can use `conformedName` instead of `company,employee.conformedName`
    *  @param expression A function that applies a SPark transformation to a column
    *  @return Dataframe with a new field that contains transformed vvalues.
    */
  def nestedWithColumnMap(df: DataFrame, inputColumnName: String, outputColumnName: String, expression: Column => Column): DataFrame = {
    // The name of the field is the last token of fieldOut
    val outputFieldName = outputColumnName.split('.').last

    // Sequential lambda name generator
    var lambdaIndex = 1
    def getLambdaName: String = {
      val name = s"v$lambdaIndex"
      lambdaIndex += 1
      name
    }

    def mapStruct(schema: StructType, path: Seq[String], parentColumn: Option[Column] = None): Seq[Column] = {
      val fieldName = path.head
      val isTop = path.lengthCompare(2) < 0
      val mappedFields = new ListBuffer[Column]()
      val newColumns = schema.fields.flatMap(field => {
        val curColumn = parentColumn match {
          case None => new Column(field.name)
          case Some(col) => col.getField(field.name).as(field.name)
        }

        if (field.name.compareToIgnoreCase(fieldName) != 0) {
          Seq(curColumn)
        } else {
          if (isTop) {
            field.dataType match {
              case dt: ArrayType =>
                mapArray(dt, path, parentColumn)
              case _ =>
                mappedFields += expression(curColumn).as(outputFieldName)
                // Retain the original column
                Seq(curColumn)
            }
          } else {
            field.dataType match {
              case dt: StructType => Seq(struct(mapStruct(dt, path.tail, Some(curColumn)): _*).as(fieldName))
              case dt: ArrayType => mapArray(dt, path, parentColumn)
              case _ => throw new IllegalArgumentException(s"Field ${field.name} is not a struct type or an array.")
            }
          }
        }
      })
      newColumns ++ mappedFields
    }

    def getDeepestArrayType(arrayType: ArrayType): DataType = {
      arrayType.elementType match {
        case a: ArrayType => getDeepestArrayType(a)
        case b => b
      }
    }

    def mapNestedArrayOfPrimitives(schema: ArrayType, curColumn: Column): Column = {
      val lambdaName = getLambdaName
      val elemType = schema.elementType

      elemType match {
        case _: StructType => throw new IllegalArgumentException(s"Unexpected usage of mapNestedArrayOfPrimitives() on structs.")
        case dt: ArrayType =>
          val innerArray = mapNestedArrayOfPrimitives(dt, _$(lambdaName))
          transform(curColumn, lambdaName, innerArray)
        case dt => transform(curColumn, lambdaName, expression(_$(lambdaName)))
      }
    }

    def mapArray(schema: ArrayType, path: Seq[String], parentColumn: Option[Column] = None, isParentArray: Boolean = false): Seq[Column] = {
      val isTop = path.lengthCompare(2) < 0
      val elemType = schema.elementType
      val lambdaName = getLambdaName
      val fieldName = path.head
      val mappedFields = new ListBuffer[Column]()

      val curColumn = parentColumn match {
        case None => new Column(fieldName)
        case Some(col) if !isParentArray => col.getField(fieldName).as(fieldName)
        case Some(col) if isParentArray => col
      }

      val newColumn = elemType match {
        case dt: StructType =>
          val innerStruct = struct(mapStruct(dt, path.tail, Some(_$(lambdaName))): _*)
          transform(curColumn, lambdaName, innerStruct).as(fieldName)
        case dt: ArrayType =>
          val deepestType = getDeepestArrayType(dt)
          deepestType match {
            case _: StructType =>
              val innerArray = mapArray(dt, path, Some(_$(lambdaName)), isParentArray = true)
              transform(curColumn, lambdaName, innerArray.head).as(fieldName)
            case _ =>
              if (isTop) {
                mappedFields += transform(curColumn, lambdaName, mapNestedArrayOfPrimitives(dt, _$(lambdaName))).as(outputFieldName)
                curColumn
              } else {
                throw new IllegalArgumentException(s"Field $fieldName is not a struct or an array of struct type.")
              }
          }
        case dt =>
          if (isTop) {
            mappedFields += transform(curColumn, lambdaName, expression(_$(lambdaName))).as(outputFieldName)
            curColumn
          } else {
            throw new IllegalArgumentException(s"Field $fieldName is not a struct type or an array.")
          }
      }
      Seq(newColumn) ++ mappedFields
    }

    val schema = df.schema
    val path = inputColumnName.split('.')
    df.select(mapStruct(schema, path): _*) // ;-]
  }

}
