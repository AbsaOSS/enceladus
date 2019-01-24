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

package za.co.absa.enceladus.utils.error

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.api.java._

import scala.collection.mutable

case class UDFLibrary()(implicit val spark: SparkSession) {

  import spark.implicits._

  spark.udf.register("stdCastErr", { (errCol: String, rawValue: String) =>
    ErrorMessage.stdCastErr(errCol, rawValue)
  })

  spark.udf.register("stdNullErr", { (errCol: String) =>
    ErrorMessage.stdNullErr(errCol)
  })

  spark.udf.register("confMappingErr", { (errCol: String, rawValues: Seq[String], mappings: Seq[Mapping]) =>
    ErrorMessage.confMappingErr(errCol, rawValues, mappings)
  })

  spark.udf.register("confCastErr", { (errCol: String, rawValue: String) =>
    ErrorMessage.confCastErr(errCol, rawValue)
  })

  spark.udf.register("confNegErr", { (errCol: String, rawValue: String) =>
    ErrorMessage.confNegErr(errCol, rawValue)
  })

  spark.udf.register("arrayDistinctErrors", (arr: mutable.WrappedArray[ErrorMessage]) => arr.distinct)

  val cleanErrCol = new UDF1[Seq[Row], Seq[Row]] {
    override def call(t1: Seq[Row]) = {
      t1.filter({ row =>
        row != null && {
          val typ = row.getString(0)
          typ != null
        }
      })
    }
  }

  spark.udf.register("cleanErrCol", cleanErrCol, ArrayType.apply(ErrorMessage.errorColSchema, false))

  val errorColumnAppend = new UDF2[Seq[Row], Row, Seq[Row]] {
    override def call(t1: Seq[Row], t2: Row) : Seq[Row] = {
      t1 :+ t2
    }
  }

  spark.udf.register("errorColumnAppend", errorColumnAppend, ArrayType.apply(ErrorMessage.errorColSchema, false))
 
}
