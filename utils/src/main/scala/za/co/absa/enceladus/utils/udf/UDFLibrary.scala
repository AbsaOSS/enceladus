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

package za.co.absa.enceladus.utils.udf

import java.util.Base64

import org.apache.spark.sql.api.java._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}
import za.co.absa.enceladus.utils.error.{ErrorMessage, Mapping}
import za.co.absa.enceladus.utils.udf.UDFNames._

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

class UDFLibrary()(implicit val spark: SparkSession) {

  spark.udf.register(stdCastErr, { (errCol: String, rawValue: String) =>
    ErrorMessage.stdCastErr(errCol, rawValue)
  })

  spark.udf.register(stdNullErr, { errCol: String => ErrorMessage.stdNullErr(errCol) })

  spark.udf.register(stdSchemaErr, { errRow: String => ErrorMessage.stdSchemaError(errRow) })

  spark.udf.register(confMappingErr, { (errCol: String, rawValues: Seq[String], mappings: Seq[Mapping]) =>
    ErrorMessage.confMappingErr(errCol, rawValues, mappings)
  })

  spark.udf.register(confCastErr, { (errCol: String, rawValue: String) =>
    ErrorMessage.confCastErr(errCol, rawValue)
  })

  spark.udf.register(confNegErr, { (errCol: String, rawValue: String) =>
    ErrorMessage.confNegErr(errCol, rawValue)
  })

  spark.udf.register(confLitErr, { (errCol: String, rawValue: String) =>
    ErrorMessage.confLitErr(errCol, rawValue)
  })

  spark.udf.register(arrayDistinctErrors, // this UDF is registered for _spark-hats_ library sake
    (arr: mutable.WrappedArray[ErrorMessage]) =>
      if (arr != null) {
        arr.distinct.filter((a: AnyRef) => a != null)
      } else {
        Seq[ErrorMessage]()
      }
  )

  spark.udf.register(cleanErrCol,
                     UDFLibrary.cleanErrCol,
                     ArrayType.apply(ErrorMessage.errorColSchema, containsNull = false))

  spark.udf.register(errorColumnAppend,
                     UDFLibrary.errorColumnAppend,
                     ArrayType.apply(ErrorMessage.errorColSchema, containsNull = false))


  spark.udf.register(binaryUnbase64,
    {stringVal: String => Try {
      Base64.getDecoder.decode(stringVal)
    } match {
      case Success(decoded) => decoded
      case Failure(_) => null
    }})
}

object UDFLibrary {
  private val cleanErrCol = new UDF1[Seq[Row], Seq[Row]] {
    override def call(t1: Seq[Row]): Seq[Row] = {
      t1.filter({ row =>
        row != null && {
          val typ = row.getString(0)
          typ != null
        }
      })
    }
  }

  private val errorColumnAppend = new UDF2[Seq[Row], Row, Seq[Row]] {
    override def call(t1: Seq[Row], t2: Row): Seq[Row] = {
      t1 :+ t2
    }
  }
}
