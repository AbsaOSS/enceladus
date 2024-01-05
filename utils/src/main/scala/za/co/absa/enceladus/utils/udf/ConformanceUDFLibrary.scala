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

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.api.java.UDF2
import org.apache.spark.sql.types.ArrayType
import za.co.absa.enceladus.utils.error.EnceladusErrorMessage
import za.co.absa.spark.commons.errorhandling.ErrorMessage
import za.co.absa.enceladus.utils.udf.ConformanceUDFNames._
import za.co.absa.spark.commons.OncePerSparkSession

import scala.collection.mutable


class ConformanceUDFLibrary()(implicit sparkToRegisterTo: SparkSession) extends OncePerSparkSession {

  override protected def register(implicit spark: SparkSession): Unit = {
    spark.udf.register(confMappingErr, { (errCol: String, rawValues: Seq[String], mappings: Seq[ErrorMessage.Mapping]) =>
      EnceladusErrorMessage.confMappingErr(errCol, rawValues, mappings)
    })

    spark.udf.register(confCastErr, { (errCol: String, rawValue: String) =>
      EnceladusErrorMessage.confCastErr(errCol, rawValue)
    })

    spark.udf.register(confNegErr, { (errCol: String, rawValue: String) =>
      EnceladusErrorMessage.confNegErr(errCol, rawValue)
    })

    spark.udf.register(confLitErr, { (errCol: String, rawValue: String) =>
      EnceladusErrorMessage.confLitErr(errCol, rawValue)
    })

    spark.udf.register(arrayDistinctErrors, // this UDF is registered for _spark-hats_ library sake
      (arr: mutable.WrappedArray[ErrorMessage]) =>
        if (arr != null) {
          arr.distinct.filter((a: AnyRef) => a != null)
        } else {
          Seq[ErrorMessage]()
        }
    )

    spark.udf.register(errorColumnAppend, // this should be removed with more general error handling implemented
                       ConformanceUDFLibrary.errorColumnAppend,
                       ArrayType(ErrorMessage.errorColSchema(spark), containsNull = false))

  }
}

object ConformanceUDFLibrary {

  private val errorColumnAppend = new UDF2[Seq[Row], Row, Seq[Row]] {
    override def call(t1: Seq[Row], t2: Row): Seq[Row] = {
      t1 :+ t2
    }
  }
}
