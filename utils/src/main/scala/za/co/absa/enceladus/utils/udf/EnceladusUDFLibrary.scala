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

import org.apache.spark.sql.SparkSession
import za.co.absa.enceladus.utils.error.{ErrorMessage, Mapping}
import za.co.absa.enceladus.utils.udf.UDFNames._
import za.co.absa.spark.commons.OncePerSparkSession

import scala.collection.mutable


class EnceladusUDFLibrary()(implicit sparkToRegisterTo: SparkSession) extends OncePerSparkSession {

  override protected def register(implicit spark: SparkSession): Unit = {
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

  }
}
