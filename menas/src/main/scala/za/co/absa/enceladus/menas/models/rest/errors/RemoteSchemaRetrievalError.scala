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

package za.co.absa.enceladus.menas.models.rest.errors

import za.co.absa.enceladus.menas.models.rest.ResponseError
import za.co.absa.enceladus.menas.models.rest.exceptions.RemoteSchemaRetrievalException
import za.co.absa.enceladus.menas.utils.SchemaType

/**
 * This error is produced when an incorrect schema format is provided.
 */
case class RemoteSchemaRetrievalError(
                                       errorType: String,
                                       schemaType: SchemaType.Value,
                                       causeDesc: Option[String]
                                     ) extends ResponseError

object RemoteSchemaRetrievalError {
  def fromException(ex: RemoteSchemaRetrievalException): RemoteSchemaRetrievalError = RemoteSchemaRetrievalError(
    errorType = "schema_retrieval_error",
    schemaType = ex.schemaType,
    causeDesc = Some(ex.cause).map { cause => s"${cause.getClass.toString}: ${cause.getMessage}" }
  )
}

