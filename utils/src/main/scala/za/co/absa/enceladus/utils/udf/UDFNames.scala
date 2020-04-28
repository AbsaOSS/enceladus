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

object UDFNames {
  final val stdCastErr = "stdCastErr"
  final val stdNullErr = "stdNullErr"
  final val stdSchemaErr = "stdSchemaErr"

  final val confMappingErr = "confMappingErr"
  final val confCastErr = "confCastErr"
  final val confNegErr = "confNegErr"
  final val confLitErr = "confLitErr"

  final val arrayDistinctErrors = "arrayDistinctErrors"
  final val cleanErrCol = "cleanErrCol"
  final val errorColumnAppend = "errorColumnAppend"

  final val uuid = "uuid"
  final val pseudoUuid = "pseudoUuid"
}
