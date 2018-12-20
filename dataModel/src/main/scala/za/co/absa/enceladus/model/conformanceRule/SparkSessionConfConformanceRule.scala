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

package za.co.absa.enceladus.model.conformanceRule

/**
 * Rule for getting values out of spark session conf.
 *
 * This is an easy way of introducing values from the info file into the dataset (such as version), where control framework will populate the conf.
 *
 * Gets value from spark.sessionState.conf
 */
//case class SparkSessionConfConformanceRule(
//    order: Int,
//    outputColumn: String,
//    controlCheckpoint: Boolean,
//    sparkConfKey: String) extends ConformanceRule
