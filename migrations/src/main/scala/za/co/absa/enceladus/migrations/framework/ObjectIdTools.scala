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

package za.co.absa.enceladus.migrations.framework

/**
  * The Object Id tools are used to fetch and inject an Object Ids from/to a JSON documents according
  * to expectations of MongoDB.
  *
  * Tools work directly with JSON strings and do not depend on a way the way they are serialized.
  *
  * The motivation for these tools is to be able to transparently retain Object Ids of documents while
  * doing JSON to JSON transformations during a migration.
  *
  * Data model usually don't contain Object Ids since it is storage-layer-specific thing. So if a JSON
  * to JSON transformation is done through deserialization from an old model, copying to the new model, and
  * subsequent serialization to a new JSON, the Object Ids will likely to get lost.
  *
  * The purpose of these tools is to extract Object Ids directly from MongoDB JSON documents and inject them
  * into the output JSON documents, if no Object Id is there.
  *
  * If a source JSON string does not contain an Object Id (if a different storage layer is used, for instance),
  * nothing will be injected into the target JSON.
  */
object ObjectIdTools {
  /**
    * Gets a MongoDB Object Id from a JSON string if it has one.
    * The format of an ObjectId is expected to be like this:
    *
    * `"_id" : { "$oid" : "5b98eea5a43a28a6154a2453" }"`
    *
    * @param document A JSON document as a string
    * @return A substring representing an ObjectId extracted from the document, if present
    */
  def getObjectIdFromDocument(document: String): Option[String] = {
    val oidRegExp = """(\"_id\"[.\s]*\:[.\s]*\{[a-zA-Z0-9\s\"\$\:]*\})""".r
    oidRegExp.findFirstIn(document)
  }

  /**
    * Puts an ObjectId (if provided) into a JSON document if there is no ObjectId there already.
    * The format of an ObjectId is expected to be like this:
    *
    * `"_id" : { "$oid" : "5b98eea5a43a28a6154a2453" }"`
    *
    * @param document    A JSON document as a string
    * @param objectIdOpt an optional ObjectId to put into the document
    * @return A substring representing an ObjectId extracted from the document, if present
    */
  def putObjectIdIfNotPresent(document: String, objectIdOpt: Option[String]): String = {
    if (getObjectIdFromDocument(document).isEmpty) {
      val newDocument = objectIdOpt match {
        case Some(id) =>
          val i = document.indexOf('{')
          if (i >= 0) {
            val start = document.slice(0, i + 1)
            val end = document.slice(i + 1, document.length)
            s"$start$id,$end"
          } else {
            document
          }
        case None =>
          document
      }
      newDocument
    } else {
      document
    }
  }

  /**
    * Extracts an object id value from an object id json formatted object id.
    * Here is an example. For the following input (all quotes are parts of the string itself):
    *
    * `"_id" : { "$oid" : "5b98eea5a43a28a6154a2453" }"`
    *
    * the output of the method will be:
    *
    * `5b98eea5a43a28a6154a2453`
    *
    * @param objIdExpression An Object Id as a JSON key-value expression.
    * @return A string value of the Object Id
    */
  def extractId(objIdExpression: String): String = {
    val idRegexp = """.*\"\$oid\"\s*:\s*\"([0-9a-fA-F]*)\".*""".r
    objIdExpression match {
      case idRegexp(id) => id
      case _ => throw new RuntimeException(s"Unable to extract Object Id from '$objIdExpression'")
    }
  }

}
