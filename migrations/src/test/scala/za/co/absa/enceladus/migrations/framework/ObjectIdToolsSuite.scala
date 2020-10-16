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

package za.co.absa.enceladus.migrations.framework

import org.scalatest.funsuite.AnyFunSuite

class ObjectIdToolsSuite extends AnyFunSuite {

  test("Test ObjectId extractor ") {
    val doc1 = """{ "_id" : { "$oid" : "5b98eea5a43a28a6154a2453" }, "name" : "Test" }"""
    val doc2 = """{ "_id": { "$oid" : "5b98eea5a43a28a6154a2453" }, "name" : "Test" }"""
    val doc3 = """{"_id":{"$oid":"5b98eea5a43a28a6154a2453"},"name":"Test"}"""
    val doc4 = "{\n\t\"_id\"\n\t:\n\t{\n\t\"$oid\"\n\t:\n\t\"5b98eea5a43a28a6154a2453\"\n\t}\n\t, \"name\" : \"Test\" }"
    val doc5 = """{"_id1":{"$oid":"5b98eea5a43a28a6154a2453"},"name":"Test"}"""

    assert(ObjectIdTools.getObjectIdFromDocument(doc1).nonEmpty)
    assert(ObjectIdTools.getObjectIdFromDocument(doc2).nonEmpty)
    assert(ObjectIdTools.getObjectIdFromDocument(doc3).nonEmpty)
    assert(ObjectIdTools.getObjectIdFromDocument(doc4).nonEmpty)
    assert(ObjectIdTools.getObjectIdFromDocument(doc5).isEmpty)
  }

  test("Test ObjectId 'injector' ") {
    val oid = """"_id":{"$oid":"5b98eea5a43a28a6154a2453"}"""

    val doc1 = """{ "name" : "Test" }"""
    val doc2 = """   { "name" : "Test" }   """
    val doc3 = """{ }"""
    val doc4 = """{ "_id" : { "$oid" : "5b98eea5a43a28a6154a2453" }, "name" : "Test" }"""

    val expected1 = """{"_id":{"$oid":"5b98eea5a43a28a6154a2453"}, "name" : "Test" }"""
    val expected2 = """   {"_id":{"$oid":"5b98eea5a43a28a6154a2453"}, "name" : "Test" }   """
    val expected3 = """{"_id":{"$oid":"5b98eea5a43a28a6154a2453"}, }"""
    val expected4 = doc4

    assert (ObjectIdTools.putObjectIdIfNotPresent(doc1, None) == doc1)
    assert (ObjectIdTools.putObjectIdIfNotPresent(doc1, Some(oid)) == expected1)
    assert (ObjectIdTools.putObjectIdIfNotPresent(doc2, Some(oid)) == expected2)
    assert (ObjectIdTools.putObjectIdIfNotPresent(doc3, Some(oid)) == expected3)
    assert (ObjectIdTools.putObjectIdIfNotPresent(doc4, Some(oid)) == expected4)
  }

  test("Test ObjectId value extractor should disregard whitespaces around keys and values") {
    val oid1 = """ "_id" : { "$oid" : "5a02c0799b2c26c8fc064e01" } """
    val oid2 = """"_id" : { "$oid" : "5a02c0799b2c26c8fc064e01" }"""
    val oid3 = """"_id":{ "$oid" : "5a02c0799b2c26c8fc064e01"}"""
    val oid4 = """"_id": { "$oid"  :  "5a02c0799b2c26c8fc064e01"  }  """
    val oid5 = """"_id":{"$oid":"5a02c0799b2c26c8fc064e01"}"""

    val expected = "5a02c0799b2c26c8fc064e01"

    assert(ObjectIdTools.extractId(oid1) == expected)
    assert(ObjectIdTools.extractId(oid2) == expected)
    assert(ObjectIdTools.extractId(oid3) == expected)
    assert(ObjectIdTools.extractId(oid4) == expected)
    assert(ObjectIdTools.extractId(oid5) == expected)
  }

}
