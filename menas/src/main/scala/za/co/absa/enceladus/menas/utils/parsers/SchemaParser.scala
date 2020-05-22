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

package za.co.absa.enceladus.menas.utils.parsers

import org.apache.avro.{Schema => AvroSchema}
import org.apache.spark.sql.avro.SchemaConverters
import org.apache.spark.sql.types.StructType
import za.co.absa.cobrix.cobol.parser.CopybookParser
import za.co.absa.cobrix.cobol.parser.exceptions.SyntaxErrorException
import za.co.absa.cobrix.spark.cobol.schema.{CobolSchema, SchemaRetentionPolicy}
import za.co.absa.enceladus.menas.models.rest.exceptions.SchemaParsingException
import za.co.absa.enceladus.menas.utils.SchemaType
import za.co.absa.enceladus.menas.utils.converters.SparkMenasSchemaConvertor

import scala.util.control.NonFatal

trait SchemaParser {
  def parse(fileContent: String): StructType
}

object SchemaParser {

  trait SchemaParserFactory {
    def getParser(schemaType: SchemaType.Value): SchemaParser
  }

  def getFactory(sparkMenasConvertor: SparkMenasSchemaConvertor): SchemaParserFactory = new SchemaParserFactory {
    override def getParser(schemaType: SchemaType.Value): SchemaParser = schemaType match {
      case SchemaType.Struct => new StructSchemaParser(sparkMenasConvertor)
      case SchemaType.Copybook => CopybookSchemaParser
      case SchemaType.Avro => AvroSchemaParser
    }
  }

  private class StructSchemaParser(sparkMenasConvertor: SparkMenasSchemaConvertor) extends SchemaParser {

    /**
     * Parses an StructType JSON file contents and converts it to Spark [[StructType]].
     *
     * @param structTypeJson A StructType JSON string.
     * @return The parsed schema as an instance of [[StructType]].
     */
    def parse(structTypeJson: String): StructType = {
      try {
        sparkMenasConvertor.convertAnyToStructType(structTypeJson)
      } catch {
        case e: IllegalStateException =>
          throw SchemaParsingException(SchemaType.Struct, e.getMessage, cause = e)
      }
    }
  }

  private object AvroSchemaParser extends SchemaParser {
    /**
     * Parses an Avro file content and converts it to Spark [[StructType]].
     *
     * @param avroFileContent An Avro-schema JSON string.
     * @return The parsed schema as an instance of [[StructType]].
     */
    def parse(avroFileContent: String): StructType = {
      try {
        val schema = new AvroSchema.Parser().parse(avroFileContent)
        SchemaConverters.toSqlType(schema).dataType.asInstanceOf[StructType]
      } catch {
        case NonFatal(e) =>
          throw SchemaParsingException(SchemaType.Avro, e.getMessage, cause = e)
      }
    }
  }

  private object CopybookSchemaParser extends SchemaParser {
    /**
     * Parses a COBOL copybook file contents and converts it to Spark [[StructType]].
     *
     * @param copybookContents A COBOL copybook contents.
     * @return The parsed schema as an instance of [[StructType]].
     */
    def parse(copybookContents: String): StructType = {
      try {
        val parsedSchema = CopybookParser.parseTree(copybookContents)
        val cobolSchema = new CobolSchema(parsedSchema, SchemaRetentionPolicy.CollapseRoot, "", false)
        cobolSchema.getSparkSchema
      } catch {
        case e: SyntaxErrorException =>
          throw SchemaParsingException(SchemaType.Copybook, e.getMessage, Some(e.lineNumber), None, Some(e.field), e)
        case e: IllegalStateException =>
          // Cobrix can throw this exception if an unknown AST object is encountered.
          // This might be considered a parsing error.
          throw SchemaParsingException(SchemaType.Copybook, e.getMessage, cause = e)
      }
    }
  }
}
