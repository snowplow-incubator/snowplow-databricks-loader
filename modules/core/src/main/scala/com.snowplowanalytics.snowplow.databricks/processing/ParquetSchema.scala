/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.databricks.processing

import com.github.mjakubowski84.parquet4s.SchemaDef
import org.apache.parquet.schema.{LogicalTypeAnnotation => PAnnotation, MessageType, Type => PType, Types}
import org.apache.parquet.schema.PrimitiveType.{PrimitiveTypeName => PPrimitive}

import com.snowplowanalytics.iglu.schemaddl.parquet.{Field, Type}
import com.snowplowanalytics.snowplow.loaders.transform.AtomicFields

private[processing] object ParquetSchema {

  /**
   * Builds the Spark schema for building a data frame of a batch of events
   *
   * The returned schema includes atomic fields and non-atomic fields but not the load_tstamp column
   */
  def forBatch(entities: Vector[Field]): MessageType = {
    val types = atomic ++ entities.map(asParquetField)
    Types
      .buildMessage()
      .addFields(types: _*)
      .named("snowplow")
  }

  /**
   * Ordered Fields corresponding to the output from Enrich
   *
   * @note
   *   this is a `val` not a `def` because we use it over and over again.
   */
  val atomic: Vector[PType] = AtomicFields.static.map(asParquetField)

  private def asParquetField(ddlField: Field): PType = {
    val normalizedName = Field.normalize(ddlField).name
    fieldType(ddlField.fieldType)
      .withRequired(ddlField.nullability.required)
      .apply(normalizedName)
  }

  val byteArrayLengthForDecimal = 16 // because 16 bytes is required by max precision 38

  private def fieldType(ddlType: Type): SchemaDef = ddlType match {
    case Type.String =>
      SchemaDef.primitive(PPrimitive.BINARY, logicalTypeAnnotation = Some(PAnnotation.stringType()))
    case Type.Boolean =>
      SchemaDef.primitive(PPrimitive.BOOLEAN)
    case Type.Integer =>
      SchemaDef.primitive(PPrimitive.INT32)
    case Type.Long =>
      SchemaDef.primitive(PPrimitive.INT64)
    case Type.Double =>
      SchemaDef.primitive(PPrimitive.DOUBLE)
    case Type.Decimal(precision, scale) =>
      val logicalType = Option(PAnnotation.decimalType(scale, Type.DecimalPrecision.toInt(precision)))
      precision match {
        case Type.DecimalPrecision.Digits9 =>
          SchemaDef.primitive(PPrimitive.INT32, logicalTypeAnnotation = logicalType)
        case Type.DecimalPrecision.Digits18 =>
          SchemaDef.primitive(PPrimitive.INT64, logicalTypeAnnotation = logicalType)
        case Type.DecimalPrecision.Digits38 =>
          SchemaDef.primitive(
            PPrimitive.FIXED_LEN_BYTE_ARRAY,
            logicalTypeAnnotation = logicalType,
            length                = Option(byteArrayLengthForDecimal)
          )
      }
    case Type.Date =>
      SchemaDef.primitive(PPrimitive.INT32, logicalTypeAnnotation = Some(PAnnotation.dateType()))
    case Type.Timestamp =>
      SchemaDef.primitive(PPrimitive.INT64, logicalTypeAnnotation = Some(PAnnotation.timestampType(true, PAnnotation.TimeUnit.MICROS)))
    case Type.Struct(fields) =>
      SchemaDef.group(fields.toVector.map(asParquetField): _*)
    case Type.Array(element, elNullability) =>
      val listElement = fieldType(element).withRequired(elNullability.required)
      SchemaDef.list(listElement)
    case Type.Json =>
      SchemaDef.primitive(PPrimitive.BINARY, logicalTypeAnnotation = Some(PAnnotation.jsonType()))
  }
}
