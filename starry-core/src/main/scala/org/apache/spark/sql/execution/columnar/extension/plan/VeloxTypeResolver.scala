package org.apache.spark.sql.execution.columnar.extension.plan

import org.apache.spark.sql.types.{ArrayType, BinaryType, DataType, LongType, StringType, StructField, StructType}
import org.json4s.JArray
import org.json4s.JsonAST.{JObject, JString, JValue}
import org.json4s.jackson.JsonMethods.parse

object VeloxTypeResolver {

  private object JSortedObject {
    def unapplySeq(value: JValue): Option[List[(String, JValue)]] = value match {
      case JObject(seq) => Some(seq.sortBy(_._1))
      case _ => None
    }
  }

  def parseDataType(json: String): DataType = parseDataType(parse(json.toLowerCase()))

  def parseDataType(json: JValue): DataType = json match {
    case JSortedObject(
      ("name", JString("type")),
      ("type", typeStr: JString),
    ) =>
      typeStr.values match {
        case "bigint" => LongType
        case "varbinary" => BinaryType
        case "varchar" => StringType
        case _ =>
          DataType.parseDataType(typeStr)
      }
    case JSortedObject(
      ("ctypes", JArray(ctypes)),
      ("name", JString("type")),
      ("type", JString("array")),
      ) =>
      val childType = ctypes.map(parseDataType).head
      ArrayType(childType)
    case JSortedObject(
        ("ctypes", JArray(ctypes)),
        ("name", JString("type")),
        ("names", JArray(names)),
        ("type", JString("row")),
        ) =>
      val types = ctypes.map(parseDataType)
      var i = 0
      val fname = () => {
        i += 1
        s"f_$i"
      }
      val fields = types.zip(names).map {
        case (fieldType, name) =>
          if (name.values.toString.nonEmpty) {
            StructField(name.values.toString, fieldType, true)
          } else {
            StructField(fname(), fieldType, true)
          }
      }
      StructType(fields)
    case other =>
      throw new UnsupportedOperationException(s"Unrecognized velox type json: $other")
  }

}
