/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.execution.columnar.expressions

import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.{EqualNullSafe, _}
import org.apache.spark.sql.catalyst.optimizer.NormalizeNaNAndZero
import org.apache.spark.sql.execution.ScalarSubquery
import org.apache.spark.sql.execution.columnar.expressions.aggregate.BitmapCountDistinctAggFunction
import org.apache.spark.sql.execution.columnar.expressions.convert.NameSig

object ExpressionMappings {

  final val SUM = "sum"
  final val AVG = "avg"
  final val COUNT = "count"
  final val MIN = "min"
  final val MAX = "max"
  final val STDDEV_SAMP = "stddev_samp"
  final val STDDEV_POP = "stddev_pop"
  final val COLLECT_LIST = "array_agg"
  final val BLOOM_FILTER_AGG = "bloom_filter_agg"
  final val VAR_SAMP = "var_samp"
  final val VAR_POP = "var_pop"
  final val BIT_AND_AGG = "bit_and"
  final val BIT_OR_AGG = "bit_or"
  final val BIT_XOR_AGG = "bit_xor"
  final val CORR = "corr"
  final val COVAR_POP = "covar_pop"
  final val COVAR_SAMP = "covar_samp"
  final val LAST = "last"
  final val LAST_IGNORE_NULL = "last_ignore_null"
  final val FIRST = "first"
  final val FIRST_IGNORE_NULL = "first_ignore_null"
  final val APPROX_DISTINCT = "approx_distinct"

  // Function names used by Substrait plan.
  final val ADD = "add"
  final val SUBTRACT = "subtract"
  final val MULTIPLY = "multiply"
  final val DIVIDE = "divide"
  final val AND = "and"
  final val OR = "or"
  final val CAST = "cast"
  final val COALESCE = "coalesce"
  final val LIKE = "like"
  final val RLIKE = "rlike"
  final val REGEXP_REPLACE = "regexp_replace"
  final val REGEXP_EXTRACT = "regexp_extract"
  final val REGEXP_EXTRACT_ALL = "regexp_extract_all"
  final val EQUAL = "equal"
  final val LESS_THAN = "lt"
  final val LESS_THAN_OR_EQUAL = "lte"
  final val GREATER_THAN = "gt"
  final val GREATER_THAN_OR_EQUAL = "gte"
  final val ALIAS = "alias"
  final val IS_NOT_NULL = "is_not_null"
  final val IS_NULL = "is_null"
  final val NOT = "not"
  final val IS_NAN = "isnan"

  // SparkSQL String functions
  final val ASCII = "ascii"
  final val CHR = "chr"
  final val EXTRACT = "extract"
  final val ENDS_WITH = "ends_with"
  final val STARTS_WITH = "starts_with"
  final val CONCAT = "concat"
  final val CONTAINS = "contains"
  final val INSTR = "strpos" // instr
  final val LENGTH = "char_length" // length
  final val LOWER = "lower"
  final val UPPER = "upper"
  final val LOCATE = "locate"
  final val LTRIM = "ltrim"
  final val RTRIM = "rtrim"
  final val TRIM = "trim"
  final val LPAD = "lpad"
  final val RPAD = "rpad"
  final val REPLACE = "replace"
  final val REVERSE = "reverse"
  final val SPLIT = "split"
  final val SUBSTRING = "substring"
  final val CONCAT_WS = "concat_ws"
  final val REPEAT = "repeat"
  final val TRANSLATE = "translate"
  final val SPACE = "space"

  // SparkSQL Math functions
  final val ABS = "abs"
  final val ACOSH = "acosh"
  final val ASINH = "asinh"
  final val ATANH = "atanh"
  final val CEIL = "ceil"
  final val FLOOR = "floor"
  final val EXP = "exp"
  final val POWER = "power"
  final val PMOD = "pmod"
  final val ROUND = "round"
  final val BROUND = "bround"
  final val SIN = "sin"
  final val SINH = "sinh"
  final val TAN = "tan"
  final val TANH = "tanh"
  final val BITWISE_NOT = "bitwise_not"
  final val BITWISE_AND = "bitwise_and"
  final val BITWISE_OR = "bitwise_or"
  final val BITWISE_XOR = "bitwise_xor"
  final val SHIFTLEFT = "shiftleft"
  final val SHIFTRIGHT = "shiftright"
  final val SQRT = "sqrt"
  final val CBRT = "cbrt"
  final val E = "e"
  final val PI = "pi"
  final val HEX = "hex"
  final val UNHEX = "unhex"
  final val HYPOT = "hypot"
  final val NameSigN = "NameSign"
  final val LOG1P = "log1p"
  final val LOG2 = "log2"
  final val LOG = "log"
  final val RADIANS = "radians"
  final val GREATEST = "greatest"
  final val LEAST = "least"
  final val REMAINDER = "modulus"
  final val FACTORIAL = "factorial"
  final val RAND = "rand"

  // PrestoSQL Math functions
  final val ACOS = "acos"
  final val ASIN = "asin"
  final val ATAN = "atan"
  final val ATAN2 = "atan2"
  final val COS = "cos"
  final val COSH = "cosh"
  final val DEGREES = "degrees"
  final val LOG10 = "log10"

  // SparkSQL DateTime functions
  // Fully supporting wait for https://github.com/ClickHouse/ClickHouse/pull/43818
  final val FROM_UNIXTIME = "from_unixtime"
  final val DATE_ADD = "date_add"
  final val DATE_SUB = "date_sub"
  final val DATE_DIFF = "datediff"
  final val TO_UNIX_TIMESTAMP = "to_unix_timestamp"
  final val UNIX_TIMESTAMP = "unix_timestamp"
  final val ADD_MONTHS = "add_months"
  final val DATE_FORMAT = "date_format"
  final val TRUNC = "trunc"
  final val GET_TIMESTAMP = "get_timestamp" // for function: to_date/to_timestamp
  final val TIMESTAMP_DIFF = "date_diff"


  // JSON functions
  final val GET_JSON_OBJECT = "get_json_object"
  final val JSON_ARRAY_LENGTH = "json_array_length"
  final val TO_JSON = "to_json"
  final val FROM_JSON = "from_json"
  final val JSON_TUPLE = "json_tuple"

  // Hash functions
  final val MURMUR3HASH = "murmur3hash"
  final val XXHASH64 = "xxhash64"
  final val MD5 = "md5"
  final val SHA1 = "sha1"
  final val SHA2 = "sha2"
  final val CRC32 = "crc32"

  // Array functions
  final val SIZE = "size"
  final val CREATE_ARRAY = "array"
  final val GET_ARRAY_ITEM = "get_array_item"
  final val ELEMENT_AT = "element_at"
  final val ARRAY_CONTAINS = "array_contains"
  final val ARRAY_MAX = "array_max"
  final val ARRAY_MIN = "array_min"
  final val SEQUENCE = "sequence"
  final val SORT_ARRAY = "sort_array"
  final val ARRAYS_OVERLAP = "arrays_overlap"
  final val SLICE = "slice"

  // Map functions
  final val CREATE_MAP = "map"
  final val GET_MAP_VALUE = "get_map_value"
  final val MAP_KEYS = "map_keys"
  final val MAP_VALUES = "map_values"
  final val MAP_FROM_ARRAYS = "map_from_arrays"

  // struct functions
  final val GET_STRUCT_FIELD = "get_struct_field"
  final val NAMED_STRUCT = "named_struct"

  // Spark 3.3
  final val SPLIT_PART = "split_part"
  final val MIGHT_CONTAIN = "might_contain"
  final val SEC = "sec"
  final val CSC = "csc"

  // Specific expression
  final val IF = "if"
  final val ATTRIBUTE_REFERENCE = "attribute_reference"
  final val BOUND_REFERENCE = "bound_reference"
  final val LITERAL = "literal"
  final val CASE_WHEN = "case_when"
  final val IN = "in"
  final val IN_SET = "in_set"
  final val SCALAR_SUBQUERY = "scalar_subquery"
  final val EXPLODE = "explode"
  final val POSEXPLODE = "posexplode"
  final val CHECK_OVERFLOW = "check_overflow"
  final val MAKE_DECIMAL = "make_decimal"
  final val PROMOTE_PRECISION = "promote_precision"

  // Directly use child expression transformer
  final val KNOWN_FLOATING_POINT_NORMALIZED = "known_floating_point_normalized"
  final val NORMALIZE_NANAND_ZERO = "normalize_nanand_zero"

  // Window functions used by Substrait plan.
  final val RANK = "rank"
  final val DENSE_RANK = "dense_rank"
  final val ROW_NUMBER = "row_number"
  final val CUME_DIST = "cume_dist"
  final val PERCENT_RANK = "percent_rank"
  final val NTILE = "ntile"
  final val LEAD = "lead"
  final val LAG = "lag"

  // Decimal functions
  final val UNSCALED_VALUE = "unscaled_value"

  /** Mapping Spark scalar expression to Substrait function name */
  // Aggregation functions used by Substrait plan.
  final val ARRAY_INTERSECT = "array_intersect"

  // Function names used by Substrait plan.
  final val EQUAL_NULL_SAFE = "equalnullsafe"
  final val BASE64 = "to_base64"
  final val ARRAY_JOIN = "array_join"
  final val ARRAY_SORT = "array_sort"
  final val ARRAY_ZIP = "zip"
  final val ARRAY_REPEAT = "array_repeat"
  final val ARRAY_ELEMENT_AT = "element_at"
  final val ARRAY_POSITION = "array_position"
  final val ARRAY_SLICE = "slice"
  final val ARRAY_DISTINCT = "array_distinct"
  final val BITMAP_COUNT_DISTINCT = "bitmap_count_distinct"

  /**
   * Mapping Spark scalar expression to Substrait function name
   */
  val SCALAR_SigS: Seq[NameSig] = Seq(
    NameSig[Add](ADD),
    NameSig[Asinh](ASINH),
    NameSig[Acosh](ACOSH),
    NameSig[Atanh](ATANH),
    NameSig[Subtract](SUBTRACT),
    NameSig[Multiply](MULTIPLY),
    NameSig[Divide](DIVIDE),
    NameSig[And](AND),
    NameSig[Or](OR),
    NameSig[Cast](CAST),
    NameSig[Coalesce](COALESCE),
    NameSig[Like](LIKE),
    NameSig[RLike](RLIKE),
    NameSig[RegExpReplace](REGEXP_REPLACE),
    NameSig[RegExpExtract](REGEXP_EXTRACT),
    NameSig[RegExpExtractAll](REGEXP_EXTRACT_ALL),
    NameSig[EqualTo](EQUAL),
    NameSig[LessThan](LESS_THAN),
    NameSig[LessThanOrEqual](LESS_THAN_OR_EQUAL),
    NameSig[GreaterThan](GREATER_THAN),
    NameSig[GreaterThanOrEqual](GREATER_THAN_OR_EQUAL),
    NameSig[Alias](ALIAS),
    NameSig[IsNotNull](IS_NOT_NULL),
    NameSig[IsNull](IS_NULL),
    NameSig[Not](NOT),
    NameSig[IsNaN](IS_NAN),
    // SparkSQL String functions
    NameSig[Ascii](ASCII),
    NameSig[Chr](CHR),
    NameSig[Extract](EXTRACT),
    NameSig[EndsWith](ENDS_WITH),
    NameSig[StartsWith](STARTS_WITH),
    NameSig[Concat](CONCAT),
    NameSig[Contains](CONTAINS),
    NameSig[StringInstr](INSTR),
    NameSig[Length](LENGTH),
    NameSig[Lower](LOWER),
    NameSig[Upper](UPPER),
    NameSig[StringLocate](LOCATE),
    NameSig[StringTrimLeft](LTRIM),
    NameSig[StringTrimRight](RTRIM),
    NameSig[StringTrim](TRIM),
    NameSig[StringLPad](LPAD),
    NameSig[StringRPad](RPAD),
    NameSig[StringReplace](REPLACE),
    NameSig[Reverse](REVERSE),
    NameSig[StringSplit](SPLIT),
    NameSig[Substring](SUBSTRING),
    NameSig[ConcatWs](CONCAT_WS),
    NameSig[StringRepeat](REPEAT),
    NameSig[StringTranslate](TRANSLATE),
    NameSig[StringSpace](SPACE),
    // SparkSQL Math functions
    NameSig[Abs](ABS),
    NameSig[Ceil](CEIL),
    NameSig[Floor](FLOOR),
    NameSig[Exp](EXP),
    NameSig[Pow](POWER),
    NameSig[Pmod](PMOD),
    NameSig[Round](ROUND),
    NameSig[BRound](BROUND),
    NameSig[Sin](SIN),
    NameSig[Sinh](SINH),
    NameSig[Tan](TAN),
    NameSig[Tanh](TANH),
    NameSig[BitwiseNot](BITWISE_NOT),
    NameSig[BitwiseAnd](BITWISE_AND),
    NameSig[BitwiseOr](BITWISE_OR),
    NameSig[BitwiseXor](BITWISE_XOR),
    NameSig[ShiftLeft](SHIFTLEFT),
    NameSig[ShiftRight](SHIFTRIGHT),
    NameSig[Sqrt](SQRT),
    NameSig[Cbrt](CBRT),
    NameSig[EulerNumber](E),
    NameSig[Pi](PI),
    NameSig[Hex](HEX),
    NameSig[Unhex](UNHEX),
    NameSig[Hypot](HYPOT),
    NameSig[Log1p](LOG1P),
    NameSig[Log2](LOG2),
    NameSig[Log](LOG),
    NameSig[ToRadians](RADIANS),
    NameSig[Greatest](GREATEST),
    NameSig[Least](LEAST),
    NameSig[Remainder](REMAINDER),
    NameSig[Factorial](FACTORIAL),
    NameSig[Rand](RAND),
    // PrestoSQL Math functions
    NameSig[Acos](ACOS),
    NameSig[Asin](ASIN),
    NameSig[Atan](ATAN),
    NameSig[Atan2](ATAN2),
    NameSig[Cos](COS),
    NameSig[Cosh](COSH),
    NameSig[Log10](LOG10),
    NameSig[ToDegrees](DEGREES),
    // SparkSQL DateTime functions
    NameSig[Year](EXTRACT),
    NameSig[YearOfWeek](EXTRACT),
    NameSig[Quarter](EXTRACT),
    NameSig[Month](EXTRACT),
    NameSig[WeekOfYear](EXTRACT),
    NameSig[WeekDay](EXTRACT),
    NameSig[DayOfWeek](EXTRACT),
    NameSig[DayOfMonth](EXTRACT),
    NameSig[DayOfYear](EXTRACT),
    NameSig[Hour](EXTRACT),
    NameSig[Minute](EXTRACT),
    NameSig[Second](EXTRACT),
    NameSig[FromUnixTime](FROM_UNIXTIME),
    NameSig[DateAdd](DATE_ADD),
    NameSig[DateSub](DATE_SUB),
    NameSig[DateDiff](DATE_DIFF),
    NameSig[ToUnixTimestamp](TO_UNIX_TIMESTAMP),
    NameSig[UnixTimestamp](UNIX_TIMESTAMP),
    NameSig[AddMonths](ADD_MONTHS),
    NameSig[DateFormatClass](DATE_FORMAT),
    NameSig[TruncDate](TRUNC),
    NameSig[GetTimestamp](GET_TIMESTAMP),
    // JSON functions
    NameSig[GetJsonObject](GET_JSON_OBJECT),
    NameSig[LengthOfJsonArray](JSON_ARRAY_LENGTH),
    NameSig[StructsToJson](TO_JSON),
    NameSig[JsonToStructs](FROM_JSON),
    NameSig[JsonTuple](JSON_TUPLE),
    // Hash functions
    NameSig[Murmur3Hash](MURMUR3HASH),
    NameSig[XxHash64](XXHASH64),
    NameSig[Md5](MD5),
    NameSig[Sha1](SHA1),
    NameSig[Sha2](SHA2),
    NameSig[Crc32](CRC32),
    // Array functions
    NameSig[Size](SIZE),
    NameSig[CreateArray](CREATE_ARRAY),
    NameSig[Explode](EXPLODE),
    NameSig[PosExplode](POSEXPLODE),
    NameSig[GetArrayItem](GET_ARRAY_ITEM),
    NameSig[ElementAt](ELEMENT_AT),
    NameSig[ArrayContains](ARRAY_CONTAINS),
    NameSig[ArrayMax](ARRAY_MAX),
    NameSig[ArrayMin](ARRAY_MIN),
    NameSig[Sequence](SEQUENCE),
    NameSig[SortArray](SORT_ARRAY),
    NameSig[ArraysOverlap](ARRAYS_OVERLAP),
    NameSig[Slice](SLICE),
    // Map functions
    NameSig[CreateMap](CREATE_MAP),
    NameSig[GetMapValue](GET_MAP_VALUE),
    NameSig[MapKeys](MAP_KEYS),
    NameSig[MapValues](MAP_VALUES),
    NameSig[MapFromArrays](MAP_FROM_ARRAYS),
    // Struct functions
    NameSig[GetStructField](GET_STRUCT_FIELD),
    NameSig[CreateNamedStruct](NAMED_STRUCT),
    // Directly use child expression transformer
    NameSig[KnownFloatingPointNormalized](KNOWN_FLOATING_POINT_NORMALIZED),
    NameSig[NormalizeNaNAndZero](NORMALIZE_NANAND_ZERO),
    // Specific expression
    NameSig[If](IF),
    NameSig[AttributeReference](ATTRIBUTE_REFERENCE),
    NameSig[BoundReference](BOUND_REFERENCE),
    NameSig[Literal](LITERAL),
    NameSig[CaseWhen](CASE_WHEN),
    NameSig[In](IN),
    NameSig[InSet](IN_SET),
    NameSig[ScalarSubquery](SCALAR_SUBQUERY),
    NameSig[CheckOverflow](CHECK_OVERFLOW),
    NameSig[MakeDecimal](MAKE_DECIMAL),
    NameSig[PromotePrecision](PROMOTE_PRECISION),
    // Decimal
    NameSig[UnscaledValue](UNSCALED_VALUE),
    NameSig[PromotePrecision](PROMOTE_PRECISION),
    NameSig[Base64](BASE64),
    NameSig[TimestampDiff](TIMESTAMP_DIFF),
    NameSig[org.apache.spark.sql.execution.columnar.expressions.DateDiff](TIMESTAMP_DIFF),
    NameSig[ArrayJoin](ARRAY_JOIN),
    NameSig[ArraySort](ARRAY_SORT),
    NameSig[ArrayRepeat](ARRAY_REPEAT),
    NameSig[ArrayDistinct](ARRAY_DISTINCT),
    NameSig[ElementAt](ARRAY_ELEMENT_AT),
    NameSig[ArrayPosition](ARRAY_POSITION),
    NameSig[ArrayPositionWithInstance](ARRAY_POSITION),
    NameSig[Slice](ARRAY_SLICE))

  /** Mapping Spark aggregation expression to Substrait function name */
  private val AGGREGATE_SigS: Seq[NameSig] = Seq(
    NameSig[Sum](SUM),
    NameSig[Average](AVG),
    NameSig[Count](COUNT),
    NameSig[Min](MIN),
    NameSig[Max](MAX),
    NameSig[StddevSamp](STDDEV_SAMP),
    NameSig[StddevPop](STDDEV_POP),
    NameSig[CollectList](COLLECT_LIST),
    NameSig[VarianceSamp](VAR_SAMP),
    NameSig[VariancePop](VAR_POP),
    NameSig[BitAndAgg](BIT_AND_AGG),
    NameSig[BitOrAgg](BIT_OR_AGG),
    NameSig[BitXorAgg](BIT_XOR_AGG),
    NameSig[Corr](CORR),
    NameSig[CovPopulation](COVAR_POP),
    NameSig[CovSample](COVAR_SAMP),
    NameSig[Last](LAST),
    NameSig[First](FIRST),
    NameSig[CollectList](COLLECT_LIST),
    NameSig[ArrayIntersect](ARRAY_INTERSECT),
    NameSig[EqualNullSafe](EQUAL_NULL_SAFE),
    NameSig[BitmapCountDistinctAggFunction](BITMAP_COUNT_DISTINCT),
  )

  /** Mapping Spark window expression to Substrait function name */
  private val WINDOW_SigS: Seq[NameSig] = Seq(
    NameSig[Rank](RANK),
    NameSig[DenseRank](DENSE_RANK),
    NameSig[RowNumber](ROW_NUMBER),
    NameSig[CumeDist](CUME_DIST),
    NameSig[PercentRank](PERCENT_RANK),
    NameSig[NTile](NTILE),
    NameSig[Lead](LEAD),
    NameSig[Lag](LAG))

  def expressionsMap: Map[Class[_], String] =
    defaultExpressionsMap

  private lazy val defaultExpressionsMap: Map[Class[_], String] = {
    (SCALAR_SigS ++ AGGREGATE_SigS ++ WINDOW_SigS)
      .map(s => (s.expClass, s.nativeName))
      .toMap[Class[_], String]
  }

}
