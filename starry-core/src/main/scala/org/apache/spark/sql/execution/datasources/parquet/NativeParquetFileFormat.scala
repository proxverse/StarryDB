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

package org.apache.spark.sql.execution.datasources.parquet

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.execution.{StarryConf, WholeStageCodegenExec}
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.vectorized.{OffHeapColumnVector, OnHeapColumnVector}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.util.SerializableConfiguration
import com.google.common.cache.{Cache, CacheBuilder}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.mapred.FileSplit
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.parquet.filter2.compat.FilterCompat
import org.apache.parquet.filter2.predicate.FilterApi
import org.apache.parquet.format.converter.ParquetMetadataConverter.{
  MetadataFilter,
  SKIP_ROW_GROUPS
}
import org.apache.parquet.hadoop._
import org.apache.parquet.hadoop.ParquetOutputFormat.JobSummaryLevel
import org.apache.parquet.hadoop.codec.CodecConfig
import org.apache.parquet.hadoop.metadata.{FileMetaData, ParquetMetadata}
import org.apache.parquet.hadoop.util.ContextUtil

import java.net.URI
import java.util.concurrent.TimeUnit

class NativeParquetFileFormat
    extends FileFormat
    with DataSourceRegister
    with Logging
    with Serializable {
  // Hold a reference to the (serializable) singleton instance of ParquetLogRedirector. This
  // ensures the ParquetLogRedirector class is initialized whether an instance of ParquetFileFormat
  // is constructed or deserialized. Do not heed the Scala compiler's warning about an unused field
  // here.
  private val parquetLogRedirector = ParquetLogRedirector.INSTANCE

  override def shortName(): String = "native parquet"

  override def toString: String = "NativeParquet"

  override def hashCode(): Int = getClass.hashCode()

  override def equals(other: Any): Boolean = other.isInstanceOf[NativeParquetFileFormat]

  override def prepareWrite(
      sparkSession: SparkSession,
      job: Job,
      options: Map[String, String],
      dataSchema: StructType): OutputWriterFactory = {
    val parquetOptions = new ParquetOptions(options, sparkSession.sessionState.conf)

    val conf = ContextUtil.getConfiguration(job)

    val committerClass =
      conf.getClass(
        SQLConf.PARQUET_OUTPUT_COMMITTER_CLASS.key,
        classOf[ParquetOutputCommitter],
        classOf[OutputCommitter])

    if (conf.get(SQLConf.PARQUET_OUTPUT_COMMITTER_CLASS.key) == null) {
      logInfo(
        "Using default output committer for Parquet: " +
          classOf[ParquetOutputCommitter].getCanonicalName)
    } else {
      logInfo(
        "Using user defined output committer for Parquet: " + committerClass.getCanonicalName)
    }

    conf.setClass(SQLConf.OUTPUT_COMMITTER_CLASS.key, committerClass, classOf[OutputCommitter])

    // We're not really using `ParquetOutputFormat[Row]` for writing data here, because we override
    // it in `ParquetOutputWriter` to support appending and dynamic partitioning.  The reason why
    // we set it here is to setup the output committer class to `ParquetOutputCommitter`, which is
    // bundled with `ParquetOutputFormat[Row]`.
    job.setOutputFormatClass(classOf[ParquetOutputFormat[Row]])

    ParquetOutputFormat.setWriteSupportClass(job, classOf[ParquetWriteSupport])

    // This metadata is useful for keeping UDTs like Vector/Matrix.
    ParquetWriteSupport.setSchema(dataSchema, conf)

    // Sets flags for `ParquetWriteSupport`, which converts Catalyst schema to Parquet
    // schema and writes actual rows to Parquet files.
    conf.set(
      SQLConf.PARQUET_WRITE_LEGACY_FORMAT.key,
      sparkSession.sessionState.conf.writeLegacyParquetFormat.toString)

    conf.set(
      SQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE.key,
      sparkSession.sessionState.conf.parquetOutputTimestampType.toString)

    conf.set(
      SQLConf.PARQUET_FIELD_ID_WRITE_ENABLED.key,
      sparkSession.sessionState.conf.parquetFieldIdWriteEnabled.toString)

    // Sets compression scheme
    conf.set(ParquetOutputFormat.COMPRESSION, parquetOptions.compressionCodecClassName)

    // SPARK-15719: Disables writing Parquet summary files by default.
    if (conf.get(ParquetOutputFormat.JOB_SUMMARY_LEVEL) == null
        && conf.get(ParquetOutputFormat.ENABLE_JOB_SUMMARY) == null) {
      conf.setEnum(ParquetOutputFormat.JOB_SUMMARY_LEVEL, JobSummaryLevel.NONE)
    }

    if (ParquetOutputFormat.getJobSummaryLevel(conf) != JobSummaryLevel.NONE
        && !classOf[ParquetOutputCommitter].isAssignableFrom(committerClass)) {
      // output summary is requested, but the class is not a Parquet Committer
      logWarning(
        s"Committer $committerClass is not a ParquetOutputCommitter and cannot" +
          s" create job summaries. " +
          s"Set Parquet option ${ParquetOutputFormat.JOB_SUMMARY_LEVEL} to NONE.")
    }

    new OutputWriterFactory {
      // This OutputWriterFactory instance is deserialized when writing Parquet files on the
      // executor side without constructing or deserializing ParquetFileFormat. Therefore, we hold
      // another reference to ParquetLogRedirector.INSTANCE here to ensure the latter class is
      // initialized.
      private val parquetLogRedirector = ParquetLogRedirector.INSTANCE

      override def newInstance(
          path: String,
          dataSchema: StructType,
          context: TaskAttemptContext): OutputWriter = {
        new ParquetOutputWriter(path, context)
      }

      override def getFileExtension(context: TaskAttemptContext): String = {
        CodecConfig.from(context).getCodec.getExtension + ".parquet"
      }
    }
  }

  override def inferSchema(
      sparkSession: SparkSession,
      parameters: Map[String, String],
      files: Seq[FileStatus]): Option[StructType] = {
    ParquetUtils.inferSchema(sparkSession, parameters, files)
  }

  /** Returns whether the reader will return the rows as batch or not. */
  override def supportBatch(sparkSession: SparkSession, schema: StructType): Boolean = {
    val conf = sparkSession.sessionState.conf
    (ParquetUtils.isBatchReadSupportedForSchema(conf, schema) && conf.wholeStageEnabled &&
    !WholeStageCodegenExec.isTooManyFields(conf, schema)) && canSupport(schema)
  }

  override def vectorTypes(
      requiredSchema: StructType,
      partitionSchema: StructType,
      sqlConf: SQLConf): Option[Seq[String]] = {
    Option(
      Seq.fill(requiredSchema.fields.length + partitionSchema.fields.length)(
        if (!sqlConf.offHeapColumnVectorEnabled) {
          classOf[OnHeapColumnVector].getName
        } else {
          classOf[OffHeapColumnVector].getName
        }))
  }

  override def isSplitable(
      sparkSession: SparkSession,
      options: Map[String, String],
      path: Path): Boolean = {
    true
  }

  override def buildReaderWithPartitionValues(
      sparkSession: SparkSession,
      dataSchema: StructType,
      partitionSchema: StructType,
      requiredSchema: StructType,
      filters: Seq[Filter],
      options: Map[String, String],
      hadoopConf: Configuration): (PartitionedFile) => Iterator[InternalRow] = {
    hadoopConf.set(ParquetInputFormat.READ_SUPPORT_CLASS, classOf[ParquetReadSupport].getName)
    hadoopConf.set(ParquetReadSupport.SPARK_ROW_REQUESTED_SCHEMA, requiredSchema.json)
    hadoopConf.set(ParquetWriteSupport.SPARK_ROW_SCHEMA, requiredSchema.json)
    hadoopConf.set(
      SQLConf.SESSION_LOCAL_TIMEZONE.key,
      sparkSession.sessionState.conf.sessionLocalTimeZone)
    hadoopConf.setBoolean(
      SQLConf.NESTED_SCHEMA_PRUNING_ENABLED.key,
      sparkSession.sessionState.conf.nestedSchemaPruningEnabled)
    hadoopConf.setBoolean(
      SQLConf.CASE_SENSITIVE.key,
      sparkSession.sessionState.conf.caseSensitiveAnalysis)

    // Sets flags for `ParquetToSparkSchemaConverter`
    hadoopConf.setBoolean(
      SQLConf.PARQUET_BINARY_AS_STRING.key,
      sparkSession.sessionState.conf.isParquetBinaryAsString)
    hadoopConf.setBoolean(
      SQLConf.PARQUET_INT96_AS_TIMESTAMP.key,
      sparkSession.sessionState.conf.isParquetINT96AsTimestamp)

    val broadcastedHadoopConf =
      sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))

    // TODO: if you move this into the closure it reverts to the default values.
    // If true, enable using the custom RecordReader for parquet. This only works for
    // a subset of the types (no complex types).
    val resultSchema = StructType(partitionSchema.fields ++ requiredSchema.fields)
    val sqlConf = sparkSession.sessionState.conf
    val enableOffHeapColumnVector = sqlConf.offHeapColumnVectorEnabled
    val enableVectorizedReader: Boolean =
      ParquetUtils.isBatchReadSupportedForSchema(sqlConf, resultSchema)
    val enableRecordFilter: Boolean = sqlConf.parquetRecordFilterEnabled
    val timestampConversion: Boolean = sqlConf.isParquetINT96TimestampConversion
    val capacity = sqlConf.parquetVectorizedReaderBatchSize
    val enableParquetFilterPushDown: Boolean = sqlConf.parquetFilterPushDown
    // Whole stage codegen (PhysicalRDD) is able to deal with batches directly
    val returningBatch = supportBatch(sparkSession, resultSchema)
    val pushDownDate = sqlConf.parquetFilterPushDownDate
    val pushDownTimestamp = sqlConf.parquetFilterPushDownTimestamp
    val pushDownDecimal = sqlConf.parquetFilterPushDownDecimal
    val pushDownStringStartWith = sqlConf.parquetFilterPushDownStringStartWith
    val pushDownInFilterThreshold = sqlConf.parquetFilterPushDownInFilterThreshold
    val isCaseSensitive = sqlConf.caseSensitiveAnalysis
    val parquetOptions = new ParquetOptions(options, sparkSession.sessionState.conf)
    val datetimeRebaseModeInRead = parquetOptions.datetimeRebaseModeInRead
    val int96RebaseModeInRead = parquetOptions.int96RebaseModeInRead

    (file: PartitionedFile) =>
      {
        assert(file.partitionValues.numFields == partitionSchema.size)

        val filePath = new Path(new URI(file.filePath))
        val split = new FileSplit(filePath, file.start, file.length, Array.empty[String])

        val sharedConf = broadcastedHadoopConf.value.value

        lazy val footerFileMetaData =
          FooterCache.getOrCreate(filePath, sharedConf, SKIP_ROW_GROUPS)
        val datetimeRebaseSpec = DataSourceUtils.datetimeRebaseSpec(
          footerFileMetaData.getKeyValueMetaData.get,
          datetimeRebaseModeInRead)
        // Try to push down filters when filter push-down is enabled.
        val pushed = if (enableParquetFilterPushDown) {
          val parquetSchema = footerFileMetaData.getSchema
          val parquetFilters = new ParquetFilters(
            parquetSchema,
            pushDownDate,
            pushDownTimestamp,
            pushDownDecimal,
            pushDownStringStartWith,
            pushDownInFilterThreshold,
            isCaseSensitive,
            datetimeRebaseSpec)
          filters
          // Collects all converted Parquet filter predicates. Notice that not all predicates can be
          // converted (`ParquetFilters.createFilter` returns an `Option`). That's why a `flatMap`
          // is used here.
            .flatMap(parquetFilters.createFilter(_))
            .reduceOption(FilterApi.and)
        } else {
          None
        }

        // PARQUET_INT96_TIMESTAMP_CONVERSION says to apply timezone conversions to int96 timestamps'
        // *only* if the file was created by something other than "parquet-mr", so check the actual
        // writer here for this file.  We have to do this per-file, as each file in the table may
        // have different writers.
        // Define isCreatedByParquetMr as function to avoid unnecessary parquet footer reads.
        def isCreatedByParquetMr: Boolean =
          footerFileMetaData.getCreatedBy().startsWith("parquet-mr")

        val convertTz =
          if (timestampConversion && !isCreatedByParquetMr) {
            Some(DateTimeUtils.getZoneId(sharedConf.get(SQLConf.SESSION_LOCAL_TIMEZONE.key)))
          } else {
            None
          }

        val int96RebaseSpec = DataSourceUtils.int96RebaseSpec(
          footerFileMetaData.getKeyValueMetaData.get,
          int96RebaseModeInRead)

        val attemptId = new TaskAttemptID(new TaskID(new JobID(), TaskType.MAP, 0), 0)
        val hadoopAttemptContext =
          new TaskAttemptContextImpl(broadcastedHadoopConf.value.value, attemptId)

        // Try to push down filters when filter push-down is enabled.
        // Notice: This push-down is RowGroups level, not individual records.
        if (pushed.isDefined) {
          ParquetInputFormat.setFilterPredicate(hadoopAttemptContext.getConfiguration, pushed.get)
        }
        val taskContext = Option(TaskContext.get())
        if (enableVectorizedReader && canSupport(requiredSchema)) {
          val vectorizedReader = if (StarryConf.nativeParquetReaderEnabled) {
            new NativeVectorizedParquetRecordReader2(
              convertTz.orNull,
              datetimeRebaseSpec.mode.toString,
              datetimeRebaseSpec.timeZone,
              int96RebaseSpec.mode.toString,
              int96RebaseSpec.timeZone,
              enableOffHeapColumnVector && taskContext.isDefined,
              capacity)
          } else {
            new VectorizedParquetRecordReader(
              convertTz.orNull,
              datetimeRebaseSpec.mode.toString,
              datetimeRebaseSpec.timeZone,
              int96RebaseSpec.mode.toString,
              int96RebaseSpec.timeZone,
              enableOffHeapColumnVector && taskContext.isDefined,
              capacity)
          }
          // SPARK-37089: We cannot register a task completion listener to close this iterator here
          // because downstream exec nodes have already registered their listeners. Since listeners
          // are executed in reverse order of registration, a listener registered here would close the
          // iterator while downstream exec nodes are still running. When off-heap column vectors are
          // enabled, this can cause a use-after-free bug leading to a segfault.
          //
          // Instead, we use FileScanRDD's task completion listener to close this iterator.
          var iter = if (StarryConf.asyncNativeParquetReaderEnabled) {
            new AsyncRecordReaderIterator(vectorizedReader)
          } else {
            new RecordReaderIterator(vectorizedReader)
          }
          try {
            vectorizedReader.initialize(split, hadoopAttemptContext)
            logDebug(s"Appending $partitionSchema ${file.partitionValues}")
            if (StarryConf.nativeParquetReaderEnabled) {
              vectorizedReader
                .asInstanceOf[NativeVectorizedParquetRecordReader2]
                .initBatch(partitionSchema, file.partitionValues)
              if (returningBatch) {
                vectorizedReader
                  .asInstanceOf[NativeVectorizedParquetRecordReader2]
                  .enableReturningBatches()
              }
            } else {
              vectorizedReader
                .asInstanceOf[VectorizedParquetRecordReader]
                .initBatch(partitionSchema, file.partitionValues)
              if (returningBatch) {
                vectorizedReader
                  .asInstanceOf[VectorizedParquetRecordReader]
                  .enableReturningBatches()
              }
            }

            // UnsafeRowParquetRecordReader appends the columns internally to avoid another copy.
            iter.asInstanceOf[Iterator[InternalRow]]
          } catch {
            case e: Throwable =>
              // SPARK-23457: In case there is an exception in initialization, close the iterator to
              // avoid leaking resources.
              iter.close()
              throw e
          }
        } else {
          logDebug(s"Falling back to parquet-mr")
          // ParquetRecordReader returns InternalRow
          val readSupport = new ParquetReadSupport(
            convertTz,
            enableVectorizedReader = false,
            datetimeRebaseSpec,
            int96RebaseSpec)
          val reader = if (pushed.isDefined && enableRecordFilter) {
            val parquetFilter = FilterCompat.get(pushed.get, null)
            new ParquetRecordReader[InternalRow](readSupport, parquetFilter)
          } else {
            new ParquetRecordReader[InternalRow](readSupport)
          }
          val iter = new RecordReaderIterator[InternalRow](reader)
          try {
            reader.initialize(split, hadoopAttemptContext)

            val fullSchema = requiredSchema.toAttributes ++ partitionSchema.toAttributes
            val unsafeProjection = GenerateUnsafeProjection.generate(fullSchema, fullSchema)

            if (partitionSchema.length == 0) {
              // There is no partition columns
              iter.map(unsafeProjection)
            } else {
              val joinedRow = new JoinedRow()
              iter.map(d => unsafeProjection(joinedRow(d, file.partitionValues)))
            }
          } catch {
            case e: Throwable =>
              // SPARK-23457: In case there is an exception in initialization, close the iterator to
              // avoid leaking resources.
              iter.close()
              throw e
          }
        }
      }
  }

  override def supportDataType(dataType: DataType): Boolean = dataType match {
    case _: AtomicType => true

    case st: StructType => st.forall(f => supportDataType(f.dataType))

    case ArrayType(elementType, _) => supportDataType(elementType)

    case MapType(keyType, valueType, _) =>
      supportDataType(keyType) && supportDataType(valueType)

    case udt: UserDefinedType[_] => supportDataType(udt.sqlType)

    case _ => false
  }
  def canSupport(structType: StructType): Boolean = {
    var result = true
    def checkData(dataType: DataType): Unit = dataType match {
      case StructType(fields) =>
        result = false
      case ArrayType(tp, _) if !tp.isInstanceOf[AtomicType] =>
        result = false
      case MapType(keyType, valueType, _) =>
        result = false
      case _: TimestampNTZType =>
        result = false
      case _: DecimalType =>
        result = false
      case _ =>
    }
    structType.fields.map(_.dataType).foreach(checkData)
    result
  }

}

object FooterCache {

  val footerCache: Cache[String, FileMetaData] = CacheBuilder.newBuilder
    .maximumSize(100)
    .expireAfterAccess(24, TimeUnit.HOURS)
    .build[String, FileMetaData]()

  def getOrCreate(path: Path, conf: Configuration, filter: MetadataFilter): FileMetaData = {
    val key = path.toString
    val cacheResult = footerCache.getIfPresent(key)
    if (cacheResult != null) {
      cacheResult
    } else {
      val data = ParquetFooterReader.readFooter(conf, path, filter).getFileMetaData
      footerCache.put(key, data)
      data
    }
  }
}
