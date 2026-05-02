// Databricks notebook source
// MAGIC %md 
// MAGIC
// MAGIC ## Description
// MAGIC
// MAGIC Version which supports Qlik encrypted data (Raw layer message payload stored as AVRO bytes - pii columns encrypted using Qlik encryption mechanism 2)
// MAGIC
// MAGIC ### Parameters:
// MAGIC
// MAGIC 1) `maxBytesPerTrigger` - limit how many bytes to read per trigger (128 mb as initial value)
// MAGIC
// MAGIC 2) `schedulerPoolPartitionNum` - coalesce(schedulerPoolPartitionNum) for each `schedulerPool`
// MAGIC
// MAGIC 3) `schedulerPools` - how many jobs to run in parallel (how many tables will be scheduled at the same time)
// MAGIC
// MAGIC ### `schedulerPoolPartitionNum` vs `schedulerPools`
// MAGIC
// MAGIC Parameters introduced to parallelize work inside of foreachBatch, where jobs are scheduled based on `table names` available in current batch.
// MAGIC
// MAGIC Result of this multiplication `schedulerPools * schedulerPoolPartitionNum` should be equal to `number of cores` in cluster.
// MAGIC
// MAGIC One can try with different values (including `maxBytesPerTrigger`). 
// MAGIC
// MAGIC To see how cluster is utilized, go to 1. `compute tab` (or go to the `jobs tab`), 2. cluster (or job), 3. `metrics tab` 4. `ganglia UI`. 4. Review cluster memory and cluster cpu. 
// MAGIC
// MAGIC We might need have a cluster with higher number of cores (rather smaller amount of memory).

// COMMAND ----------

// MAGIC %md
// MAGIC ##ChangeLog
// MAGIC - Changes due mergeIssues 
// MAGIC   - 01 - Changes in parallel processing - where jobs are scheduled based on table names instead of schema ids available in current batch 
// MAGIC   - 02 - Addition of Timestamp to fetch data from Raw layer
// MAGIC   - 03 - New methods to handle, in the case of mergeOnAllColumns=true 
// MAGIC   - 04 - Process DQ records 
// MAGIC   - 05 - fetched timestamp from CDC metadata to help in ordering of cdc changes(ordering done based on changesequence and timestamp)
// MAGIC - Changes with to Hyperscale merge [2022-06-28]  
// MAGIC   - 01 - Adding UUID
// MAGIC   - 02 - DQ process based on tableName instead of messageSchemaId
// MAGIC   - 03 - Adding runtime debug log (will add runtime log entries in the case of debug=true)
// MAGIC   - 04 - Functionality to Stop Stream Gracefully
// MAGIC - Changes due to Hash collisions & Some other minor changes [2022-09-01]  
// MAGIC   - 01 - Change Hash funcation to SHA2-512
// MAGIC   - 02 - Introduce default values inthe case of null - only for hash caculations, no any data alternate
// MAGIC   - 03 - New config notebook which contains list of tables and cdc processing start time of each table, using this notebook we can addd new tables in to same job in the future if required 
// MAGIC - Changes to bring RRN as PK [2023-06-26]
// MAGIC   - 01 - Remove all DQ menthods
// MAGIC   - 02 - Remove all Hash logic
// MAGIC   - 03 - Add RRN logic for merging
// MAGIC   - 04 - Remove trimming of values
// MAGIC - Optimization realted Changes [2023-10-24]
// MAGIC   - 01 - table structure changes , Flatten strcut and keep PK at begining of the tables
// MAGIC   - 03 - Split of CDC table list config notebook into two notebooks, one to control raw to  Strcuctured and one to control for Strcuctured to Db
// MAGIC   - 04 - Two difeernt notebooks and location for debug events
// MAGIC   - 05 - Add UDP counts funcatinality (for now it's only raw counts)
// MAGIC - Data Validity - Include Trim [2024-04-05]
// MAGIC   - 01 - Add Trimming Value
// MAGIC

// COMMAND ----------

// MAGIC %md
// MAGIC ## Helper Notebooks

// COMMAND ----------

// MAGIC %run ../_Shared/Config/cdcTableListConfigsStructured

// COMMAND ----------

// MAGIC %run ../_Shared/Metadata/EvaluateDataTableMetadata

// COMMAND ----------

// MAGIC %run ../_Shared/ColumnMappings/EncryptionColumnMappings

// COMMAND ----------

// MAGIC %run ../_Shared/PII_Columns/loadPiiSourceDataTypes

// COMMAND ----------

// MAGIC %run ../_Shared/Config/SessionConfigs

// COMMAND ----------

// MAGIC %run ../_Shared/logging/debugLogHelpersCdcMergingStructured

// COMMAND ----------

// MAGIC %md
// MAGIC ## Params & libs

// COMMAND ----------

// DBTITLE 1,Import Encryption Recursion Logic
import Recurse_v3_Encrypt_v2._

// COMMAND ----------

import io.delta.tables.DeltaTable
import org.apache.spark.sql.avro.functions._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import scala.collection.mutable.WrappedArray
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.functions.upper
import scala.collection.mutable.ArrayBuffer
import java.time.LocalTime._
import java.time.LocalDateTime._
import java.time.format.DateTimeFormatter
import collection.JavaConverters._
import scala.util.Try

// COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text("secretScope","adb-kv-adls")
dbutils.widgets.text("rawFileSystem","raw")
dbutils.widgets.text("structuredFileSystem","structured")
dbutils.widgets.text("storageAccountKey","pbtb-st-name")
dbutils.widgets.text("maxBytesPerTrigger", "")
dbutils.widgets.text("schedulerPools", "")
dbutils.widgets.text("schedulerPoolPartitionNum", "")
dbutils.widgets.text("defaultCdcProcessingStartTs", "2023-08-25 00:00:00") 
dbutils.widgets.text("triggerIntervalMinutes", "")
dbutils.widgets.text("logLevel", "")
dbutils.widgets.text("autoTerminateFlag", "")
dbutils.widgets.text("streamStopTriggerUTCtime","")
dbutils.widgets.text("priorityOfTableList","")
dbutils.widgets.dropdown("fromAvroParseMode","FAILFAST", Seq("PERMISSIVE", "FAILFAST"))

// COMMAND ----------

import scala.util.Try

val systemName = "IL"
val secretScope = dbutils.widgets.get("secretScope")
val rawFileSystem = dbutils.widgets.get("rawFileSystem")
val structuredFileSystem = dbutils.widgets.get("structuredFileSystem")
val storageAccountKey = dbutils.widgets.get("storageAccountKey")
val storageAccountName = dbutils.secrets.get(scope = secretScope, key = storageAccountKey)
val maxBytesPerTrigger = Try(dbutils.widgets.get("maxBytesPerTrigger").toLong).getOrElse(268435456L)
val schedulerPools = Try(dbutils.widgets.get("schedulerPools").toInt).getOrElse(4)
val schedulerPoolPartitionNum = Try(dbutils.widgets.get("schedulerPoolPartitionNum").toInt).getOrElse(8)
val triggerIntervalMinutes = Try(dbutils.widgets.get("triggerIntervalMinutes").toInt).getOrElse(5)
val logLevel = dbutils.widgets.get("logLevel")
val priorityOfTableList = dbutils.widgets.get("priorityOfTableList").trim

val rawZonePrefix = "abfss://" + rawFileSystem + "@" + storageAccountName + ".dfs.core.windows.net"
val structuredZonePrefix = "abfss://" + structuredFileSystem + "@" + storageAccountName + ".dfs.core.windows.net"

val dataTableRawPath = s"${rawZonePrefix}/${systemName}/cdc/data/"
val dataStructuredCheckpointPath = s"${structuredZonePrefix}/${systemName}/tables/_checkpoints/${priorityOfTableList}"  
def GetDataTableStructuredPath(tableSchema:String, tableName:String) = s"${structuredZonePrefix}/${systemName}/tables/${tableSchema}/${tableName}" 
val defaultCdcProcessingStartTs = dbutils.widgets.get("defaultCdcProcessingStartTs")
val streamingQueryName = s"${systemName}_StreamRawTableDataToDelta_${priorityOfTableList}"

val autoTerminateFlag = Try(dbutils.widgets.get("autoTerminateFlag").toBoolean).getOrElse(false)
val streamStopTriggerUTCtime = Try(java.time.LocalTime.parse(dbutils.widgets.get("streamStopTriggerUTCtime"))).getOrElse(java.time.LocalTime.parse("15:00:00"))
val fromAvroParseMode = dbutils.widgets.get("fromAvroParseMode")
val fromAvroOptions = Map("mode" -> s"$fromAvroParseMode").asJava


// COMMAND ----------

// MAGIC %md
// MAGIC ## Helper Methods

// COMMAND ----------

if(priorityOfTableList == "") {
  throw new RuntimeException("Invalid value for priorityOfTableList, please provide valid value for param : priorityOfTableList")
} else {
  if (!cdcTableList.get(priorityOfTableList).isDefined) {
    throw new RuntimeException("Invalid value for priorityOfTableList, can not find provided priority value in ../_Shared/Config/cdcTableListConfigs, please provide a valid value for param : priorityOfTableList")
  }
}

// COMMAND ----------

val dtimeformatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
Try(java.time.LocalDateTime.parse(defaultCdcProcessingStartTs,dtimeformatter)).getOrElse(throw new RuntimeException("Specify valid defaultCdcProcessingStartTs parameter in yyyy-MM-dd HH:mm:ss format !!!"))

// COMMAND ----------

val tableList = cdcTableList.get(priorityOfTableList).get.map{case(k,v) => k.toUpperCase -> v}
val tableNames = tableList.keySet.map(_.toUpperCase)

tableList.foreach {
  case(tab, ts) => {
    Try(java.time.LocalDateTime.parse(ts,dtimeformatter)).getOrElse(throw new RuntimeException(s"Specify a valid cdcProcessingStartTs in yyyy-MM-dd HH:mm:ss format at ../_Shared/Config/cdcTableListConfigs for the table ${tab} !!!"))
  }
}

def getTableLevelCdcProciessingStartTs(table: String, schema : String) = tableList.get((schema+"."+table).toUpperCase).get
tableNames.size

// COMMAND ----------

spark.conf.set("spark.sql.optimizer.maxIterations",1000000)
spark.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true")
spark.sql("SET spark.databricks.delta.properties.defaults.enableChangeDataFeed = true")
spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite",true)
spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.autoCompact",true)
spark.conf.set("spark.sql.shuffle.partitions","auto")

// COMMAND ----------

def getStrcutredLayerSchema(tableSchema: String,tableName: String) =  Try(spark.read.load(GetDataTableStructuredPath(tableSchema,tableName)).schema).getOrElse(new StructType())

def convertToIBMColumnHandleDataTypeMismatch(tableSchema: String, tableName: String, columns: Seq[ColumnDetails]): Seq[IBMDB2ColumnV2] = { 
  
  val strcutredLayerSchema  = getStrcutredLayerSchema(tableSchema, tableName)
  val piiColumns = columns.filter(col => col.isPii).map(_.name)
  val allCDCcolumns = columns.map(_.name.toUpperCase)
  val commonColumnsExceptPii = (allCDCcolumns intersect strcutredLayerSchema.map(_.name.toUpperCase)) diff piiColumns
  val newColumns = allCDCcolumns diff (commonColumnsExceptPii ++ piiColumns)
  
  val piiColIBM2 = if(piiColumns.isEmpty) Nil else piiColumnsIBMDB2SeqConfig.get(tableName).getOrElse(Nil).filter(colm => piiColumns.contains(colm.name))
  val newColIBM2 = if(newColumns.isEmpty) Nil else {
    columns.filter(colmn => newColumns.contains(colmn.name)).map(column => {
      column match {
        case ColumnDetails(name, _, _, "NUMERIC", length, p, s, isPii) => IBMDB2ColumnV2(name, "NUMERIC", Some(p), Some(s), isPii)
        case ColumnDetails(name, _, _, typ, length, p, s, isPii) => IBMDB2ColumnV2(name, typ, None, None, isPii)
      }
    })
  }.toList

  val commonColExceptPiiIBM2 = strcutredLayerSchema.filter(strCol => commonColumnsExceptPii.contains(strCol.name)).map(colm => (colm.name, colm.dataType)).map(colmn => {
    colmn match {
      case (name, d: DecimalType) =>  IBMDB2ColumnV2(name, "DECIMAL", Some(d.precision), Some(d.scale), false)
      case (name, LongType) =>  IBMDB2ColumnV2(name, "LONG", None, None, false)
      case (name, StringType) =>  IBMDB2ColumnV2(name, "STRING", None, None, false)
      case (name, TimestampType) =>  IBMDB2ColumnV2(name, "TIMESTAMP", None, None, false)
      case (name, DateType) =>  IBMDB2ColumnV2(name, "DATE", None, None, false)
      case (name, IntegerType) =>  IBMDB2ColumnV2(name, "INTEGER", None, None, false)
      case (_,_) => throw new RuntimeException("Undefined data type for CDC to Strcutred mapping , dataType : "+colmn._2.toString+", Column: "+colmn._1.toString+", table: "+tableName)
    }
  })

  val allCDCcolIBM2 = (piiColIBM2 ++ newColIBM2 ++ commonColExceptPiiIBM2).distinct

  val allCDCcolIBM2map = allCDCcolIBM2.map(x => x.name -> x).toMap
  val finalAllCDCcolIBM2conveterd = (allCDCcolumns zip allCDCcolumns.map(allCDCcolIBM2map)).map{case(a,b) => b}.toSeq
  finalAllCDCcolIBM2conveterd
}

// COMMAND ----------

def updateDestinationSchema(structuredTablePath: String, batchOutputDF: DataFrame, tableSchema: String, tableName: String): Unit = {
  val currentDLFields = getDestinationSchemaFieldNames(structuredTablePath)
  val newCols = detectNewColumns(currentDLFields, batchOutputDF.schema.fieldNames)

  if (newCols.nonEmpty) {
    val columns = newCols.mkString(",")
    val message = s"Found new columns for table = ${tableSchema}.${tableName}. Adding new column manually as it's going to be used in merge: ${columns}."
    println(message)
    addNewFields(batchOutputDF.schema, structuredTablePath)
  }
}

val frameWorkColumns = List("_operation", "_changeSequence", "_messageSchemaId", "_qlikCapturedTime", "_ehEnqueuedTime", "_rawLayerProcessingTime", "_structuredLayerProcessingTime")

val notMetadata = (colName: String) => !frameWorkColumns.map(_.toUpperCase).contains(colName.toUpperCase)


def getDestinationSchemaFieldNames(path: String): Array[String] = {
  val onlyDataColumns = spark.read.format("delta").load(path)
    .schema.fieldNames
    .filter(notMetadata)
  
  onlyDataColumns
}

def detectNewColumns(destinationColumns: Array[String], cdcDataColumns: Array[String]): Seq[String] = {
  val desitnationColUppercased = destinationColumns.map(_.toUpperCase).toSeq
  val cdcDataColsUppercased = cdcDataColumns.filter(notMetadata).map(_.toUpperCase).toSeq
  cdcDataColsUppercased diff desitnationColUppercased
}

def addNewFields(currentSchema: StructType, outputPath: String): Unit = {
  val emptyDFWithNewFields = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], currentSchema)
  emptyDFWithNewFields.write.format("delta").mode("append").option("mergeSchema", true).save(outputPath)
}

// COMMAND ----------

def trimAllStringColumns(df: DataFrame) : DataFrame = {
  var newDf = df
  val listOfColumns = df.schema.filter(column => column.dataType.isInstanceOf[StringType]).map(_.name)
  if (listOfColumns.isEmpty) {
  
  } else {
    listOfColumns.foreach {
      column => {
        newDf = newDf.withColumn(column, trim(col(column)))
      }   
    }
  }
  return newDf
}

// COMMAND ----------

spark.udf.register("decrypt_target_unified_v2_sql", (row: Row) => if (row != null) {
   val keyId = row.getAs[String] ("keyId")
   val iv = row.getAs[String]("iv")
   val cipherText = row.getAs[String]("cipherText")
   val version = row.getAs[Integer]("version")
   val column = EncryptedColumnV2(version, keyId, cipherText, iv)
   decryptUnifiedv2(column)  
} else null)

// COMMAND ----------

spark.udf.register("parse_and_decrypt_qlik_sql", (column: String) => if (column!= null) decryptQlik(column) else null)

// COMMAND ----------

// DBTITLE 1,Add UDP counts
val runId = dbutils.notebook.getContext.parentRunId.getOrElse("-1").toLong
val jobId = dbutils.notebook.getContext.jobId.getOrElse("-1").toLong
val udpCountDetailTablePath = s"${structuredZonePrefix}/${systemName}/tables/cdcProcessingCount"
val udpCountAggTablePath = s"${rawZonePrefix}/udpCounts"

def AppendRawCdcCount(batchId: Long, sourceName: String, sourceSchema: String, tableName: String, messageSchemaId: String, cdcDf: DataFrame): Unit = {
  val groupByColms = Seq("qlikCapturedDate", "ehEnqueuedDate", "rawLayerDate")
  val otherColumnsMap = scala.collection.immutable.Map(
      "jobId" -> lit(jobId)
      ,"runId" -> lit(runId)
      ,"batchId" -> lit(batchId)
      ,"messageSchemaId" -> lit(messageSchemaId)
      ,"sourceName" ->  lit(sourceName)
      ,"sourceSchema" -> lit(sourceSchema)
      ,"tableName" -> lit(tableName)
      ,"logTimeStamp" -> current_timestamp
      ,"layer" -> lit("raw-cdc-data")
  )
  val selecOrder = Seq("sourceName", "layer", "sourceSchema", "tableName", "messageSchemaId", "jobId", "runId", "batchId", "qlikCapturedDate", "ehEnqueuedDate", "rawLayerDate","logTimeStamp") 
  val countDf = cdcDf
              .groupBy(groupByColms.map(col):_*)
              .pivot($"soruceOperation")
              .agg(count(lit(1)).as("cdcCount"))
              .withColumns(otherColumnsMap)
  val finalDf =  countDf.select((selecOrder ++ (countDf.columns diff selecOrder).sorted).map(col):_*)
  finalDf.write.format("delta").mode("append").option("mergeSchema", true).save(udpCountDetailTablePath)
}


def updateUdpCountAggTable(sourceName : String): Unit = {
  var filter = lit(true)
  if(DeltaTable.isDeltaTable(spark, udpCountAggTablePath)) {
    val getAggTabletMaxRawLayerDate = spark.read.load(udpCountAggTablePath).select(max($"rawLayerDate")).as[String].collect
    if(getAggTabletMaxRawLayerDate.nonEmpty) {
      val aggTabletMaxRawLayerDate =  getAggTabletMaxRawLayerDate.head 
      filter = $"rawLayerDate" >= s"${aggTabletMaxRawLayerDate}"
      spark.sql(s"""DELETE FROM delta.`${udpCountAggTablePath}` WHERE rawLayerDate = '${aggTabletMaxRawLayerDate}' and sourceName = '${sourceName}'""")
    }
  }
  val newRecordsAgg = spark.read.load(udpCountDetailTablePath)
                      .filter(filter)
                      .groupBy($"sourceName", $"layer", $"sourceSchema", $"tableName", $"rawLayerDate")
                      .agg(
                        sum(coalesce($"INSERT", lit(0))).as("INSERT")
                        ,sum(coalesce($"UPDATE", lit(0))).as("UPDATE")
                        ,sum(coalesce($"DELETE", lit(0))).as("DELETE")
                      ).withColumn("_logTimeStamp", current_timestamp())
                      
  if(!newRecordsAgg.isEmpty) newRecordsAgg.write.format("delta").partitionBy("sourceName").mode("append").save(udpCountAggTablePath)
  val maxDateInDetailTable = spark.read.load(udpCountDetailTablePath).select(max($"rawLayerDate")).as[String].collect.head
  spark.sql(s"""DELETE FROM delta.`${udpCountDetailTablePath}` WHERE rawLayerDate < date_sub('${maxDateInDetailTable}', 365) and sourceName = '${sourceName}'""")
  spark.sql(s""" OPTIMIZE delta.`${udpCountAggTablePath}` WHERE sourceName = '${sourceName}'""")
  spark.sql(s""" OPTIMIZE delta.`${udpCountDetailTablePath}`""")
  spark.sql(s""" VACUUM delta.`${udpCountDetailTablePath}` RETAIN 720 HOURS""")
  
}

// COMMAND ----------

if(DeltaTable.isDeltaTable(spark, udpCountDetailTablePath) && priorityOfTableList == "P1") updateUdpCountAggTable(systemName)

// COMMAND ----------

// MAGIC %md
// MAGIC ## CDCMerge

// COMMAND ----------

// hint: scala companion object
sealed trait CDCMerge {
  val df: DataFrame
  val mergeQuery: String
  val updateExpr: Map[String, String]
}
case class MergeWithKeys(df: DataFrame, mergeQuery: String, updateExpr: Map[String, String]) extends CDCMerge 

object CDCMerge {
  
  private def getMergeWithKeys(inputDF: DataFrame, metadata: MetadataPayload, partitionNum: Integer): MergeWithKeys = {
    
    val sourceColumnsDef = convertToIBMColumnHandleDataTypeMismatch(metadata.tableSchema, metadata.tableName, metadata.allColumns)
    val allPii = sourceColumnsDef.filter(col => col.isPII).map(_.name)
    val allColumnNames = sourceColumnsDef.map(_.name)
    
    val srcDataPartitionColumns = Seq("DATAPARTITIONNUM", "DATAPARTITIONNAME") 
    val qlikCapturedDataPartitionColumns = allColumnNames.filter(x => srcDataPartitionColumns.contains(x.toUpperCase))
    val qlikMissingDataPartitionColumns = srcDataPartitionColumns diff qlikCapturedDataPartitionColumns
     
    val withDlEncryptionStructureDF = parseQlikEncryptedColumns(inputDF, allPii)
    val decryptedDF = decryptUnified_v2(withDlEncryptionStructureDF, allPii)

    // Apply source types conversion
    val withTypesDF = CDCDataLakeTypeConversion.toSourceTypes(decryptedDF, sourceColumnsDef)
    val trimedDF = trimAllStringColumns(withTypesDF)

    val windowSpec  = Window.partitionBy(metadata.mergeColumns.map(column => col(column.name)):_*).orderBy($"_changeSequence".desc)

    // windowing function causes shuffle, it will create 200 partitions (by default - we should specify it to number of cores in cluster), 
    // migh cause performance issues, changing to equal to schedulerPoolPartitionNum
    // save is done for per each table in a loop and there is no possibility to have large number of rows for one table
    // reverted to trimmedDf from withTypesDF - MSAS
    
    val outputDF = trimedDF.coalesce(partitionNum)
                     .withColumn("_rowNumber",row_number().over(windowSpec))                                               
                     .filter($"_rowNumber" === 1)
                     .withColumn("_structuredLayerProcessingTime", current_timestamp())

    val finalDF = if(qlikMissingDataPartitionColumns.size ==2) {
      outputDF.withColumn("DATAPARTITIONNUM", lit(-1L)).withColumn("DATAPARTITIONNAME", lit(""))
    } else if (qlikMissingDataPartitionColumns.size == 1 && qlikMissingDataPartitionColumns.head == "DATAPARTITIONNUM") {
      outputDF.withColumn("DATAPARTITIONNUM", lit(-1L))
    } else if (qlikMissingDataPartitionColumns.size == 1 && qlikMissingDataPartitionColumns.head == "DATAPARTITIONNAME") {
      outputDF.withColumn("DATAPARTITIONNAME", lit(""))
    } else {
      outputDF
    }

    val firstSetOfCOlumns = Seq("RRN", "DATAPARTITIONNUM", "DATAPARTITIONNAME", "_operation", "_changeSequence", "_messageSchemaId", "_qlikCapturedTime", "_ehEnqueuedTime", "_rawLayerProcessingTime", "_structuredLayerProcessingTime")
    val selectColumnOrder =  firstSetOfCOlumns ++ allColumnNames.filter(x => ! srcDataPartitionColumns.contains(x.toUpperCase) && ! x.equalsIgnoreCase("RRN")).toSeq
    val outputMicroBatchDF = encrypt_v2(finalDF, allPii).select(selectColumnOrder.map(col):_*)
    val mergeQuery = metadata.mergeColumns.map(column => s"existing.${column.name} <=> update.${column.name}").mkString(" AND ")

    val updateExpr = if(qlikMissingDataPartitionColumns.size > 0) {
      selectColumnOrder.filter(colmn => ! qlikMissingDataPartitionColumns.contains(colmn.toUpperCase)).map(clm => clm->s"update.${clm}").toMap
    } else {
      selectColumnOrder.map(clm => clm->s"update.${clm}").toMap
    }
    MergeWithKeys(outputMicroBatchDF, mergeQuery, updateExpr)  
  }
  
  
  //  case 1: PRIMARY available and PKs are not contains any PII column
  def apply(df: DataFrame, metadata: MetadataPayload, partitionNum: Integer): CDCMerge = {
    val piiAsPartOfTheKey = metadata.mergeColumns.filter(_.isPii).nonEmpty
    if (!metadata.mergingAllColumns && !piiAsPartOfTheKey) getMergeWithKeys(df, metadata, partitionNum)
    else throw new RuntimeException(s"Config Error => No Valid Merging Approach Found for table: ${metadata.tableSchema}.${metadata.tableName}, messageSchemaId: ${metadata.messageSchemaId}")
  }
}

// COMMAND ----------

// MAGIC %md
// MAGIC ## processDataForSchemaId

// COMMAND ----------

def processDataForSchemaId(batchId: Long, batchDF: DataFrame, metadata: MetadataPayload, partitionNum: Integer): Unit = {
  
  val startLogDF = getLogInfo(metadata.tableSchema, metadata.tableName, metadata.messageSchemaId,"delta",Timestamp.valueOf(java.time.LocalDateTime.now),"processDataForSchemaId: Start",logLevel)
  saveLogInfo(startLogDF, logLevel)
  
  var inputDF = batchDF.coalesce(partitionNum)
                       .select($"body", $"rawLayerProcessingTime".as("_rawLayerProcessingTime"), $"enqueuedTime".as("_ehEnqueuedTime"))
                       .filter($"body.messageSchemaId" === metadata.messageSchemaId)
  
  val cdcProcessingStartTs = getTableLevelCdcProciessingStartTs(metadata.tableName, metadata.tableSchema)

  val avroDecodedDf = inputDF
                          .withColumn("content", from_avro($"body.message", metadata.dataSchema, fromAvroOptions))
  avroDecodedDf.persist                          
  
  val rawCountInputDf = avroDecodedDf
                          .select(to_date($"content.headers.timestamp").as("qlikCapturedDate"), to_date($"_ehEnqueuedTime").as("ehEnqueuedDate"), to_date($"_rawLayerProcessingTime").as("rawLayerDate"), $"content.headers.operation".as("soruceOperation"))

  val inputDFfromAvro = avroDecodedDf
                          .filter(($"content.headers.timestamp".cast(TimestampType)) > cdcProcessingStartTs)
                          .select($"content", $"_rawLayerProcessingTime", $"_ehEnqueuedTime", $"body")
  
  if(! inputDFfromAvro.isEmpty) {

    val frameWorkColumnsMap = scala.collection.immutable.Map(
      "_operation" -> $"headers.operation"
      ,"_changeSequence" -> concat(lit("10"), $"headers.changeSequence") // Add the '10' to differentiate from FL
      ,"_messageSchemaId" -> lit(metadata.messageSchemaId)  
      ,"_qlikCapturedTime" -> to_timestamp($"headers.timestamp")
      ,"_ehEnqueuedTime" -> $"_ehEnqueuedTime"
      ,"_rawLayerProcessingTime" -> $"_rawLayerProcessingTime"
      ,"_structuredLayerProcessingTime" -> current_timestamp
    )    
    
    val cdcMergeInputDFBeforePkcheck = inputDFfromAvro.select($"content.beforeData", $"content.data", $"_ehEnqueuedTime", $"content.headers", $"_rawLayerProcessingTime").withColumns(frameWorkColumnsMap).drop("headers")
    
    // Check for any changes on PK values itselves
    val mergeColumns = metadata.mergeColumns.map(column => { if (column.isPii) s"!(parse_and_decrypt_qlik_sql(data.${column.name}) <=> parse_and_decrypt_qlik_sql(beforeData.${column.name}))" else s"!(data.${column.name} <=> beforeData.${column.name})"
      })
    val mergeColumnQuery = """_operation = 'UPDATE' AND beforeData IS NOT NULL AND data IS NOT NULL AND (""" + mergeColumns.mkString(" OR ") + ")"

    val pkChangedDated = cdcMergeInputDFBeforePkcheck.filter(mergeColumnQuery)      
    val noPKchangedData = cdcMergeInputDFBeforePkcheck.except(pkChangedDated)
    val newInserts = pkChangedDated
                        .withColumn("_operation", lit("INSERT"))
                        .withColumn("beforeData", lit(null))

    val newDeletes = pkChangedDated
                        .withColumn("_operation", lit("DELETE"))
                        .withColumn("data", $"beforeData")
                        .withColumn("beforeData", lit(null))

    val unionDF = noPKchangedData.union(newInserts).union(newDeletes)
    val selectColms = frameWorkColumnsMap.keySet.toSeq.sorted ++ unionDF.select($"data.*").columns.map("data."+_) 
    val cdcMergeInputDF = unionDF.select(selectColms.map(col):_*)
    
    val cdcMergeInputLogDF = getLogInfo(metadata.tableSchema, metadata.tableName, metadata.messageSchemaId,"delta",Timestamp.valueOf(java.time.LocalDateTime.now),"cdcMergeInputDF",logLevel)
    saveLogInfo(cdcMergeInputLogDF, logLevel)
    
    val outputPath = GetDataTableStructuredPath(metadata.tableSchema, metadata.tableName)      
    val output: CDCMerge = CDCMerge(cdcMergeInputDF, metadata, partitionNum)
    val cdcDF = output.df
    val mergeCondition = output.mergeQuery
    val updateExpr = output.updateExpr
    
    if(! cdcDF.isEmpty) { // Process
      val processStartPKLogDF = getLogInfo(metadata.tableSchema, metadata.tableName, metadata.messageSchemaId,"delta",Timestamp.valueOf(java.time.LocalDateTime.now),"cdcDF Process PK: Start",logLevel)
      saveLogInfo(processStartPKLogDF, logLevel)
      
      if (DeltaTable.isDeltaTable(spark, outputPath)) {
        updateDestinationSchema(outputPath, cdcDF, metadata.tableSchema, metadata.tableName)
        val deltaTable = DeltaTable.forPath(spark, outputPath)    
        deltaTable.as("existing")
          .merge(cdcDF.as("update"),mergeCondition)  
          .whenMatched("update._changeSequence > existing._changeSequence").updateExpr(updateExpr) // If changeSequence number is newer then insert, otherwise don't.            
          .whenNotMatched.insertAll()
          .execute()
      } else { 
          val pkTableInitLogTime = Timestamp.valueOf(java.time.LocalDateTime.now)
          cdcDF.write.format("delta").mode("overwrite").save(outputPath) 
          val pkTableInitLogLogDF = getLogInfo(metadata.tableSchema, metadata.tableName, metadata.messageSchemaId,"delta",pkTableInitLogTime,"cdcDF Process PK: Init Table",logLevel)
          saveLogInfo(pkTableInitLogLogDF, logLevel)
      }
    
      val processEndPKLogDF = getLogInfo(metadata.tableSchema, metadata.tableName, metadata.messageSchemaId,"delta",Timestamp.valueOf(java.time.LocalDateTime.now),"cdcDF Process PK: End",logLevel)
      saveLogInfo(processEndPKLogDF, logLevel)
    }

    AppendRawCdcCount(batchId, systemName, metadata.tableSchema, metadata.tableName, metadata.messageSchemaId, rawCountInputDf)
  }
  avroDecodedDf.unpersist
  
  val endLogDF = getLogInfo(metadata.tableSchema, metadata.tableName, metadata.messageSchemaId,"delta",Timestamp.valueOf(java.time.LocalDateTime.now),"processDataForSchemaId: End",logLevel)
  saveLogInfo(endLogDF, logLevel)
}

// COMMAND ----------

// MAGIC %md
// MAGIC ## processDataForTable

// COMMAND ----------

// Added to handle parallel run issue
case class metaParhHashKV (table: String, metaArray: Array[MetadataPayload], index:Int)

def findSchemaIdofTable(schemaNTable: String, metadataList: Array[MetadataPayload]): Array[MetadataPayload] = { 
  metadataList.filter(meta => meta.tableSchema+"."+meta.tableName == schemaNTable).sortBy(_.timestamp)
} 

def processDataForTable(batchId: Long, batchDF: DataFrame, metadataArray: Array[MetadataPayload], partitionNum: Integer): Unit = {
 metadataArray.foreach {
   metadata => {
     println("Starting method processDataForSchemaId : " +metadata)
     processDataForSchemaId(batchId, batchDF, metadata, partitionNum = schedulerPoolPartitionNum)
     println("Ending method processDataForSchemaId : " +metadata)
   }
 } 
}

// COMMAND ----------

// MAGIC %md
// MAGIC ## upsertToDelta

// COMMAND ----------

import scala.collection.parallel._

// Method to upsert the logic
// maxBytexPerTrigger to limit amount of data loaded for micro batch (128 mb - starting value)
// scheduler pools added to increase parallelism of processing 
// schedulerPools parameter specify how many tables to run in parallel
// schedulerPoolPartitionNum indicates how many partions (task) will be processed for one pool
// schedulerPools x schedulerPoolPartitionNum should give number of available cores in cluster

// shuffle parttion should be equal to number of cluster's cores (sometimes multiplied by 2)
// (when running single app, there is a bit different - here we can keep it lower, it will create that many partition for each schema id)
// stored in checkpoint location, after restarting, the value will be populated from there (even you change it here)
// to change shuffle partition, one have to specify new checkpoint location

// spark.conf.set("spark.sql.shuffle.partitions", schedulerPoolPartitionNum) // Set to Auto at beggining

def getSchedulerPoolName(i: Int): String = {
  s"dl_raw_to_structured_stream_pool_${priorityOfTableList}_" + i 
}

def upsertToDelta (batchDF: DataFrame, batchId: Long) : Unit = {
  println(s"Batch: ${batchId}")
  
  val batchStartLogDF = getLogInfo("MicroBatch", "MicroBatch", "MicroBatch","delta",Timestamp.valueOf(java.time.LocalDateTime.now),s"Batch - ${batchId}: Start",logLevel)
  saveLogInfo(batchStartLogDF, logLevel)
  
  batchDF.persist()
  batchDF.count // To apply persist
  
  val batchMetaStartLogTime = Timestamp.valueOf(java.time.LocalDateTime.now)
  val validMetadataBatchDF = batchDF.select($"body.messageSchemaId").distinct().map(row=>row.getString(0)).collect()        
  val metadataListBatchDF = GetCurrentMetadata(metadataSchemaIds = validMetadataBatchDF)
  
  val distinctTables = metadataListBatchDF.map(metadata => metadata.tableSchema+"."+metadata.tableName).distinct
                      .filter(tab => tableNames.contains(tab.toUpperCase()))
  
  val fianlAllmetadataList = metadataListBatchDF.sortBy(_.timestamp)
  
  val mapMetadata = scala.collection.mutable.Map[String, Array[MetadataPayload]]()
  distinctTables.foreach(
    schemaNTable => {
      mapMetadata += (schemaNTable -> findSchemaIdofTable(schemaNTable,fianlAllmetadataList))
    }
  )
  
  val metadataParHashMap = mapMetadata.zipWithIndex.par
  val metaParhHashkv = metadataParHashMap.map(x => metaParhHashKV(x._1._1, x._1._2, x._2))
  
  val threadPool = new java.util.concurrent.ForkJoinPool(schedulerPools)
  metaParhHashkv.tasksupport = new ForkJoinTaskSupport(threadPool)
  println("Total table count in this microbatch : "+metaParhHashkv.count(x=>true))
  
  val batchMetaLogDF = getLogInfo("MicroBatch", "MicroBatch", "MicroBatch","delta",batchMetaStartLogTime,s"Batch - ${batchId}: Table Count = "+metaParhHashkv.count(x=>true)+", List of Tables: "+distinctTables.mkString(", "),logLevel)
  saveLogInfo(batchMetaLogDF, logLevel)
  
  metaParhHashkv.foreach {
    tableMeta => {
      val table = tableMeta.table
      val index = tableMeta.index
      val metadataSortedArray = tableMeta.metaArray
      
      val poolName = getSchedulerPoolName(index % schedulerPools)
      println(s"Scheduling job for: ${table} \nwith pool: ${poolName}")
      println(s"Running job for: ${table}")
      println(metadataSortedArray.mkString("\n"))
      spark.sparkContext.setLocalProperty("spark.scheduler.pool", poolName)
      processDataForTable(batchId, batchDF, metadataSortedArray, partitionNum = schedulerPoolPartitionNum)
    }
  }
  spark.sparkContext.setLocalProperty("spark.scheduler.pool", null)
  batchDF.unpersist()
  
  val batchEndLogDF = getLogInfo("MicroBatch", "MicroBatch", "MicroBatch","delta",Timestamp.valueOf(java.time.LocalDateTime.now),s"Batch - ${batchId}: End",logLevel)
  saveLogInfo(batchEndLogDF, logLevel)
}

// COMMAND ----------

val inputDataStreamingDF = if (maxBytesPerTrigger == 0) {
  spark.readStream.format("delta")
  .option("ignoreChanges", "true")
  .load(dataTableRawPath)
  .filter(s"enqueuedTime > '${defaultCdcProcessingStartTs}'")
} else {
  spark.readStream.format("delta")
  .option("maxBytesPerTrigger", maxBytesPerTrigger)
  .option("ignoreChanges", "true")
  .load(dataTableRawPath) 
  .filter(s"enqueuedTime > '${defaultCdcProcessingStartTs}'")
}

// COMMAND ----------

inputDataStreamingDF
  .writeStream
  .format("delta")
  .queryName(streamingQueryName)
  .option("checkpointLocation", dataStructuredCheckpointPath)
  .option("mergeSchema", "true")
  .foreachBatch(upsertToDelta _)
  .trigger(Trigger.ProcessingTime(triggerIntervalMinutes+" minutes"))
  .start()

// COMMAND ----------

// MAGIC %md
// MAGIC ## AutoTerminate

// COMMAND ----------

def GetStreamingQuery(queryName: String) = Try(spark.streams.active.filter(_.name == queryName).last).getOrElse(null)

def IsStillActiveQuery(uuid: java.util.UUID) = Try(spark.streams.get(uuid).isActive).getOrElse(false)

def IsStreamingQueryStarted(queryName: String, maxWaitTime: Int = 5): Boolean = {
  var maxWaitTimeToStartQuery = maxWaitTime
  var streamingQuery  = GetStreamingQuery(queryName)
  while(maxWaitTimeToStartQuery > 0 && streamingQuery == null) {
    Thread.sleep(60000)
    streamingQuery  = GetStreamingQuery(queryName)
    maxWaitTimeToStartQuery = maxWaitTimeToStartQuery - 1
  }
  streamingQuery != null 
}

// COMMAND ----------

def StopStreamQuery(streamingQuery: org.apache.spark.sql.streaming.StreamingQuery, streamStopTriggerUTCtime: java.time.LocalTime, killTimeHourOffset: Int ): Unit = {
  while((java.time.LocalTime.now.isBefore(streamStopTriggerUTCtime) || java.time.LocalTime.now.isAfter(java.time.LocalTime.parse("16:00:00"))) && IsStillActiveQuery(streamingQuery.id)) {
    println(s"wating till reach to streamStopTriggerUTCtime ($streamStopTriggerUTCtime), Now : "+java.time.LocalDateTime.now)
    println(streamingQuery.status+"\n")
    Thread.sleep(30 * 60000)
  }
  val stopStreamStartingTimestamp = java.time.LocalDateTime.now
  if(IsStillActiveQuery(streamingQuery.id)) {
    println("Time to stop the stream : "+stopStreamStartingTimestamp)
    println(streamingQuery.status)
  }
  while(IsStillActiveQuery(streamingQuery.id) && java.time.LocalDateTime.now.isBefore(stopStreamStartingTimestamp.plusHours(killTimeHourOffset))) {
    val streamingQueryStatus = streamingQuery.status
    println("Trying to Stop the stream gracefully")
    println(streamingQueryStatus)
    if(!streamingQueryStatus.isDataAvailable && !streamingQueryStatus.isTriggerActive && streamingQueryStatus.message.toLowerCase.equals("waiting for next trigger")) {
      streamingQuery.stop()
      println("Stop the stream gracefully : "+java.time.LocalDateTime.now)
    }
    streamingQuery.awaitTermination(2 * 60000)
  }
  
  if(IsStillActiveQuery(streamingQuery.id)) {
    streamingQuery.stop()
    val stopStatus = streamingQuery.awaitTermination(30 * 60000)
    if(stopStatus) println("Stop the stream forcefully : "+java.time.LocalDateTime.now) else throw new RuntimeException("Unable to Stop the Stream : "+streamingQueryName)
  }
}

// COMMAND ----------

def StopStreamGracefully(streamingQuery: org.apache.spark.sql.streaming.StreamingQuery, streamStopTriggerUTCtime: java.time.LocalTime): Unit = {
  while((java.time.LocalTime.now.isBefore(streamStopTriggerUTCtime) || java.time.LocalTime.now.isAfter(java.time.LocalTime.parse("16:00:00"))) && IsStillActiveQuery(streamingQuery.id))  {
    println(s"wating till reach to streamStopTriggerUTCtime ($streamStopTriggerUTCtime), Now : "+java.time.LocalDateTime.now)
    println(streamingQuery.status+"\n")
    Thread.sleep(30 * 60000)
  }
  val stopStreamStartingTimestamp = java.time.LocalDateTime.now
  if(IsStillActiveQuery(streamingQuery.id)) {
    println("Time to stop the stream : "+stopStreamStartingTimestamp)
    println(streamingQuery.status)
  }
  while(IsStillActiveQuery(streamingQuery.id)) {
    val streamingQueryStatus = streamingQuery.status
    println("Trying to Stop the stream gracefully")
    println(streamingQueryStatus)
    if(!streamingQueryStatus.isDataAvailable && !streamingQueryStatus.isTriggerActive && streamingQueryStatus.message.toLowerCase.equals("waiting for next trigger")) {
      streamingQuery.stop()
      println("Stop the stream gracefully : "+java.time.LocalDateTime.now)
    }
    streamingQuery.awaitTermination(2 * 60000)
  }
}

// COMMAND ----------

if (!IsStreamingQueryStarted(streamingQueryName)) throw new RuntimeException("Can not find any active streaming query with name : "+streamingQueryName)
val streamingQuery = GetStreamingQuery(streamingQueryName)
println(streamingQuery.status)

// COMMAND ----------

if(autoTerminateFlag) StopStreamGracefully(streamingQuery, streamStopTriggerUTCtime)

// COMMAND ----------

updateUdpCountAggTable(systemName)