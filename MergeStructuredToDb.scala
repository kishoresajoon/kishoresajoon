// Databricks notebook source
// MAGIC %md
// MAGIC ## Change Log
// MAGIC - Inital version : loading CDC data into Hyperscale SQL table
// MAGIC   - 01 - First Read CDC changes from structured layer
// MAGIC   - 02 - Bring latest change of given SK into temp table in SQL DB
// MAGIC   - 03 - Then do merge from temp to orignal table in SQL DB
// MAGIC - Optimization realted Changes [2023-10-04] 
// MAGIC   - 01 - table structure changes , Flatten strcut and keep PK / Hash at begining of the tables
// MAGIC   - 02 - Add Clsutered index 
// MAGIC   - 03 - Seperate config notebook for CDC table list config for Strcuctured to Db
// MAGIC   - 04 - CDC processing start point , based on delta version (earlier based on delta timestamp)
// MAGIC

// COMMAND ----------

// MAGIC %md
// MAGIC ## Helper Notebooks

// COMMAND ----------

// MAGIC %run ../_Shared/AZURESQLDBConnector/CustomMsSqlServerDialect

// COMMAND ----------

import org.apache.spark.sql.jdbc.JdbcDialects
JdbcDialects.registerDialect(CustomMsSqlServerDialect)

// COMMAND ----------

// MAGIC %run ../_Shared/Config/cdcTableListConfigsHyperscaleDb

// COMMAND ----------

// MAGIC %run ../_Shared/Metadata/EvaluateDataTableMetadata

// COMMAND ----------

// MAGIC %run ../_Shared/logging/debugLogHelpersCdcMergingSqlDb

// COMMAND ----------

// MAGIC %run ../_Shared/Schema/TypeConversions

// COMMAND ----------

// MAGIC %run ../_Shared/ColumnMappings/EncryptionColumnMappings

// COMMAND ----------

// MAGIC %run ../_Shared/Config/SessionConfigs

// COMMAND ----------

// MAGIC %run ../_Shared/AZURESQLDBConnector/HyperscaleDBConnector

// COMMAND ----------

spark.conf.set("spark.sql.optimizer.maxIterations",1000000)
spark.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true")
spark.sql("SET spark.databricks.delta.properties.defaults.enableChangeDataFeed = true")
spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite",true)
spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.autoCompact",true)
spark.sql("set spark.databricks.delta.changeDataFeed.timestampOutOfRange.enabled = true")
spark.conf.set("spark.databricks.delta.changeDataFeed.timestampOutOfRange.enabled", true)

// COMMAND ----------

import io.delta.tables.DeltaTable
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._
import java.time.LocalTime._
import java.time.LocalDateTime._
import java.time.format.DateTimeFormatter

// COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text("secretScope","adb-kv-adls")
dbutils.widgets.text("structuredFileSystem","structured")
dbutils.widgets.text("storageAccountKey","pbtb-st-name")
dbutils.widgets.text("schedulerPools", "16")
dbutils.widgets.text("schedulerPoolPartitionNum", "16")
dbutils.widgets.text("logLevel", "DEBUG")
dbutils.widgets.text("priorityOfTableList","P1")
dbutils.widgets.text("runParallel","True")
dbutils.widgets.text("maxBytesPerTrigger","")

// JDBC PARAMS
dbutils.widgets.text("jdbcUsernameKey", "db-il-username")
dbutils.widgets.text("jdbcPasswordKey", "db-il-Password")
dbutils.widgets.text("jdbcPort", "1433")
dbutils.widgets.text("jdbcDatabase", "db_il")
dbutils.widgets.text("dbServerName", "db-server-name")
dbutils.widgets.text("jdbcConnectionNumber", "8")

// COMMAND ----------

import scala.util.Try

val systemName = "IL"
val secretScope = dbutils.widgets.get("secretScope")
val structuredFileSystem = dbutils.widgets.get("structuredFileSystem")
val storageAccountKey = dbutils.widgets.get("storageAccountKey")
val storageAccountName = dbutils.secrets.get(scope = secretScope, key = storageAccountKey)
val schedulerPools = Try(dbutils.widgets.get("schedulerPools").toInt).getOrElse(4)
val logLevel = dbutils.widgets.get("logLevel")
val schedulerPoolPartitionNum = Try(dbutils.widgets.get("schedulerPoolPartitionNum").toInt).getOrElse(4)
val structuredZonePrefix = "abfss://" + structuredFileSystem + "@" + storageAccountName + ".dfs.core.windows.net"
val structuredLayer =  s"${structuredZonePrefix}/${systemName}"
val priorityOfTableList = dbutils.widgets.get("priorityOfTableList").split(",").map(_.trim.toUpperCase)
val runParallel = Try(dbutils.widgets.get("runParallel").toBoolean).getOrElse(false)
val maxBytesPerTrigger = Try(dbutils.widgets.get("maxBytesPerTrigger").toLong).getOrElse(1073741824L)

// COMMAND ----------

val jdbcUsernameKey = dbutils.widgets.get("jdbcUsernameKey")
val jdbcPasswordKey = dbutils.widgets.get("jdbcPasswordKey")
val dbServerName = dbutils.widgets.get("dbServerName")
val jdbcPort = dbutils.widgets.get("jdbcPort")
val jdbcDatabase = dbutils.widgets.get("jdbcDatabase")
val jdbcConnectionNumber = Try(dbutils.widgets.get("jdbcConnectionNumber").toInt).getOrElse(10)

val dbServer = dbutils.secrets.get(scope = secretScope, key = dbutils.widgets.get("dbServerName"))
val jdbcUser = dbutils.secrets.get(scope = secretScope, key = dbutils.widgets.get("jdbcUsernameKey"))
val jdbcPass = dbutils.secrets.get(scope = secretScope, key = dbutils.widgets.get("jdbcPasswordKey"))
val hostname = s"${dbServer}.database.windows.net"
val jdbcUrl = s"jdbc:sqlserver://${hostname}:${jdbcPort};database=${jdbcDatabase}"
val jdbcDriver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"

val connectionProperties = Utils.connectionProperties(jdbcUser, jdbcPass, jdbcDriver, jdbcUrl)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Helper Methods

// COMMAND ----------

def GetDataTableStructuredPath(tableSchema:String, tableName:String) = s"${structuredLayer}/tables/${tableSchema}/${tableName}" 
def GetDBTableCheckPointPath(tableSchema:String, tableName:String) = s"${structuredLayer}/database/_checkpoints/${tableSchema}/${tableName}" 
def GetDbTableSchema(schemaName: String) = s"${systemName}"
def GetDbTempTableSchema(schemaName: String) = s"${systemName}_TEMP"
def CreateDBschema(schemaName: String): Unit = {
  HyperscaleDB.executeStatement(s"""IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name= '${schemaName}')
exec ('CREATE SCHEMA [${schemaName}]')""", connectionProperties );
}
def GetDbTableName(schemaName: String, tableName:String) = s"${schemaName}.${tableName}"

// COMMAND ----------

spark.udf.register("parse_and_decrypt_qlik_sql", (column: String) => if (column!= null) decryptQlik(column) else null)

def encryptColumns(inputDF: DataFrame, columns: Seq[String]): DataFrame = {
  recurseEncryptv2(inputDF, columns.toArray)
}

def decryptColumns(inputDF: DataFrame, columns: Seq[String]): DataFrame = {
  recurseDecryptv2(inputDF, columns.toArray)
}

// COMMAND ----------

val runId = dbutils.notebook.getContext.parentRunId.getOrElse("-1").toString
val jobId = dbutils.notebook.getContext.jobId.getOrElse("-1").toString
val dummyMessageSchemaId = s"jobId: ${jobId}, runId: ${runId}"

// COMMAND ----------

def getLisfOfTablesBasedOnConfig(keys: Array[String]): scala.collection.mutable.Map[String,Int] = {
  val returnVal =  scala.collection.mutable.Map[String,Int]()
  keys.foreach {
    key => returnVal ++= dbMergeCdcTableList.get(key).get
  }
  returnVal
}

def GetTableLatestVersion(sourceSchema: String, tableName: String) = {
  val deltaTable = DeltaTable.forPath(spark, GetDataTableStructuredPath(sourceSchema, tableName))
  val deltaLog = deltaTable.getClass.getMethod("deltaLog").invoke(deltaTable)
  val snapshot = deltaLog.getClass.getMethod("unsafeVolatileSnapshot").invoke(deltaLog)
  val snapshotVersion = snapshot.getClass.getMethod("version").invoke(snapshot).toString.toLong
  snapshotVersion
}

// COMMAND ----------

case class tableMeta(schemaName: String, tableName: String, configuredDeltaVersion: Long, snapshotDeltaVersion: Long)

val listofTableAsPerConfig = getLisfOfTablesBasedOnConfig(priorityOfTableList)
println("listofTableAsPerConfig.size = "+listofTableAsPerConfig.size)

// COMMAND ----------

val tableListToRun = listofTableAsPerConfig.map{
  case(k, v) => tableMeta(k.split("\\.").head, k.split("\\.").last, v.toLong, GetTableLatestVersion(k.split("\\.").head, k.split("\\.").last))
}.filter(x => (x.snapshotDeltaVersion >= x.configuredDeltaVersion))

println("tableListToRun.size = "+tableListToRun.size)

// COMMAND ----------

// MAGIC %md
// MAGIC ## upsertToDB

// COMMAND ----------

def upsertToDB(tableSchema: String, tableName: String, batchDF: DataFrame, batchId: Long): Unit = {
  
  val startUpsertToDBLogDF = getLogInfo(tableSchema, tableName, dummyMessageSchemaId,"db",Timestamp.valueOf(java.time.LocalDateTime.now),"upsertToDB: Start",logLevel)
  saveLogInfo(startUpsertToDBLogDF, logLevel)
  
  val dbTempTableSchema =  GetDbTempTableSchema(tableSchema)
  val dbTempTableName =  GetDbTableName(dbTempTableSchema, tableName)
  CreateDBschema(dbTempTableSchema)

  val dbTableSchema =  GetDbTableSchema(tableSchema)
  val dbTableName =  GetDbTableName(dbTableSchema, tableName)
  CreateDBschema(dbTableSchema)

  val piiColNames = Try{piiConfigMap(tableName)}.getOrElse(Seq[String]())
//   println("PII columns : "+piiColNames.mkString(", "))
  
  val decryptedDF = if (!piiColNames.isEmpty) {
    decryptColumns(batchDF,piiColNames)
  } else {
    batchDF
  }
  val mergeColumns = List("RRN")

  val windowSpec = Window.partitionBy(mergeColumns.map(col):_*).orderBy($"_changeSequence".desc)
  val withFlatMetadata = decryptedDF
                  .withColumn("_dbProcessingTimestamp", current_timestamp)
                  .withColumn("_rowNumber",row_number().over(windowSpec))
                  .filter($"_rowNumber" === 1)
                  .drop("_rowNumber")

  val metaDataColumnsDb = List("RRN", "DATAPARTITIONNUM", "DATAPARTITIONNAME" ,"_operation", "_changeSequence", "_messageSchemaId", "_qlikCapturedTime", "_ehEnqueuedTime", "_rawLayerProcessingTime", "_structuredLayerProcessingTime", "_dbProcessingTimestamp") 

  val selectColumnOrderDb = metaDataColumnsDb.filter(cl => withFlatMetadata.columns.map(_.toUpperCase).contains(cl.toUpperCase)) ++ withFlatMetadata.schema.fieldNames.filterNot(colm => metaDataColumnsDb.map(_.toUpperCase).contains(colm.toUpperCase))
  val dbFinalDF = withFlatMetadata.select(selectColumnOrderDb.map(col(_)):_*)
  val dlTableSchame = dbFinalDF.schema
  
  if(!HyperscaleDB.tableExist(dbTempTableSchema, tableName, connectionProperties)) HyperscaleDB.createTable(dbTempTableName, dbFinalDF.schema, connectionProperties)
  if(!HyperscaleDB.tableExist(dbTableSchema, tableName, connectionProperties)) HyperscaleDB.createTable(dbTableName, dbFinalDF.schema, connectionProperties)
  HyperscaleDB.updateDestinationSchemaV2(dbTempTableName, dlTableSchame, connectionProperties)
  HyperscaleDB.updateDestinationSchemaV2(dbTableName, dlTableSchame, connectionProperties)

  if(!dbFinalDF.isEmpty) {
    //Temp Table Overwrite
    val tempTableOverwriteLogTime = Timestamp.valueOf(java.time.LocalDateTime.now)
    HyperscaleDB.writeToDB(dbFinalDF, dbTempTableName, connectionProperties, "overwrite", schedulerPoolPartitionNum)
    val tempTableOverwriteLogDF = getLogInfo(tableSchema, tableName, dummyMessageSchemaId,"db",tempTableOverwriteLogTime,"TempTableOverwrite",logLevel)
  saveLogInfo(tempTableOverwriteLogDF, logLevel)
    
    //Final Table Merge
    val finalTableMergeLogTime = Timestamp.valueOf(java.time.LocalDateTime.now)
    val mergeQuery = QueryGenerator.tableMergeStmtV3(dbTableName, dbTempTableName, dlTableSchame.fieldNames, mergeColumns)
    HyperscaleDB.executeStatement(mergeQuery, connectionProperties)
    val finalTableMergeLogDF = getLogInfo(tableSchema, tableName, dummyMessageSchemaId,"db",finalTableMergeLogTime,"FinalTableMerge",logLevel)
    saveLogInfo(finalTableMergeLogDF, logLevel)
  }
  
  val endUpsertToDBLogDF = getLogInfo(tableSchema, tableName, dummyMessageSchemaId,"db",Timestamp.valueOf(java.time.LocalDateTime.now),"upsertToDB: End",logLevel)
  saveLogInfo(endUpsertToDBLogDF, logLevel)
}

// COMMAND ----------

// MAGIC %md
// MAGIC ## processDataForTable

// COMMAND ----------

def processDataForTable(tableSchema: String, tableName: String, cdcStartingVersion:Long): Unit = {
  
  val streamingQueryName = s"${systemName}_MergeStructuredToDb_${tableSchema}_${tableName}"
  val structuredLayerTablePath = GetDataTableStructuredPath(tableSchema, tableName)
  val checkpointLocation = GetDBTableCheckPointPath(tableSchema, tableName)

  val readStream = spark.readStream
                      .format("delta")
                      .option("readChangeFeed", "true")
                      .option("startingVersion", cdcStartingVersion)
                      .option("maxBytesPerTrigger", maxBytesPerTrigger)
                      .load(structuredLayerTablePath)
                      .where(col("_change_type") =!= "update_preimage" && col("_change_type") =!= "delete")

  val writeStream = readStream
                      .drop("_change_type", "_commit_version", "_commit_timestamp")
                      .writeStream
                      .format("delta")
                      .option("checkpointLocation", checkpointLocation)
                      .queryName(streamingQueryName)
                      .foreachBatch((batchDF: DataFrame, batchId: Long) => upsertToDB(tableSchema, tableName, batchDF, batchId)) 
                      .trigger(Trigger.AvailableNow)
  writeStream.start()
  
  Thread.sleep(2000)
  val streamingQuery  = spark.streams.active.filter(_.name == streamingQueryName)
  if (!streamingQuery.isEmpty) {
    val streamingQueryId =  streamingQuery.last
    if(streamingQueryId.isActive) streamingQueryId.awaitTermination else streamingQueryId.stop
  }
  println(s"Ending job for: ${tableSchema}.${tableName}")
}

// COMMAND ----------

// MAGIC %md
// MAGIC ##processDataForAllTables

// COMMAND ----------

import scala.collection.parallel._

// Method to upsert the logic
// scheduler pools added to increase parallelism of processing 
// schedulerPools parameter specify how many tables to run in parallel
// schedulerPoolPartitionNum indicates how many partions (task) will be processed for one pool
// schedulerPools x schedulerPoolPartitionNum should give number of available cores in cluster

// shuffle parttion should be equal to number of cluster's cores (sometimes multiplied by 2)
// (when running single app, there is a bit different - here we can keep it lower, it will create that many partition for each table)
// stored in checkpoint location, after restarting, the value will be populated from there (even you change it here)
// to change shuffle partition, one have to specify new checkpoint location


def getSchedulerPoolName(i: Int): String = {
  s"dl_${systemName}_structured_to_db_schedular_pool_" + i 
}

def processDataForAllTables(runParallel: Boolean) : Unit = {
  
  val startProcessAllLogTime = Timestamp.valueOf(java.time.LocalDateTime.now)

  val listofSchemaNtables =  tableListToRun
  println("Total table count in this run : "+listofSchemaNtables.size)

  val startProcessAllLogDF = getLogInfo("Batch", "Batch", dummyMessageSchemaId,"db",startProcessAllLogTime,"processDataForAllTables: Start , Table Count = "+listofSchemaNtables.size,logLevel)
  saveLogInfo(startProcessAllLogDF, logLevel)
 
  if(runParallel) {
    spark.conf.set("spark.sql.shuffle.partitions", schedulerPoolPartitionNum)
    val tableListParHashMap = listofSchemaNtables.zipWithIndex.par
  
    val threadPool = new java.util.concurrent.ForkJoinPool(schedulerPools)
    tableListParHashMap.tasksupport = new ForkJoinTaskSupport(threadPool)

    tableListParHashMap.foreach {

      tableMeta => {
        val schema = tableMeta._1.schemaName
        val table = tableMeta._1.tableName
        val cdcStartDeltaVersion = tableMeta._1.configuredDeltaVersion
        val index = tableMeta._2
      
        val poolName = getSchedulerPoolName(index % schedulerPools)
        println(s"Scheduling job for: ${schema}.${table} \nwith pool: ${poolName}")
        println(s"Running job for: ${schema}.${table} , Found Change Data feed starting from delta version ${cdcStartDeltaVersion}")
        spark.sparkContext.setLocalProperty("spark.scheduler.pool", poolName)
        processDataForTable(schema, table, cdcStartDeltaVersion)
      }
    }
    spark.sparkContext.setLocalProperty("spark.scheduler.pool", null)
    
  } else {
    listofSchemaNtables.foreach {
      tableMeta => {
        val schema = tableMeta.schemaName
        val table = tableMeta.tableName
        val cdcStartDeltaVersion = tableMeta.configuredDeltaVersion
        println(s"Running job for: ${schema}.${table} , Found Change Data feed starting from delta version ${cdcStartDeltaVersion}")
        processDataForTable(schema, table, cdcStartDeltaVersion)
      }
    } 
  } 
}

// COMMAND ----------

processDataForAllTables(runParallel)