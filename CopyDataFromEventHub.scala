bhjjhgjgkjgljkhl// Databricks notebook source
// MAGIC %run ../_Shared/Config/SessionConfigs

// COMMAND ----------

// DBTITLE 1,Notebook Parameters
dbutils.widgets.text("secretScope","")
dbutils.widgets.text("rawFileSystem","")
dbutils.widgets.text("storageAccountKey","")
dbutils.widgets.text("eventHubKey","")
dbutils.widgets.text("eventHubName", "")
dbutils.widgets.text("consumerGroup", "")
dbutils.widgets.text("maxEventsPerTrigger", "") 
dbutils.widgets.text("triggerIntervalSeconds","")
dbutils.widgets.text("optimizeRawDataPath","")

// COMMAND ----------

import scala.util.Try
import org.apache.spark.sql.streaming.Trigger

val systemName = "IL"
val secretScope = dbutils.widgets.get("secretScope")
val rawFileSystem = dbutils.widgets.get("rawFileSystem")
val storageAccountKey = dbutils.widgets.get("storageAccountKey")
val eventHubKey = dbutils.widgets.get("eventHubKey")
val eventHubName = dbutils.widgets.get("eventHubName")
val consumerGroup = dbutils.widgets.get("consumerGroup")
val maxEvents = Try(dbutils.widgets.get("maxEventsPerTrigger").toInt).getOrElse(50000) 
val triggerIntervalSeconds = Try(dbutils.widgets.get("triggerIntervalSeconds").toInt).getOrElse(30)
val optimizeRawDataPath = Try(dbutils.widgets.get("optimizeRawDataPath").trim.toBoolean).getOrElse(false)
val storageAccountName = dbutils.secrets.get(scope = secretScope, key = storageAccountKey)
val eventHubConnectionString = dbutils.secrets.get(scope = secretScope, key = eventHubKey)
val rawZonePrefix = "abfss://" + rawFileSystem + "@" + storageAccountName + ".dfs.core.windows.net"

// COMMAND ----------

spark.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true")
spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite",true)
spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.autoCompact",true)

// COMMAND ----------

val dataTableRawPath = rawZonePrefix + s"/${systemName}/cdc/data/"

// COMMAND ----------

if(optimizeRawDataPath) Try{display(spark.sql(s"optimize delta.`${dataTableRawPath}`"))}

// COMMAND ----------

val replicateEnvelope = """{
"type":"record",
"name":"MessageEnvelope",
"fields":[
    {"name":"magic","type":{"type":"fixed","name":"Magic","size":5}},
    {"name":"type","type":"string"},
    {"name":"headers","type":["null",{"type":"map","values":"string"}]},
    {"name":"messageSchemaId","type":["null","string"]},
    {"name":"messageSchema","type":["null","string"]},
    {"name":"message","type":"bytes"}
]
}"""

// COMMAND ----------

import org.apache.spark.eventhubs.{ ConnectionStringBuilder, EventHubsConf, EventPosition }
import org.apache.spark.sql.avro.functions._
import org.apache.spark.eventhubs._
import org.apache.spark.sql.functions.{lit, current_timestamp}
import java.time._

val connStr = new com.microsoft.azure.eventhubs.ConnectionStringBuilder(eventHubConnectionString)
  .setEventHubName(eventHubName)

val dataEventHubParameters =
  EventHubsConf(connStr.toString())
  .setStartingPosition(EventPosition.fromSequenceNumber(0L))   
  .setConsumerGroup(consumerGroup)
  .setMaxEventsPerTrigger(maxEvents) 

// EventPosition.fromOffset("246812")          // Specifies offset 246812
// EventPosition.fromSequenceNumber(100L)      // Specifies sequence number 100
// EventPosition.fromEnqueuedTime(Instant.now) // Any event after the current time
// EventPosition.fromStartOfStream             // Specifies from start of stream
// EventPosition.fromEndOfStream               // Specifies from end of stream

val streamingInputDFData = 
  spark.readStream
    .format("eventhubs")
    .options(dataEventHubParameters.toMap)
    .load()
    .withColumn("body", from_avro($"body", replicateEnvelope))
    .withColumn("rawLayerProcessingTime", current_timestamp())

// COMMAND ----------

streamingInputDFData
        .writeStream
        .format("delta")
        .queryName(s"${systemName}_CopyDataFromEventHub")
        .option("checkpointLocation", s"${dataTableRawPath}/_checkpoints")
        .trigger(Trigger.ProcessingTime(triggerIntervalSeconds+" seconds"))
        .outputMode("append")        
        .start(dataTableRawPath)

// COMMAND ----------

//display(streamingInputDFData)

// COMMAND ----------

