// Databricks notebook source
// MAGIC %run ../_Shared/Config/SessionConfigs

// COMMAND ----------

dbutils.widgets.text("secretScope","")
dbutils.widgets.text("rawFileSystem","")
dbutils.widgets.text("storageAccountKey","")
dbutils.widgets.text("eventHubKey","")
dbutils.widgets.text("eventHubName", "")
dbutils.widgets.text("consumerGroup", "")

// COMMAND ----------

val systemName = "IL"
val secretScope = dbutils.widgets.get("secretScope")
val rawFileSystem = dbutils.widgets.get("rawFileSystem")
val storageAccountKey = dbutils.widgets.get("storageAccountKey")
val eventHubKey = dbutils.widgets.get("eventHubKey")
val eventHubName = dbutils.widgets.get("eventHubName")
val consumerGroup = dbutils.widgets.get("consumerGroup")

val storageAccountName = dbutils.secrets.get(scope = secretScope, key = storageAccountKey)
val eventHubConnectionString = dbutils.secrets.get(scope = secretScope, key = eventHubKey)
val rawZonePrefix = "abfss://" + rawFileSystem + "@" + storageAccountName + ".dfs.core.windows.net"

// COMMAND ----------

spark.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true")
spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite",true)
spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.autoCompact",true)

// COMMAND ----------

val metadataTableRawPath = rawZonePrefix + s"/${systemName}/cdc/metadata/"

// COMMAND ----------

import scala.util.Try
Try{display(spark.sql(s"optimize delta.`${metadataTableRawPath}`"))}

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
import org.apache.spark.sql.avro.functions._
import java.time._

val connStr = new com.microsoft.azure.eventhubs.ConnectionStringBuilder(eventHubConnectionString)
  .setEventHubName(eventHubName)

println(connStr)
val metadataEventHubParameters =
  EventHubsConf(connStr.toString())
  .setStartingPosition(EventPosition.fromSequenceNumber(0L))
  .setConsumerGroup(consumerGroup)

val streamingInputDFMetadata = 
  spark.readStream
    .format("eventhubs")
    .options(metadataEventHubParameters.toMap)
    .load()
    .withColumn("body", from_avro($"body", replicateEnvelope))

// COMMAND ----------

streamingInputDFMetadata.writeStream
  .format("delta")
  .queryName(s"${systemName}_CopyMetadataFromEventHub")
  .option("checkpointLocation", s"${metadataTableRawPath}/_checkpoints")
  .outputMode("append")
  .start(metadataTableRawPath)