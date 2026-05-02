// Databricks notebook source
// MAGIC %run ../_Shared/Config/SessionConfigs

// COMMAND ----------

// DBTITLE 1,Notebook Parameters
dbutils.widgets.text("secretScope","adb-kv-adls")
dbutils.widgets.text("rawFileSystem","raw")
dbutils.widgets.text("structuredFileSystem","structured")
dbutils.widgets.text("storageAccountKey","pbtb-st-name")

// COMMAND ----------

val systemName = "IL"
val secretScope = dbutils.widgets.get("secretScope")
val rawFileSystem = dbutils.widgets.get("rawFileSystem")
val structuredFileSystem = dbutils.widgets.get("structuredFileSystem")
val storageAccountKey = dbutils.widgets.get("storageAccountKey")

val storageAccountName = dbutils.secrets.get(scope = secretScope, key = storageAccountKey)
val rawZonePrefix = "abfss://" + rawFileSystem + "@" + storageAccountName + ".dfs.core.windows.net"
val structuredZonePrefix = "abfss://" + structuredFileSystem + "@" + storageAccountName + ".dfs.core.windows.net"

// COMMAND ----------

spark.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true")
spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite",true)
spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.autoCompact",true)

// COMMAND ----------

val metadataTableRawPath = s"${rawZonePrefix}/${systemName}/cdc/metadata/"
val metadataStructuredTablePath = s"${structuredZonePrefix}/${systemName}/metadata/"

// COMMAND ----------

import scala.util.Try
Try{display(spark.sql(s"optimize delta.`${metadataStructuredTablePath}`"))}

// COMMAND ----------

// DBTITLE 1,Metadata Helper Method to Map Complex Structs
import org.apache.spark.sql.functions.map
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.struct
import org.apache.spark.sql.functions.array
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.Column
import org.apache.spark.sql.types.StructType
import scala.collection.mutable

def mapStruct(schema: StructType, prefixToMap: String, state: String = "", currentColumn: Column = null) : Array[Column] = {  
     schema.fields.flatMap(f => {
          val newState = state + f.name + ".";
          var newColumn = (if (currentColumn != null) currentColumn(f.name) else col(f.name)).alias(f.name)

          if (prefixToMap startsWith newState) {
            f.dataType match {
              case structType: StructType => {
                if(newState.equals(prefixToMap)){                      
                      var columns = structType.fields.map(inner =>{
                        struct(lit(inner.name).alias("key"), newColumn(inner.name).alias("value"))
                      })
                      Array(array(columns:_*).alias(f.name))                      
                }  
                else {                  
                  Array(struct(mapStruct(structType, prefixToMap, newState, newColumn):_*).alias(f.name))
                }
              }
              case _ => Array(newColumn)
            }          
          } else {
            Array(newColumn)
          }
      })    
}

// COMMAND ----------

// DBTITLE 1,Metadata Structured Import
import org.apache.spark.sql.avro.functions._
import org.apache.spark.sql.DataFrame
import java.io.FileNotFoundException
import io.delta.tables.DeltaTable
import org.apache.spark.sql.functions.concat
import org.apache.spark.sql.functions.filter
import org.apache.spark.sql.functions.transform

case class metadataSchemaPair(messageSchemaId : String, messageSchema: String)

val metadataRawDF = spark.readStream.format("delta").load(metadataTableRawPath);
    
def foreachBatchMethod (batchDF: DataFrame, batchId: Long) : Unit = {
      batchDF.persist()
      val distinctValuesDF = batchDF
            .withColumn("_messageSchemaId", $"body.messageSchemaId")
            .dropDuplicates("_messageSchemaId")
            .drop("_messageSchemaId")
      var newRowDistinctDF : DataFrame = null;

      if (DeltaTable.isDeltaTable(spark, metadataStructuredTablePath)) {
          val existingMetadataDF = spark.read.format("delta").load(metadataStructuredTablePath)          
          newRowDistinctDF = distinctValuesDF.join(existingMetadataDF, distinctValuesDF("body.messageSchemaId") === existingMetadataDF("messageSchemaId"), "leftanti")
      }
      
      else {
          newRowDistinctDF = distinctValuesDF
      }
      var metadataCollection = newRowDistinctDF.select("body.messageSchemaId", "body.messageSchema")
                                       .collect()
                                       .map(metadata => metadataSchemaPair(metadata.getAs[String]("messageSchemaId"), metadata.getAs[String]("messageSchema")))                                 

      for(metadata <- metadataCollection) {
        val withSchemaDF = newRowDistinctDF.filter($"body.messageSchemaId" === metadata.messageSchemaId)
                                    .withColumn("message", from_avro($"body.message", metadata.messageSchema))                                

        // Turn the table names into a map, this ensures the schema is more consistent and easier to query if we need to get values such as primary keys
        val withMappedTableNamesDf =  withSchemaDF.select(mapStruct(withSchemaDF.schema, "message.tableStructure.tableColumns."):_*)
                                        .select($"body.messageSchemaId", 
                                                $"message.lineage.schema",
                                                $"message.lineage.table",
                                                $"message.lineage.timestamp",
                                                $"message.tableStructure.tableColumns",
                                                $"message.dataSchema")

        withMappedTableNamesDf.write.mode("append").format("delta").save(metadataStructuredTablePath)    
      }
     batchDF.unpersist()    
  }

// COMMAND ----------

metadataRawDF.writeStream.queryName(s"${systemName}_CopyRawMetadataToDelta").foreachBatch(foreachBatchMethod _).start()

// COMMAND ----------

