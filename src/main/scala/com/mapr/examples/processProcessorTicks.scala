/*

   Copyright 2019 MapR Technologies

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

*/
package com.mapr.examples
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import com.mapr.db.spark.streaming.MapRDBSourceConfig

object processProcessorTicks {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("ProcessProcessorTicks")
      .getOrCreate()

    import spark.implicits._

    val processTickSchema = new StructType()
      .add("contextSwitches", new StructType().add("$numberLong", LongType))
      .add("cpu64bit", BooleanType)
      .add("family", StringType)
      .add("identifier", StringType)
      .add("interrupts", new StructType().add("$numberLong", LongType))
      .add("logicalProcessorCount", new StructType().add("$numberLong", LongType))
      .add("model", StringType)
      .add("name", StringType)
      .add("physicalPackageCount", new StructType().add("$numberLong", LongType))
      .add("physicalProcessorCount", new StructType().add("$numberLong", LongType))
      .add("processorCpuLoadBetweenTicks", ArrayType(DoubleType, true))


    val lines = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "maprdemo.mapr.io:9092")
      .option("subscribe", "/demo/streams/sensorDataChangeLog:sensor_data")
      .option("checkpointLocation", "/tmp")
      .option("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
      .load()
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]

    // The JSON Object is returned as a string in lines
    // Therefore I use get_json_object to read out a section of the string as a json object

    val query = lines
      .select(
        get_json_object(col("value"), "$._id").alias("id"),
        get_json_object(col("value"), "$.$opType").alias("opType"),
        col("value"),
        get_json_object(col("value"), "$.$$document.generation_ts.$numberLong").alias("generation_ts"),
        get_json_object(col("value"), "$.$$document.processor").alias("p"))
      .filter(col("opType") === "$RECORD_INSERT")
      .select(
        col("value"),
        concat_ws("_", col("id"), col("generation_ts")).alias("_id"),
        col("id").alias("hostname"),
        col("generation_ts"),
        from_json(col("p"), processTickSchema).alias("p1")
      )
      .select(
        col("value"),
        col("_id"),
        col("hostname"),
        col("generation_ts"),
        col("p1.processorCpuLoadBetweenTicks")(0).alias("proc_clbt0"),
        col("p1.processorCpuLoadBetweenTicks")(1).alias("proc_clbt1"),
        col("p1.processorCpuLoadBetweenTicks")(2).alias("proc_clbt2"),
        col("p1.processorCpuLoadBetweenTicks")(3).alias("proc_clbt3"),
        col("p1.processorCpuLoadBetweenTicks")(4).alias("proc_clbt4"),
        col("p1.processorCpuLoadBetweenTicks")(5).alias("proc_clbt5"),
        col("p1.processorCpuLoadBetweenTicks")(6).alias("proc_clbt6"),
        col("p1.processorCpuLoadBetweenTicks")(7).alias("proc_clbt7")
      )

    //    val a = query.writeStream.format("console").option("truncate",false).option("numRows",10).start()
    val b = query
      .writeStream
      .format(MapRDBSourceConfig.Format)
      .option(MapRDBSourceConfig.TablePathOption, "/demo/tables/processorCPULoadBetweenTicks")
      .option(MapRDBSourceConfig.IdFieldPathOption, "_id")
      .option(MapRDBSourceConfig.CreateTableOption, false)
      .option(MapRDBSourceConfig.BulkModeOption, false)
      .option(MapRDBSourceConfig.SampleSizeOption, 1000)
      .option("checkpointLocation", "/tmp")
      .outputMode("append")
      .start()

    //   a.awaitTermination()
    b.awaitTermination()

  }
}
