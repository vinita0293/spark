/*
 | * Licensed to the Apache Software Foundation (ASF) under one or more
 | * contributor license agreements.  See the NOTICE file distributed with
 | * this work for additional information regarding copyright ownership.
 | * The ASF licenses this file to You under the Apache License, Version 2.0
 | * (the "License"); you may not use this file except in compliance with
 | * the License.  You may obtain a copy of the License at
 | *
 | *    http://www.apache.org/licenses/LICENSE-2.0
 | *
 | * Unless required by applicable law or agreed to in writing, software
 | * distributed under the License is distributed on an "AS IS" BASIS,
 | * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 | * See the License for the specific language governing permissions and
 | * limitations under the License.
 | */
// scalastyle: off println

package com.spark.optimization.parser

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{lit, udf}

object PrepareData extends App {

  System.setProperty("hive.metastore.uris", "thrift://localhost:9083")

  val spark = SparkSession
    .builder()
    .master("local[6]")
    .appName("Spark Pi")
    .enableHiveSupport()
    .getOrCreate()

  import spark.sqlContext.implicits._

  spark.sparkContext.hadoopConfiguration
    .set("mapreduce.input.fileinputformat.input.dir.recursive", "true")

  // --------training
  val resultantRDD = spark.sparkContext
    .wholeTextFiles("/Volumes/MacintoshHD2/Shanks_Files/Shanks_Repo/Data/sample_maildircopy", 8)
    .map(x => (x._1, x._2.replace("\n", ""))).map(EnronUtils.EnronParser)

  val randomNoUDF = udf { number: Int => new scala.util.Random().nextInt(999999999) % 100 }

  val resultantDF = resultantRDD.toDF()
  resultantDF.withColumn("part_col", randomNoUDF(lit(1)))
    .write.partitionBy("part_col").mode(SaveMode.Overwrite).saveAsTable("enron_data_large")
}
