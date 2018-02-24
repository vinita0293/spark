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
package org.apache.spark.examples

import org.apache.spark.sql.SparkSession


/** Computes an approximation to pi */


object Cute {
  def main(args: Array[String]) {

//    System.setProperty("hive.metastore.uris"uris, "thrift://localhost:9083")

    val spark = SparkSession
      .builder()
      .master("local[4]")
      .appName("Spark Pi")
//      .enableHiveSupport()
      .getOrCreate()

    val query1 =
      """select * from  one1 as table1 join   one2 as table2
        |where  (table1.User_Name=table2.User_Name
        |and  (table1.`From`='1111')
        |and  cast(table1.Mime_Version as int)
        |and  table2.Message_ID='yyyy'
        |and  table2.File_No='11'
        |)
        |or
        |(table1.User_Name=table2.User_Name
        |and  table1.`From`='1111'
        |and  cast(table1.Mime_Version as int)
        |and  table2.Message_ID='yyyy'
        |and  table2.File_No='22')""".stripMargin


    val query2 =
      """select Topic from  one1 as table1 join   one2 as table2
        |where  (table1.User_Name=table2.User_Name
        |and  table1.`From`='1111'
        |and  table1.Mime_Version='343'
        |and  table2.Message_ID='xxxx'
        |and  table2.File_No='11'
        |)
        |or
        |(table1.User_Name=table2.User_Name
        |and  table1.`From`='1111'
        | or   table2.Message_ID='xxxx'
        |and  table2.File_No='22'
        |)""".stripMargin

    val query3 =
      """select Topic from  one1 as table1 join   one2 as table2
        |where table1.User_Name=table2.User_Name
        |and  table1.`From`='1111'
        |and  table1.Mime_Version='343'
        |and  table2.Message_ID='xxxx'
        |or  (table2.Message_ID='yyyy' and table2.File_No='11')
         """.stripMargin

    val query4 =
      """select Topic from  one1 as table1 join   one2 as table2
        |where table1.User_Name=table2.User_Name
        |and  table1.`From`='1111'
        |and  table1.Subject='xxxx'
        |or  (table1.Subject='xxxx' or table1.Mime_Version='343')
      """.stripMargin

//((User_Name#25 = User_Name#34) && (From#26 = 1111)) || (Mime_Version#30 = 343))

    spark.sql(query4).explain(false)
    spark.stop()
  }
}
