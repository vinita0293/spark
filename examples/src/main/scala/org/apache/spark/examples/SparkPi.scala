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

// scalastyle:off println
package org.apache.spark.examples

import scala.math.random

import org.apache.spark.sql.SparkSession

/** Computes an approximation to pi */
object SparkPi {
  def main(args: Array[String]) {

    System.setProperty("hive.metastore.uris", "thrift://localhost:9083")

    val spark = SparkSession
      .builder()
      .master("local[4]")
      .appName("Spark Pi")
      .enableHiveSupport()
      .getOrCreate()

    val slices = if (args.length > 0) args(0).toInt else 2
    val n = math.min(100000L * slices, Int.MaxValue).toInt // avoid overflow
    val count = spark.sparkContext.parallelize(1 until n, slices).map { i =>
      val x = random * 2 - 1
      val y = random * 2 - 1
      if (x*x + y*y < 1) 1 else 0
    }.reduce(_ + _)
    println("Pi is roughly " + 4.0 * count / (n - 1))





     val query1 =
      """select Topic   from  one1 as table1 join   one2 as table2
        |where  (table1.User_Name=table2.User_Name
        |and  table1.`From`='From: 1.11913372.-2@multexinvestornetwork.com'
        |and  table1.Subject="Subject: December 14, 2000 - Bear Stearns' predictions for"
        |and  table1.Mime_Version='Mime-Version: 1.0'
        |and table1.Content_Type="12345"
        |and  table2.`Date`='Date: Mon, 9 Apr 2001 '
        |and  table2.Message_ID='Message-ID: <12406231.1075848299564.JavaMail.evans@thyme>'
        |and  table2.File_No='1.')
        | or
        | (table1.User_Name=table2.User_Name
        |and  table1.`From`='From: jennifer.k.dickinson@usa.conoco.com'
        |and  table1.Subject='Subject: hi'
        |and  table1.Mime_Version='Mime-Version: 1.0'
        |and table1.Content_Type="6789"
        |and  table2.`Date`='Date: Mon, 9 Apr 2001 '
        |and  table2.Message_ID='Message-ID: <12406231.1075848299564.JavaMail.evans@thyme>'
        |and  table2.File_No='2.') """.stripMargin


   /* val one= spark.sql( """select `From`,User_Name from one1 as table1
                                |where  (table1.`From`='From: 1.11913372.-2@multexinvestornetwork.com'
                                |and  table1.Subject="Subject: December 14, 2000 - Bear Stearns' predictions for"
                                |and  table1.Mime_Version='Mime-Version: 1.0'
                                |and table1.Content_Type="12345" )
                                |or (table1.`From`='From: jennifer.k.dickinson@usa.conoco.com'
                                |and  table1.Subject='Subject: hi'
                                |and  table1.Mime_Version='Mime-Version: 1.0'
                                |and table1.Content_Type="6789")""".stripMargin)



    val two  = spark.sql("""select User_Name,Topic,File_No from  one2 as table2
                                 |where  (table2.`Date`='Date: Mon, 9 Apr 2001 '
                                 |and    table2.Message_ID='Message-ID: <12406231.1075848299564.JavaMail.evans@thyme>'
                                 |and    (table2.File_No='1.' or table2.File_No='2.' ))""".stripMargin)

    one.createOrReplaceTempView("table1filter")
    two.createOrReplaceTempView("table2filter")

    val finaltable ="""select Topic from table1filter , table2filter
                      |where  (table1filter.User_Name=table2filter.User_Name
                      |and table1filter.`From`='From: 1.11913372.-2@multexinvestornetwork.com'
                      |and  table2filter.File_No='1.')
                      |or (table1filter.User_Name=table2filter.User_Name
                      |and  table1filter.`From`='From: jennifer.k.dickinson@usa.conoco.com'
                      |and  table2filter.File_No='2.')""".stripMargin

*/

    val df=spark.sql(query1)

    df.show(false)
    Thread.sleep(40000)
    spark.stop()
  }
}







