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

package com.spark.optimization.exec.QueryGroupByCount

import com.spark.optimization.exec.AbstractJob

/** QueryGroupByCount */

object QueryGroupByCountOptJob extends AbstractJob {

  def main(args: Array[String]): Unit = {
    spark.sql(query).show()

    Thread.sleep(4000000)
    spark.stop()
  }

  override val query: String =
    """
      |Select table1.topic, count(1) FROM enron_data as table1 join enron_data table2
      |where (table1.User_Name=table2.User_Name
      |and table1. `From` = 'From: arsystem@mailman.enron.com'
      |and table1.Mime_Version='Mime-Version: 1.0'
      |and table2.to = 'To: k..allen@enron.com')
      |or
      | (table1.User_Name=table2.User_Name
      |and table1. `From` = 'From: monica.l.brown@accenture.com'
      |and table1.Mime_Version='Mime-Version: 1.0'
      |and table2.to = 'To: pallen@enron.com'
      |)
      |group by table1.topic
    """.stripMargin
}
