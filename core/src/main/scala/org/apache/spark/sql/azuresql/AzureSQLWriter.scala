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

package org.apache.spark.sql.azuresql

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.execution.QueryExecution
import java.sql.{Connection, DriverManager, SQLException}
import java.util.Properties

import org.apache.spark.sql.types.StructType

/**
  * The [[AzureSQLWriter]] class is used to write data from a batch query
  * or structured streaming query, given by a [[QueryExecution]], to Azure SQL Database or Azure SQL Data Warehouse.
  */
private[spark] object AzureSQLWriter extends Logging {
  val DRIVER_NAME: String = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
  var connection:java.sql.Connection = _
  var statement:java.sql.Statement = _
  var subset = false //This variable identifies whether you want to write to all columns of the SQL table or just select few.

  override def toString: String = "AzureSQLWriter"

  def write(
             sparkSession: SparkSession,
             data: DataFrame,
             queryExecution: QueryExecution,
             saveMode: SaveMode,
             parameters: Map[String, String]
           ): Unit = {
    if(parameters.isEmpty){
      throw new IllegalArgumentException("When writing to Azure SQL DB, connection information is required.")
    }

    val sqlConf:AzureSQLConfig = AzureSQLConfig.load(parameters)

    if(parameters.get("directCopy") == "true"){
      loadSqlData(connection, false, sqlConf, data)
    } else {
      val table = sqlConf.tableName
      val jdbcWrapper: SqlJDBCWrapper = new SqlJDBCWrapper

      //TODO: Remove the error below.
      throw new SQLException("Currently only directCopy supported")

      connection = jdbcWrapper.setupConnection(sqlConf.connectionString, sqlConf.username, sqlConf.password, Option(DRIVER_NAME))   //TODOv2: Hard-coding the driver name for now
      connection.setAutoCommit(false)
      val mappedData = dataMapper(connection, table, data) //TODO: Handle data type conversions smoothly & check table existence, checks column names and types and map them to dataframe columns
      val schema = mappedData.schema
      if(data.schema == mappedData.schema){     //TODOv2: Instead of this, read from the params, so we don't have to call jdbcWrapper or dataMapper.
        subset = true;
      }
      loadSqlData(connection, subset, sqlConf, mappedData)
      connection.commit()
    }

    //TODOv2: Provide the option to the user to define the columns they're writing to and/or column mapping
    //TODOv2: Handle creation of tables and append/overwrite of tables ; for v1, only append mode
  }

  /*
  This method checks to see if the table exists in SQL. If it doesn't, it creates the table. This method also ensures that the data types of the data frame are compatible with that of Azure SQL database. If they aren't, it converts them and returns the converted data frame
   */
  def dataMapper(
                conn: Connection,
                table: String,
                df: DataFrame
                         ): DataFrame = {
    return df   //Placeholder
  }

  /*
  Prepares the Insert statement and calls the [[SqlJDBCWrapper.executeCmd]]
   */
  def loadSqlData(
                 conn: Connection,
                 subset: Boolean,
                 conf: AzureSQLConfig,
                 data: DataFrame
                 ): Unit = {
    if(subset){
      var schemaStr:String = ""
      val schema = data.schema
      schema.fields.foreach{
        schemaStr += _.name + ","
      }
      schemaStr.substring(0,schemaStr.length-2)
      //val insertStatement = s"INSERT INTO $table ("+ schemaStr + ") VALUES ("
      //TODO: Handle append mode
    } else {
      val connectionProperties = new Properties()
      connectionProperties.put("user", conf.username)
      connectionProperties.put("password", conf.password)
      connectionProperties.put("driver", DRIVER_NAME)
      try{
        data.write.mode(SaveMode.Append).jdbc(conf.connectionString, conf.tableName, connectionProperties)
      } catch {
        case e: Exception =>
          log.error("Error writing batch data to SQL DB. Error details: "+ e)
          throw e
      }
    }
  }
}


/*
val jdbc_url = s"jdbc:sqlserver://${serverName}:${jdbcPort};database=${database};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"


//Creating Properties
    val connectionProperties = new Properties()
    connectionProperties.put("user", s"$username")
    connectionProperties.put("password", s"$password")
    connectionProperties.put("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")

    //TODO: Write data to SQL DB

    val serverName = parameters.get(SERVER_KEY).map(_.trim)
    val portNumber = parameters.get(PORT_KEY).map(_.trim).flatMap(s => Try(s.toInt).toOption)
    val database = parameters.get(DB_KEY).map(_.trim)
    val username = parameters.get(USER_KEY).map(_.trim)
    val password = parameters.get(PWD_KEY).map(_.trim)
    val tableName = parameters.get(TABLE_KEY).map(_.trim)


 */