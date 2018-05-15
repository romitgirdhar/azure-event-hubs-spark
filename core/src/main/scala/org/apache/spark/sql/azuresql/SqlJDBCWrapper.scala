package org.apache.spark.sql.azuresql


import java.sql.{Connection, Driver, DriverManager, PreparedStatement, ResultSet, ResultSetMetaData, SQLException}
import java.util.Properties

import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils
import scala.collection.JavaConverters._

private[azuresql] class SqlJDBCWrapper extends Logging {
  private val DRIVER_NAME = "com.microsoft.sqlserver.jdbc.SQLServerDriver"

  /* Checks to see if the driver exists. If not, throws an error. For now, the assumption is that "sqlserver" sub-protocol is used. */
  private def getDriver(
                       userSpecifiedDriverClass: Option[String]
                       ): String = {
    userSpecifiedDriverClass.getOrElse{
      try {
        Utils.classForName(DRIVER_NAME).getName
      }
      catch {
        case e: ClassNotFoundException =>
          throw new ClassNotFoundException(
            "Could not find the Microsoft SQL JDBC Driver. Please add the library to your Spark cluster before continuing", e
          )
      }
    }
  }


  def setupConnection(
                       jdbcUrl: String,
                       jdbcUsername: String,
                       jdbcPassword: String,
                       userSpecifiedDriverClass: Option[String]
                     ): Connection = {
    val driverClass = getDriver(userSpecifiedDriverClass)
    val driverWrapper = DriverManager.getDrivers.asScala.collectFirst{  //TODO: See if this works. Or if this can be simplified by just doing a DriverManager.getConnection()
      case d if d.getClass.getCanonicalName == driverClass => d
    }.getOrElse{
      throw new IllegalArgumentException(s"Did not find the correct driver specified: $driverClass")
    }

    val connectionProperties = new Properties()
    connectionProperties.put("user", s"$jdbcUsername")
    connectionProperties.put("password", s"$jdbcPassword")

    driverWrapper.connect(jdbcUrl, connectionProperties)
  }

  /*
  Simple execute command method. Execute the prepared statement.
   */

  def executeCmd(
                  statement: PreparedStatement
                ): Boolean = {
    try{
      statement.execute()   //TODOv2: Create a new thread and wrap it in a Runnable/callable to increase performance
    }
    catch{
      case s: SQLException =>
        log.error("Encountered SQLException while executing query: "+ s)
        throw s
      case e: Exception =>
        log.error("Encountered exception while writing to SQL DB: "+ e)
        throw e
    }

  }
}
