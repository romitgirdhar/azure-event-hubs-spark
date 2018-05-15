package org.apache.spark.sql.azuresql

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.{OutputMode, StreamTest, StreamingQuery}
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest.time.Span

class AzureSQLSinkTest extends StreamTest with SharedSQLContext {
  import testImplicits._
//TODO: Start here. Start creating the test and then run it.
  override val streamingTimeout: Span = 30

  override def beforeAll(): Unit = {
    super.beforeAll()
    //Your code here
  }

  override def afterAll(): Unit = {
    //Your code here
    super.afterAll()

  }

  private def createReader(testFileLocation: String): DataFrame = {
    //TODO: Prep test data
    val testschema = StructType(
      StructField("rideId", StringType) ::
        StructField("trainId", StringType):: Nil)
    spark.readStream.schema(testschema).json(testFileLocation)
  }

  /*private def createWriter(inputDF: DataFrame, sqlConfig: AzureSQLConfig, withOutputMode: Option[OutputMode]): StreamingQuery = {
    inputDF.writeStream.format("azuresql").option("directCopy","true").option("")
  }*/

  test("Structured Streaming - Write to Azure SQL DB with Connection String"){
    val df = createReader("/sqltestdata")
    //TODO: Create Util functions to create SQL DB, table and truncate it and call the functions here
    df.writeStream.format("azuresql").option("directCopy","true").option("connectionString", "").option("table","testtable").start()
    //TODO: Read data from SQL DB and check if it matches the data written (see EH implementation)
  }

  test("Structured Streaming - Write to Azure SQL DB with variables defined"){

  }

  test("Structured Streaming - Write to Azure SQL DB with Connection String & variables defined"){

  }

  test("Structured Streaming - Incorrect username/password"){

  }

  test("Structured Streaming - Incorrect Server Name"){

  }

  test("Structured Streaming - Incorrect Database Name"){

  }

  test("Structured Streaming - Incomplete options defined"){

  }

  test("Structured Streaming - Incorrect Connection String"){

  }

  test("Structured Streaming - Table does not exist"){

  }
}
