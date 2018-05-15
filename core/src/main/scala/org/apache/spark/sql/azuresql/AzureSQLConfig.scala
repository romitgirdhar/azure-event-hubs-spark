package org.apache.spark.sql.azuresql

import java.sql.SQLException
import java.util.Locale
import java.util.regex.Pattern

import org.apache.spark.internal.Logging
import org.apache.spark.sql.azuresql

import scala.util.Try
//Parse the configuration and return an object with jdbc_url, usernamer, pwd

class AzureSQLConfig ()
  extends Serializable
    with Logging {

  import AzureSQLConfig._

  private var server: String = _
  private var database: String = _
  var username: String = _  //Trying to see if I can just send an object of this case classe with these 3 public variables that will be accessible in AzureSQLWriter
  var password: String = _
  private var portNumber: Int = 1433 //You should not be able to access these properties externally. Test this
  var tableName: String = _
  private var encryptKey: Boolean = true
  private var trustServerCert: Boolean = false
  private var hostNameInCert: String = "*.database.windows.net"
  private var loginTimout: Int = 30
  var connectionString: String = _

  private def this (connectionStr: String, tableName: String, userName: Option[String], password: Option[String]){
    //TODO: Check if the connection string is valid and parse out other details
    this()
    parse(connectionStr, userName, password)
    this.tableName = tableName
  }

  private def this(serverName: String, db: String, username: String, pwd: String, table: String, portNumber: Option[String], encrypt: Option[String], trustServerCert: Option[String], hostNameInCert: Option[String], loginTimeout: Option[String]) {
    this()
    this.server = serverName
    this.database = db
    this.username = username
    this.password = pwd
    this.tableName = table
    if(portNumber.isDefined){
      this.portNumber = Try(portNumber.get.toInt).getOrElse(this.portNumber)
    }
    if(encrypt.isDefined){
      this.encryptKey= Try(encrypt.get.toBoolean).getOrElse(this.encryptKey)
    }
    if(trustServerCert.isDefined){
      this.trustServerCert = Try(trustServerCert.get.toBoolean).getOrElse(this.trustServerCert)
    }
    if(hostNameInCert.isDefined){
      this.hostNameInCert = hostNameInCert.get
    }
    if(loginTimeout.isDefined){
      this.loginTimout = Try(loginTimeout.get.toInt).getOrElse(this.loginTimout)
    }

    this.connectionString = s"jdbc:sqlserver://${this.server}:${this.portNumber};database=${this.database};encrypt=${this.encryptKey};trustServerCertificate=${this.trustServerCert};hostNameInCertificate=${this.hostNameInCert};loginTimeout=${this.loginTimout};"
  }

  private def parse(connStr: String, user: Option[String], pass: Option[String]): Unit = {
    if (connectionString.isEmpty || connectionString != null || connectionString.trim.isEmpty) {
      log.error("Illegal Connection String encountered")
      throw new SQLException("connectionString cannot be empty")
    }

    val connection = if (connectionString takeRight 1 equals ";") {
      connectionString dropRight 1
    } else {
      connectionString
    }

    val svrNameParse = connection.stripPrefix("jdbc:").split(":")
    val subProtocol = svrNameParse(0)   //Note: For now just supporting sqlserver. But, this can support mysql and postgres down the road.

    if(subProtocol.equals("sqlserver")){
      //Parsing the Server name and the port number
      try{
        this.server = svrNameParse(1).stripPrefix("//")
      } catch {
        case e: SQLException =>
          throw new SQLException(s"Malformed Connection String. The Connection String must be in the format jdbc:sqlserver://<HOST>:<PORT>;database=<DATABASE>")
      }

      this.portNumber = Try(svrNameParse(2).toInt).getOrElse(this.portNumber)

      val keyValuePattern = Pattern.compile(KeysWithDelimitersRegex, Pattern.CASE_INSENSITIVE)
      val values = keyValuePattern.split(connection)
      val keys = keyValuePattern.matcher(connection)

      if (values == null || values.length <= 1 || keys.groupCount == 0) {
        log.error("Connection String cannot be parsed.")
        throw new SQLException("Connection String cannot be parsed.")
      }

      if (values(0) != null && !values(0).isEmpty && !values(0).trim.isEmpty) {
        log.error(String.format(Locale.US, "Cannot parse part of ConnectionString: %s", values(0)))
        throw new SQLException(
          String.format(Locale.US, "Cannot parse part of ConnectionString: %s", values(0)))
      }

      //Ensuring we have the correct number of keys and values
      var valueIndex: Int = 0
      while (keys.find) {
        valueIndex += 1
        var key = keys.group
        key = key.substring(1, key.length - 1)

        if (values.length < valueIndex + 1) {
          throw new Exception(
            s"Value for the connection string parameter name: $key, not found")
        }

        if (key.equalsIgnoreCase(DatabaseNameKey)) {
          this.database = values(valueIndex)
        } else if (key.equalsIgnoreCase(UsernameKey)) {
          this.username = values(valueIndex)
        } else if (key.equalsIgnoreCase(PasswordKey)) {
          this.password = values(valueIndex)
        } else if (key.equalsIgnoreCase(EncryptKey)) {
          this.encryptKey = Try(values(valueIndex).toBoolean).getOrElse(this.encryptKey)
        } else if (key.equalsIgnoreCase(TrustServerCertificateKey)) {
          this.trustServerCert = Try(values(valueIndex).toBoolean).getOrElse(this.trustServerCert)
        } else if (key.equalsIgnoreCase(HostNameInCertificateKey)) {
          this.hostNameInCert = Try(values(valueIndex)).getOrElse(this.hostNameInCert)
        } else if (key.equalsIgnoreCase(LoginTimeoutKey)) {
          this.loginTimout = Try(values(valueIndex).toInt).getOrElse(this.loginTimout)
        }
      }

      if(user.isDefined){
        this.username = Try(user.get).getOrElse(this.username)
      }
      if(pass.isDefined){
        this.password = Try(pass.get).getOrElse(this.password)
      }
      if(this.username == "" || this.password == "" || this.username == null && this.password == null){
        log.error("Username and/or password is missing. unable to connect to the SQL DB")
        throw new SQLException("Username and/or password is missing. unable to connect to the SQL DB")
      }

      this.connectionString = s"jdbc:sqlserver://${this.server}:${this.portNumber};database=${this.database};encrypt=${this.encryptKey};trustServerCertificate=${this.trustServerCert};hostNameInCertificate=${this.hostNameInCert};loginTimeout=${this.loginTimout};"

    } else {
      log.error("Only SQL Server protocol (sqlserver://) supported currently.")
      throw new SQLException("Only SQL Server protocol (jdbc:sqlserver://) supported currently.")
    }

  }
}

object AzureSQLConfig extends Logging {

  // Option key values => You could use a combination of these keys
  private val ServerKey = "server"
  private val DatabaseNameKey = "database"
  private val UsernameKey = "username"
  private val PasswordKey = "password"
  private val PortNumberKey = "port"
  private val TableNameKey = "table"
  private val EncryptKey = "encrypt"
  private val TrustServerCertificateKey = "trustServerCertificateKey"
  private val HostNameInCertificateKey = "hostNameInCertificateKey"
  private val LoginTimeoutKey = "loginTimeout"
  private val ConnectionStringKey = "connectionString"
  private val DirectMappingKey = "directMapping"
  private val TableExistsKey = "tableExists"

  private val KeyValueSeparator = "="
  private val KeyValuePairDelimiter = ";"
  private val SqlProtocolDelimiter = ":"
  private val AllKeyEnumerateRegex = "(" + DatabaseNameKey + "|" + UsernameKey + "|" +
    PasswordKey + "|" + EncryptKey + "|" + TrustServerCertificateKey +
    "|" + HostNameInCertificateKey + "|" + LoginTimeoutKey + "|" + ")"
  private val KeysWithDelimitersRegex = KeyValuePairDelimiter + AllKeyEnumerateRegex + KeyValueSeparator
  //private val SqlStringDelimiter = "(" + SqlProtocolDelimiter + "|" + KeysWithDelimitersRegex + ")"

  def load(parameters: Map[String, String]): AzureSQLConfig = {
    var config = _
    if(parameters.get(TableNameKey).isDefined){
      if(parameters.get(ConnectionStringKey).isDefined){
        config = new AzureSQLConfig(parameters.get(ConnectionStringKey).get, parameters.get(TableNameKey).get, parameters.get(UsernameKey), parameters.get(PasswordKey))
      } else if (parameters.get(ServerKey).isDefined && parameters.get(DatabaseNameKey).isDefined && parameters.get(UsernameKey).isDefined && parameters.get(PasswordKey).isDefined) {
        //parameters.
        config = new AzureSQLConfig(serverName = parameters.get(ServerKey).get,
          db = parameters.get(DatabaseNameKey).get,
          username = parameters.get(UsernameKey).get,
          pwd = parameters.get(PasswordKey).get,
          table = parameters.get(TableNameKey).get,
          parameters.get(PortNumberKey),
          parameters.get(EncryptKey),
          parameters.get(TrustServerCertificateKey),
          parameters.get(HostNameInCertificateKey),
          parameters.get(LoginTimeoutKey))
      } else {
        log.error(s"Parameters not defined correctly. Either define the Connection String or the server, db, credentials along with table name")
      }
    } else {
      log.error(s"Table name not defined. Please define a table name to write to by adding an option for $TableNameKey")
      throw new Exception(s"Table name not defined. Please define a table name to write to by adding an option for $TableNameKey")
    }

    return config
  }
}

//val jdbc_url = s"jdbc:sqlserver://${serverName}:${jdbcPort};database=${database};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"
