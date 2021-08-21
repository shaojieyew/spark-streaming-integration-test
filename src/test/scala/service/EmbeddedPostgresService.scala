package service

import java.io.File

import ru.yandex.qatools.embed.postgresql.EmbeddedPostgres
import ru.yandex.qatools.embed.postgresql.distribution.Version

import scala.io.Source
import scala.reflect.io.Directory

class EmbeddedPostgresService(port:Int = 5432) {

  var postgres:EmbeddedPostgres = _
  var url: String = _
  def start(): Unit ={
    new Directory(new File("tmp/pgsql/data")).deleteRecursively()
    postgres = new EmbeddedPostgres(Version.V10_7, "tmp/pgsql/data", "src/test/resources/pgsql","tmp/pgsql/service")
    url = postgres.start("localhost", port, "dbName", "userName", "password")
  }

  def stop(): Unit ={
    postgres.stop()
  }

  def clear(): Unit ={
    executeQuerySql("select 'drop table \"' || tablename || '\" cascade;' from pg_tables;")
  }
  def truncate(table:String): Unit ={
    executeSql("TRUNCATE "+table+";")
  }

  def executeFile(filePath:String ): Unit ={
    executeSql(Source.fromFile(filePath).getLines().mkString(System.lineSeparator()))
  }

  def executeQueryFile(filePath:String ): Unit ={
    executeQuerySql(Source.fromFile(filePath).getLines().mkString(System.lineSeparator()))
  }

  def executeSql(sql: String ): Unit ={
    import java.sql.DriverManager
    val conn = DriverManager.getConnection(url)
    try{
      conn.createStatement.execute(sql)
      println(sql)
    } finally {
      conn.close
    }
  }
  def executeQuerySql(sql: String ): Unit ={
    import java.sql.DriverManager
    val conn = DriverManager.getConnection(url)
    try{
      conn.createStatement.executeQuery(sql)
      println(sql)
    } finally {
      conn.close
    }
  }
}