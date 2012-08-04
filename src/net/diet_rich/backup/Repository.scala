package net.diet_rich.backup

import java.io.{File,IOException}
import java.sql.Connection
import net.diet_rich.backup.db.RepositoryInfoDB
import net.diet_rich.util.sql.DBConnection

object RepositoryApp extends App {
  
  if (args.length != 2) throw new IllegalArgumentException("Arguments: <repository> <hash algorithm>")
  
  Repository create (new File(args(0)), args(1))

}

object Repository {
  
  final val DBNAME = "database"
  final val DBDIR = "db"
  final val DATADIR = "data"

  final val DBVERSIONKEY = "database version"
  final val DBVERSION = "1.0"
  final val HASHALGORITHMKEY = "hash algorithm"
    
  def dbdir(dir: File) = new File(dir, DBDIR)
  def datadir(dir: File) = new File(dir, DATADIR)
    
  def create(dir: File, hashAlgorithm: String) = {
    HashProvider.digester(hashAlgorithm) // just to check, create and discard
    
    if (dbdir(dir) exists) throw new IllegalArgumentException("Repository database folder already exists.")
    if (datadir(dir) exists) throw new IllegalArgumentException("Repository data folder already exists.")
    
    if (!dbdir(dir).mkdirs) throw new IOException("Can't create database folder.")
    if (!datadir(dir).mkdirs) throw new IOException("Can't create data folder.")
    
    val connection = dbConnection(dir)
    RepositoryInfoDB createTables connection
    RepositoryInfoDB add (connection, DBVERSIONKEY, DBVERSION)
    RepositoryInfoDB add (connection, HASHALGORITHMKEY, hashAlgorithm)
    
    // add hash algorithm (checked to support clone?)
    throw new AssertionError
  }

  private def dbConnection(dir: File): Connection = {
    if (!dbdir(dir).exists) throw new IOException("Database folder does not exist.")
    val dbfile = new File(dbdir(dir), DBNAME)
    DBConnection.h2FileDB(dbfile)
  }

  def connectToDB(dir: File): Connection = {
    val connection = dbConnection(dir)
    val dbversion = RepositoryInfoDB read (connection, DBVERSIONKEY)
    if (dbversion != Some(DBVERSION)) throw new IllegalStateException(
          "expected database version <%s> instead of <%s>" format (DBVERSION, dbversion.getOrElse("undefined"))
    )
    connection
  }

  def readHashAlgorithm(connection: Connection): String =
    RepositoryInfoDB read (connection, DBVERSIONKEY) get
  
}