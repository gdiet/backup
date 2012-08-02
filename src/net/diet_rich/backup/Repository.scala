package net.diet_rich.backup

import java.io.{File,IOException}
import java.sql.Connection
import net.diet_rich.backup.db.RepositoryInfoDB
import net.diet_rich.util.sql.DBConnection

object RepositoryApp extends App {
  
  if (args.length < 1) throw new IllegalArgumentException("Repository needs at least a repository argument.")
  if (args.length > 1) throw new IllegalArgumentException("Too many arguments.")
  
  Repository create (new File(args(0)))

}

object Repository {
  
  final val DBNAME = "database"
  final val DBDIR = "db"
  final val DATADIR = "data"

  def dbdir(dir: File) = new File(dir, DBDIR)
  def datadir(dir: File) = new File(dir, DATADIR)
    
  def create(dir: File) = {
    if (dbdir(dir) exists) throw new IllegalArgumentException("Repository database folder already exists.")
    if (datadir(dir) exists) throw new IllegalArgumentException("Repository data folder already exists.")
    
    if (!dbdir(dir).mkdirs) throw new IOException("Can't create database folder.")
    if (!datadir(dir).mkdirs) throw new IOException("Can't create data folder.")
    
    val connection = dbConnection(dir)
    RepositoryInfoDB createTables connection
    
    // add database version and hash algorithm (checked to support clone?)
    throw new AssertionError
  }

  def dbConnection(dir: File): Connection = {
    if (!dbdir(dir).exists) throw new IOException("Database folder does not exist.")
    val dbfile = new File(dbdir(dir), DBNAME)
    DBConnection.h2FileDB(dbfile)
  }
  
  
}