// Copyright (c) 2011 Georg Dietrich
// Licensed under the MIT license: http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.backup.storelogic

import net.diet_rich.util.logging.Logged

/*
 * c0  c1  c2  c3  c4                                                          c19
 *  #   #   #   #   #   #   #   #   #   #   #   #   #   #   #   #   #   #   #   # 
 *  0   5  10                                                                      r0
 *  1   6  11                                                                      r1
 *  2   7  12                                                                      r2
 *  3   8  13                                                                      r3
 *  4   9                                                                      99  r4
 * 100 105 110 
 * 101 106 111 
 * 102 107 112
 * 103 108 113
 * 104 109
 * 200 205 210 
 * 201 206 211 
 * 202 207 212
 * 203 208 213
 * 204 209
 * 300 305 310 
 * 301 306 311 
 * 302 307 312
 * 303 308 313
 * 304 309                                                                     399 r19
 */
trait DataFilesPattern extends Logged {
  import DataFilesPattern._

  // methods to implement when trait is used
  
  // implemented logic
  
  // FIXME write some tests
  
  /** @return the error correction row and column file names for a given data file. */
  def ecNamesForFileID(fileID: Long) : (String, String) = {
    require(fileID >= 0)
    
    val base = fileID - fileID % 400
    val column = fileID % 100 / 5
    val row = fileID % 5 + 5 * ((fileID % 400) / 100)
    val columnName = "ec_%15d_c%2d".formatLocal(java.util.Locale.US, base, column)
    val rowName = "ec_%15d_r%2d".formatLocal(java.util.Locale.US, base, row)
    (columnName, rowName)
  }
  
  /** @return the file name for a given data file. */
  def nameForFileID(fileID: Long) : String = "%15d".formatLocal(java.util.Locale.US, fileID)

  /** @return the data file IDs protected by an error correction file. */
  def idsForECname(ecName: String) : Seq[Long] = {
    val parts = ecName.split('_')
    require(parts(0) == "ec")
    val base = Integer.parseInt(parts(1))
    val colRow = parts(1)(1)
    val index = Integer.parseInt(parts(1).substring(1))
    if (colRow == 'c') {
      for (n <- Seq.range(0, 300, 100); m <- 0 to 4) yield m + n + index * 5L
    } else if (colRow == 'r') {
      for (n <- Seq.range(0, 100, 5)) yield n + index % 5 + index / 5 * 100L
    } else {
      throwError(new IllegalFileNameException, "illegal EC file name", ecName)
    }
  }
  
}

object DataFilesPattern {
  class IllegalFileNameException extends Exception
}
