// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.backup

object BackupApp extends App {
  if (args.length < 3) throw new IllegalArgumentException("Backup needs at least source, repository and target arguments")
  if (args.length > 3) throw new IllegalArgumentException("Too many arguments")
  
  new Backup(args(0), args(1), args(2))
}

class Backup(source: String, repository: String, target: String) {
  import Backup._
  
  throw new UnsupportedOperationException // FIXME
}

object Backup {
}
