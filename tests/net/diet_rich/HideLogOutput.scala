// Copyright (c) 2011 Georg Dietrich
// Licensed under the MIT license: http://www.opensource.org/licenses/mit-license.php
package net.diet_rich

class HideLogOutput {

  @org.testng.annotations.BeforeSuite
  def hideLogOuput : Unit = {
    import net.diet_rich.util.logging.{Logged,Logger}
   
    Logged.defaultLogListener = Some(Logger.NULLLOGGER)
  }

}