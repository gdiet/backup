package net.diet_rich.backup.database

object Tryout extends App {

  trait A1 {
    def a: Unit = println("A1")
  }

  trait A2 extends A1 {
    override def a: Unit = { println("A2"); super.a }
  }
 
  new A2 {
    
  }.a
  
}