package net.diet_rich.test.backup.alg

import net.diet_rich.backup.alg.SourceEntry

class TestSource {

  val source = """
    root,false
    .branch1,false
    ..leaf,true,17,cont-leaf
    ..leaf1a,true,17,cont-leaf2a
    ..leaf1b,true,17,cont-leaf2a
    .branch2,false
    ..leaf,true,17,cont-leaf
    ..leaf2a,true,17,cont-leaf2a
    ..leaf2b,true,17,cont-leaf2a
    """
    
  def dir(name: String, children: List[SourceEntry]): SourceEntry = throw new UnsupportedOperationException
    
  def parse(lines: Iterator[String], level: Int, currentList: List[SourceEntry]): (List[SourceEntry], Option[String]) =
    if (!lines.hasNext) (currentList, None) else {
      val line = lines.next.trim()
      val lineLevel = line.indexWhere(_ != '.')
      val lineContent = line.substring(lineLevel).split(',')
      if (level == lineLevel) {
        
      } else {
        
      }
      
      (currentList, None)
    }

//  def hasData: Boolean
//  def name: String
//  def time: Long
//  def size: Long
//  def children: Iterable[SourceEntry]
//  /** implementations are responsible for closing the reader. */
//  def read[ReturnType]: (SeekReader => ReturnType) => ReturnType
  
}