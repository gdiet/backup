// Copyright (c) 2011 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.scala

/** This object is defining a 'namespace' of definitions 
 *  that can be imported with ´import ManagedIO._´. 
 *  The members defined are for data processing in push mode 
 *  allowing explicit control of start and end of processing.
 *  
 *  @author  Georg Dietrich
 *  @version 1.0, 2011-11-01
 */
object ManagedIO {

  /** The ´SourceSignal´ trait is the base trait for any signal
   *  a ´DataSource´ may return to indicate no more data is
   *  available.
   *  
   *  @author  Georg Dietrich
   *  @version 1.0, 2011-11-01
   */
  trait SourceSignal
  
  /** The end-of-input source signal.
   * 
   *  @author  Georg Dietrich
   *  @version 1.0, 2011-11-01
   */
  case object EOI extends SourceSignal
  
  /** The ´DataSource´ trait defines the source data pull accessor
   *  to use in a ´PushingSource´. The ´PushingSource´ guarantees
   *  to call the ´close´ method on the data accessor after use.
   *  {{{
   *    def dataSource = new DataSource[Int] {
   *      var n = 0;
   *      def fetch = { n = n + 1 ; if (n < 1000) continue(n) else finished(EOI) }
   *      def close = { }
   *    }
   *  }}}
   * 
   *  @tparam A  the source value object type.
   *  
   *  @author  Georg Dietrich
   *  @version 1.0, 2011-11-01
   */
  trait DataSource[A] {
    /** @return either (left) the next source value or
     *          (right) a signal indicating no more data is available.
     */
    def fetch : Either[A,SourceSignal]
    def close : Unit
  }

  object PushingSource {
    /** Create a ´PushingSource´ from a ´DataSource´ factory.
     * 
     *  @tparam A  the source value object type.
     */
    def apply[A](data: => DataSource[A]) : PushingSource[A] = new PushingSource[A] {
      override def source() = data;
    }
  }
  
  /** A data source that is processed in push mode allowing explicit
   *  control of start and end of processing.
   *  
   *  @tparam A  the source value object type.
   *  
   *  @author  Georg Dietrich
   *  @version 1.0, 2011-11-01
   */
  trait PushingSource[A] {
    
    /** Each call to ´source´ fetches a new source ´DataSource´.
     */
    protected def source() : DataSource[A]
    
    /** Produce the data to process.
     * 
     *  @parameter processor the processor to receive the data.
     */
    final def produce[B](processor: DataProcessor[A,B]) : B = {
      val localSource = source()
      @annotation.tailrec
      def loopProduce[B](localProcessor: DataProcessor[A,B]) : B = {
        localProcessor.process(localSource.fetch) match {
          case Right(result) => result
          case Left(nextProcessor) => loopProduce(nextProcessor)
        }
      }
      try {
        loopProduce(processor)
      } finally {
        try { localSource.close } catch { case _ => /* suppressed */ }
      }
    }
  }

  /** The ´DataProcessor´ trait defines the data receiver
   *  for a ´PushingSource´.
   * 
   *  @tparam A  the input value object type.
   *  @tparam B  the result object type.
   * 
   *  @author  Georg Dietrich
   *  @version 1.0, 2011-11-01
   */
  trait DataProcessor[A,B] {
    /** Process the current source state, either an input value 
     *  or the signal indicating no more data is available.
     *  
     *  @return either the result or the processor for the next
     *          source state. If the result is returned, the
     *          ´DataSource´ is closed.
     */
    def process(item: Either[A,SourceSignal]) : Either[DataProcessor[A,B],B]
  }
  
  /** Convenience type used for inline data processor definitions.
   *  {{{
   *    def processor : DataProc[Int,Unit] = {
   *      case Left(x) => finished(Unit)
   *      case Right(x) => println(x); continue(processor)
   *    }
   *  }}}
   * 
   *  @tparam A  the input value object type.
   *  @tparam B  the result object type.
   * 
   *  @author  Georg Dietrich
   *  @version 1.0, 2011-11-01
   */
  type InlineDataProcessor[A,B] = Either[A,SourceSignal] => Either[DataProcessor[A,B],B]
  
  /** Convenience conversion used for inline data processor definitions.
   */
  implicit def setupProcessor[A,B](processor: InlineDataProcessor[A,B]) : DataProcessor[A,B] =
    new DataProcessor[A,B] {
      def process(item: Either[A,SourceSignal]) : Either[DataProcessor[A,B],B] = processor(item)
    }
  
  /** Create a 'finished' signal to return in the data source or data processor.
   */
  def finished[B](b:B) = Right(b)
  
  /** Create a 'continue' signal to return in the data source or data processor.
   */
  def continue[A](a:A) = Left(a)
  
}
