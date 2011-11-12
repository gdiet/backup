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
   *  a ´DataSource´ may return.
   *  
   *  @author  Georg Dietrich
   *  @version 1.1, 2011-11-12
   */
  trait SourceSignal[-A]
  
  /** The end-of-input source signal.
   * 
   *  @author  Georg Dietrich
   *  @version 1.1, 2011-11-12
   */
  case object EOI extends SourceSignal[Any]

  /** The signal that an error occurred when fetching source data.
   * 
   *  @author  Georg Dietrich
   *  @version 1.1, 2011-11-12
   */
  case class SourceError(error: Throwable) extends SourceSignal[Any]

  /** The signal containing the next data item.
   * 
   *  @author  Georg Dietrich
   *  @version 1.0, 2011-11-12
   */
  case class Next[A](data: A) extends SourceSignal[A]

  /** The ´DataSource´ trait defines the source data pull accessor
   *  to use in a ´PushingSource´. The ´PushingSource´ guarantees
   *  to call the ´close´ method on the data accessor after use.
   *  {{{
   *    def dataSource = new DataSource[Int] {
   *      var n = 0;
   *      def fetch = { n = n + 1 ; if (n < 1000) Next(n) else EOI }
   *      def close = { }
   *    }
   *  }}}
   * 
   *  @tparam A  the source value object type.
   *  
   *  @author  Georg Dietrich
   *  @version 1.1, 2011-11-01
   */
  trait DataSource[A] {
    def fetch : SourceSignal[A]
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
   *  @version 1.2, 2011-11-12
   */
  trait PushingSource[A] {
    
    /** Each call to ´source´ fetches a new source ´DataSource´.
     */
    protected def source() : DataSource[A]
    
    /** Produce the data to process. Note that in ´SourceError´
     *  conditions the ´DataProcessor´ is called once for clean-up. 
     *  If it returns a ´finished(result)´, the error is suppressed,
     *  else the error is thrown.
     * 
     *  @parameter processor the processor to receive the data.
     */
    final def produce[B](processor: DataProcessor[A,B]) : B = {
      val localSource = source()
      @annotation.tailrec
      def loopProduce[B](localProcessor: DataProcessor[A,B]) : B = {
        val sourceData = try { localSource.fetch } catch { case e => SourceError(e) }
        (sourceData, localProcessor.process(sourceData)) match {
          case (_, Right(result))  => result
          case (SourceError(e), _) => throw e;
          case (_, Left(nextProcessor)) => loopProduce(nextProcessor)
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
   *  @version 1.1, 2011-11-12
   */
  trait DataProcessor[A,B] {
    /** Process the current source state, either an input value or 
     *  a source signal indicating e.g. no more data is available.
     *  
     *  @return either the result or the processor for the next
     *          source state. If the result is returned, the
     *          ´DataSource´ is closed.
     */
    def process(item: SourceSignal[A]) : Either[DataProcessor[A,B],B]
  }
  
  /** Convenience type used for inline data processor definitions.
   *  {{{
   *    def processor : InlineDataProcessor[Int,Unit] = {
   *      case Left(x)  => println(x); continue(processor)
   *      case Right(x) => finished(Unit)
   *    }
   *  }}}
   * 
   *  @tparam A  the input value object type.
   *  @tparam B  the result object type.
   * 
   *  @author  Georg Dietrich
   *  @version 1.0, 2011-11-01
   */
  type InlineDataProcessor[A,B] = SourceSignal[A] => Either[DataProcessor[A,B],B]
  
  /** Convenience conversion used for inline data processor definitions.
   */
  implicit def setupProcessor[A,B](processor: InlineDataProcessor[A,B]) : DataProcessor[A,B] =
    new DataProcessor[A,B] {
      def process(item: SourceSignal[A]) : Either[DataProcessor[A,B],B] = processor(item)
    }
  
  /** Create a 'finished' signal to return in the data source or data processor.
   */
  def finished[B](b:B) = Right(b)
  
  /** Create a 'continue' signal to return in the data source or data processor.
   */
  def continue[A](a:A) = Left(a)
  
}
