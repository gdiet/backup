package dedup
package backend

class FileHandlesWriteSpec extends org.scalatest.freespec.AnyFreeSpec with TestFile:
  type IAE = IllegalArgumentException

  s"Scenario test for ${°[FileHandlesWrite]}" - {
    val handles = FileHandlesWrite(testFile.toPath)

    "for missing handles" - {
      "getSize returns None" in { assert(handles.getSize(1) === None) }
      "dataEntry returns None" in { assert(handles.dataEntry(2, _ => !!!) === None) }
      "read returns None" in { assert(handles.read(1, 0, 10) === None) }
      "release throws an IllegalArgumentException" in { intercept[IAE](handles.release(1)) }
      "removeAndGetNext throws an IllegalArgumentException" in { intercept[IAE](handles.removeAndGetNext(1, null)) }
      "release returns None if the exception is suppressed" in {
        suppressing("WriteHandle.release")(assert(handles.release(1) === None))
      }
      "removeAndGetNext returns None if the exception is suppressed" in {
        suppressing("WriteHandle.removeAndGetNext.missing")(assert(handles.removeAndGetNext(1, null) === None))
      }
    }

    "for file handle 1" - {
      "addIfMissing returns true because entry is missing, then false because it is already present" in {
        assert(handles.addIfMissing(1))
        assert(!handles.addIfMissing(1))
      }
      "getSize returns None because entry is empty" in {
        assert(handles.getSize(1) === None)
      }

      "dataEntry(1, _ => 10) returns a newly created data entry with size 10" in {
        assert(handles.dataEntry(1, _ => 10).get.size === 10)
      }
      def entry = handles.dataEntry(1, _ => !!!).get
      "dataEntry(1, _ => !!!) returns the existing data entry with size 10" in {
        assert(entry.size === 10)
      }
      "getSize(1) returns 10 because that is the size of entry 1" in {
        assert(handles.getSize(1) === Some(10))
      }

      "writing more data to the entry increases the size" in {
        entry.write(Iterator(10L -> Array[Byte](1,2,3)))
        assert(handles.getSize(1) === Some(13))
      }

      "the data can be read" in {
        handles.read(1, 9, 3).map(_.toList) match
          case None => assert(false)
          case Some(9L -> Left(1L) :: 10L -> Right(bytes) :: Nil) => assert(bytes.toSeq == Seq[Byte](1,2))
          case other => assert(false, s"bad result: $other")
      }

      "releasing the entry returns the entry because it must be queued explicitly" in {
        val theEntry = entry
        assert(handles.release(1) === Some(theEntry))
      }

      "size() and read() now return None because the entry is empty" in {
        assert(handles.getSize(1) === None)
        assert(handles.read(1, 9, 3) === None)
      }

      "addIfMissing returns true because entry is missing (although queued for storing), then false" in {
        assert(handles.addIfMissing(1))
        assert(!handles.addIfMissing(1))
      }

      "the entry is queued, so it's size is still returned" in {
        assert(handles.getSize(1) === Some(13))
      }

      "the data from the queue can be read" in {
        handles.read(1, 9, 3).map(_.toList) match
          case None => assert(false)
          case Some(9L -> Left(1L) :: 10L -> Right(bytes) :: Nil) => assert(bytes.toSeq == Seq[Byte](1, 2))
          case other => assert(false, s"bad result: $other")
      }

      // TODO Needs test because this was bugged
//      "adding the entry again returns true because ...?"
//      handles.addIfMissing(1)

    }
  }
