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

    "addIfMissing(1) returns true because entry 1 is missing" in {
      assert(handles.addIfMissing(1))
    }

    "addIfMissing(1) returns false because entry 1 is already present" in {
      assert(!handles.addIfMissing(1))
    }

    "getSize(1) returns None because entry 1 is empty" in {
      assert(handles.getSize(1) === None)
    }

    "dataEntry(1, _ => 10) returns a data entry with size 10" in {
      assert(handles.dataEntry(1, _ => 10).get.size === 10)
    }

    "getSize(1) returns 10 because that is the size of entry 1" in {
      assert(handles.getSize(1) === Some(10))
    }

    "dataEntry(1, _ => 12) returns the existing data entry with size 10" in {
      assert(handles.dataEntry(1, _ => !!!).get.size === 10)
    }


//    handles.dataEntry(1, _ => !!!).get.write(Iterator(3L -> Array[Byte](1,2,3)))
  }
