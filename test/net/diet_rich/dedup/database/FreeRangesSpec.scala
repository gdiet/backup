// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.database

import net.diet_rich.util.vals._
import org.specs2.mutable._

class FreeRangesSpec extends SpecificationWithJUnit {
  
  val defaultBlockSize = Size(10)

  val defaultFreeAreaStart = Position(25)
  
  val defaultSlices = Set(
        Range(Position(0), Position(5)),
        Range(Position(5), Position(10)),
        Range(Position(23), Position(25))
      )
  val overlappingSlices = Set(
        Range(Position(2), Position(5)),
        Range(Position(4), Position(6))
      )
  val oneSliceAcrossBlocks = Set(
        Range(Position(2), Position(5)),
        Range(Position(8), Position(11))
      )
  val oneSliceIntoFreeArea = Set(
        Range(Position(2), Position(5)),
        Range(Position(20), Position(26))
      )
  
  val rangeCompatibleWithDefaultSlices = Range(Position(10), Position(15))
  val rangeOverlappingWithDefaultSlices = Range(Position(6), Position(7))
  val rangeAcrossBlocks = Range(Position(15), Position(21))
  val rangeIntoFreeArea = Range(Position(30), Position(35))
  
  def defaultFreeRanges = FreeRanges(
        defaultBlockSize, 
        defaultFreeAreaStart, 
        defaultSlices
      )
      
  def emptyFreeRanges = FreeRanges(
        defaultBlockSize, 
        defaultFreeAreaStart, 
        Set()
      )
  
  "The free data range management factory" should {
    "use the block size, the start of the free area, and the additional free block slices" in {
      FreeRanges(
        defaultBlockSize, 
        defaultFreeAreaStart, 
        defaultSlices
      ) must haveClass [FreeRanges]
    }
    "allow for no additional free block slices" in {
      FreeRanges(
        defaultBlockSize, 
        defaultFreeAreaStart, 
        Set()
      ) must haveClass [FreeRanges]
    }
    "check that additional free block slices do not overlap" in {
      FreeRanges(
        defaultBlockSize, 
        defaultFreeAreaStart, 
        overlappingSlices
      ) must throwAn [AssertionError] (message = "overlapping slices")
    }
    "check that there are no slices across blocks" in {
      FreeRanges(
        defaultBlockSize, 
        defaultFreeAreaStart, 
        oneSliceAcrossBlocks
      ) must throwAn [AssertionError] (message = "slices across blocks")
    }
    "check that no slices overlap the start of the free area" in {
      FreeRanges(
        defaultBlockSize, 
        defaultFreeAreaStart, 
        oneSliceIntoFreeArea
      ) must throwAn [AssertionError] (message = "slices in free area")
    }
  }

  "A free data range object" should {
    "have a function to enqueue another slice" in {
      defaultFreeRanges.add(rangeCompatibleWithDefaultSlices) must haveClass [FreeRanges]
    }
    "check the validity of a slice when enqueueing" in {
      defaultFreeRanges.add(rangeOverlappingWithDefaultSlices) must throwAn [AssertionError] (message = "overlapping slices")
      defaultFreeRanges.add(rangeAcrossBlocks) must throwAn [AssertionError] (message = "slices across blocks")
      defaultFreeRanges.add(rangeIntoFreeArea) must throwAn [AssertionError] (message = "slices in free area")
      defaultFreeRanges.add(defaultSlices.head) must throwAn [AssertionError] (message = "slice already present")
    }
    "have a get function to fetch a slice (and the new free data range object)" in {
      defaultFreeRanges.get must haveClass [(FreeRanges, Range)]
    }
  }

  "The get function of a free data range object" should {
    "return all slices it has stored" in {
      val (_, returnedSlices) = defaultSlices.foldLeft((defaultFreeRanges, Set[Range]())){
        case ((freeRanges, set), _) =>
          val (newRanges, range) = freeRanges.get
          (newRanges, set + range)
      }
      returnedSlices === defaultSlices
    }
    "return a new slice from the start of the free area if it has no slices stored" in {
      val (_, range) = emptyFreeRanges.get
      range === Range(defaultFreeAreaStart, defaultFreeAreaStart + Size(5))
    }
    "update the start-of-free-area marker when return a new slice" in {
      val (FreeRanges(_, newFreeAreaStart, _), _) = emptyFreeRanges.get
      newFreeAreaStart === defaultFreeAreaStart + Size(5)
    }
  }
}