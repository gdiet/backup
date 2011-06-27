// Copyright (c) 2011 Georg Dietrich
// Licensed under the MIT license: http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.backup.storelogic

class DataFilesPattern_Names {
  import org.fest.assertions.Assertions.assertThat
  import org.testng.annotations.Test
  import net.diet_rich.testutils.Assertions._
  import DataFilesPattern._

  @Test
  def test_nameForFileID : Unit = {
    assertThat(nameForFileID(100)) isEqualTo "000000000000100"
  }

  @Test
  def test_ecNamesForFileID : Unit = {
    ensure that ecNamesForFileID(-1) willThrow new IllegalArgumentException
    assertThat(ecNamesForFileID(0)) isEqualTo ("ec_000000000000000_c00", "ec_000000000000000_r00")
    assertThat(ecNamesForFileID(1)) isEqualTo ("ec_000000000000000_c00", "ec_000000000000000_r01")
    assertThat(ecNamesForFileID(99)) isEqualTo ("ec_000000000000000_c19", "ec_000000000000000_r04")
    assertThat(ecNamesForFileID(799)) isEqualTo ("ec_000000000000400_c19", "ec_000000000000400_r19")
  }
 
  @Test
  def test_idsForECnameCol : Unit = {
    assertThat(
        idsForECname("ec_000000000000000_c01")
        sameElements 
        List(5,6,7,8,9, 105,106,107,108,109, 205,206,207,208,209,  305,306,307,308,309)
    ).isTrue
    assertThat(
        idsForECname("ec_000000000000400_c19")
        sameElements 
        List(495,496,497,498,499, 595,596,597,598,599, 695,696,697,698,699,  795,796,797,798,799)
    ).isTrue
  }

  @Test
  def test_idsForECnameRow : Unit = {
    assertThat(
        idsForECname("ec_000000000000000_r01")
        sameElements 
        Seq.range(1, 101, 5)
    ).isTrue
    assertThat(
        idsForECname("ec_000000000000400_r19")
        sameElements 
        Seq.range(704, 804, 5)
    ).isTrue
  }
  
  @Test
  def test_illegalIdForECname : Unit = {
    ensure that idsForECname("ec_000000000000000_X01") willThrow new IllegalArgumentException
    ensure that idsForECname("XX_000000000000000_r01") willThrow new IllegalArgumentException
    ensure that idsForECname("ecX000000000000000_r01") willThrow new IllegalArgumentException
    ensure that idsForECname("ec_000000000000000Xr01") willThrow new IllegalArgumentException
    
    ensure that idsForECname("ec_000000000000000_r1") willThrow new IllegalArgumentException
    ensure that idsForECname("ec_00000000000000_r01") willThrow new IllegalArgumentException
  }

}