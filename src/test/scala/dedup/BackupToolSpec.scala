package dedup

class BackupToolSpec extends org.scalatest.freespec.AnyFreeSpec:
  import BackupTool.*

  "createWildcardPattern" in {
    assert(createWildcardPattern("abc") == "\\Qabc\\E")
    assert(createWildcardPattern("a?c") == "\\Qa\\E.\\Qc\\E")
    assert(createWildcardPattern("a*c") == "\\Qa\\E.*\\Qc\\E")

    val pattern = createWildcardPattern("a?b*c.d")
    println(pattern)
    assert("aXbc.d".matches(pattern))     // 0 characters for *
    assert("aXbXXXc.d".matches(pattern))  // 3 characters for *
    assert(!"abc.d".matches(pattern))     // character for ? missing
    assert(!"aXbcXd".matches(pattern))    // `.` is not a wildcard
    assert(!"XaXbc.d".matches(pattern))   // prefix not expected
    assert(!"aXbc.dX".matches(pattern))   // suffix not expected
  }

  "deriveIgnoreRules" in {
    def w = createWildcardPattern
    assert(deriveIgnoreRules(Seq()) == Seq())
    assert(deriveIgnoreRules(Seq("a?b/c*", "d/*e?f")) == Seq( List(w("a?b"), w("c*")), List(w("d"), w("*e?f")) ))
  }
