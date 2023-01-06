package dedup

import java.io.FileWriter
import java.nio.charset.StandardCharsets
import scala.util.Using.resource

class BackupToolSpec extends org.scalatest.freespec.AnyFreeSpec with TestFile:
  import BackupTool.*
  def w = createWildcardPattern

  "createWildcardPattern" in {
    assert(createWildcardPattern("abc") == "\\Qabc\\E")
    assert(createWildcardPattern("a?c") == "\\Qa\\E.\\Qc\\E")
    assert(createWildcardPattern("a*c") == "\\Qa\\E.*\\Qc\\E")
    assert(createWildcardPattern("ab/") == "\\Qab/\\E")

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
    assert(deriveIgnoreRules(Seq()) == Seq())
    assert(deriveIgnoreRules(Seq("a?b/c*", "d/*e?f/")) == Seq( List(w("a?b"), w("c*")), List(w("d"), w("*e?f/")) ))
  }

  "ignoreRulesFromFile" in {
    resource(FileWriter(testFile, StandardCharsets.UTF_8))(_.write(
      """# Comment will be ignored, same as empty line (containing only spaces) below
        |
        |log/*.log
        |temp/
        |""".stripMargin))
    val rules = ignoreRulesFromFile(testFile.getParentFile, testFile)
    assert(rules == Seq( List(w("log"), w("*.log")), List(w("temp/")) ))
  }
