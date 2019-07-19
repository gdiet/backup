package dedup

object Step2_Store extends App {
  Store.run(Map(
    "source" -> """e:\georg\hg\tb-a\backend""",
    "target" -> "/backup/v1"
  ))
  Store.run(Map(
    "source" -> """e:\georg\hg\tb-a\backend""",
    "target" -> "/backup/v2"
  ))
  Store.run(Map(
    "source" -> """e:\georg\hg\tb-a\backend""",
    "target" -> "/backup/v3",
    "reference" -> "/backup/v*"
  ))
}
