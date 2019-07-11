package dedup

object Step2_Store extends App {
  Store.run(Map(
    "source" -> """e:\georg\privat\dev\backup\src""",
    "target" -> "/backup/v1"
  ))
}
