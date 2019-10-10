package dedup

import dedup.Server

object Step2_Init extends App {
  Step1_Clear.main(Array())
  Server.main(Array("init"))
}
