package dedup2

import java.util.concurrent.atomic.AtomicBoolean

case class Settings(
 readonly: Boolean,
 copyWhenMoving: AtomicBoolean = new AtomicBoolean(false)
)
