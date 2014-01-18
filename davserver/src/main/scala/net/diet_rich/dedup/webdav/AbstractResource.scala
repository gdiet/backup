// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.webdav

import java.util.Date
import io.milton.http.Auth
import io.milton.http.Request
import io.milton.http.http11.auth.DigestResponse
import io.milton.resource.DigestResource
import io.milton.resource.PropFindableResource

trait AbstractResource extends DigestResource with PropFindableResource {
  import AbstractResource._
  
  // DigestResource
  def authenticate(digestRequest: DigestResponse): Object = digestRequest.getUser
  def isDigestAllowed(): Boolean = true
  
  // PropFindableResource
  def getCreateDate(): Date = getModifiedDate()
  
  // Resource
  def getUniqueId(): String = null
  def getName(): String
  def authenticate(user: String, password: String): Object = user
  // TODO read-only for now
  def authorise(request: Request, method: Request.Method, auth: Auth): Boolean = !method.isWrite
  def getRealm(): String = "dedup@diet-rich.net"
  // TODO for read-write, the current date would probably be the better default
  def getModifiedDate(): Date = date
  def checkRedirect(request: Request): String = null
}
object AbstractResource {
  val date = new Date()
}
