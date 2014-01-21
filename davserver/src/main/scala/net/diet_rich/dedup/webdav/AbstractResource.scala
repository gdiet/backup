// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.webdav

import io.milton.http._
import io.milton.http.http11.auth.DigestResponse
import io.milton.resource._

import java.util.Date

import net.diet_rich.util.CallLogging

trait AbstractResource extends DigestResource with PropFindableResource with CallLogging {
  // DigestResource
  def authenticate(digestRequest: DigestResponse): Object =
    debug(s"authenticate(digestRequest: '$digestRequest')") { digestRequest.getUser() }
  def isDigestAllowed(): Boolean = debug("isDigestAllowed()") { true }
  
  // PropFindableResource
  def getCreateDate(): Date = debug("getCreateDate()") { getModifiedDate() }
  
  // Resource
  def getUniqueId(): String = debug("getUniqueId()") { null }
  def getName(): String
  def authenticate(user: String, password: String): Object =
    debug(s"authenticate(user: '$user', password: '$password')") { user }
  // TODO read-only for now
  def authorise(request: Request, method: Request.Method, auth: Auth): Boolean =
    debug(s"authorise(request: '$request', method: '$method', auth: '$auth')") { !method.isWrite }
  def getRealm(): String = debug("getRealm()") { "dedup@diet-rich.net" }
  def getModifiedDate(): Date
  def checkRedirect(request: Request): String = debug(s"checkRedirect(request: '$request')") { null }
}
