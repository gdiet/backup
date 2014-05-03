// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.webdav

import java.util.Date

import io.milton.http.Auth
import io.milton.http.Request
import io.milton.http.http11.auth.DigestResponse
import io.milton.resource._
import net.diet_rich.dedup.database.TreeEntry
import net.diet_rich.util.CallLogging
import net.diet_rich.util.Logging

trait AbstractResource extends DigestResource with PropFindableResource with Logging with CallLogging {
  val typeIdentifier: String
  val treeEntry: TreeEntry
  val fileSystem: DedupFileSystem
  
  private def resourcePath = fileSystem path treeEntry.id getOrElse treeEntry.toString
  override def toString() = s"$typeIdentifier($resourcePath)"

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
  def authorise(request: Request, method: Request.Method, auth: Auth): Boolean =
    debug(s"authorise(request: '$request', method: '$method', auth: '$auth')") { !method.isWrite }
  def getRealm(): String = debug("getRealm()") { "dedup@diet-rich.net" }
  def getModifiedDate(): Date
  def checkRedirect(request: Request): String = debug(s"checkRedirect(request: '$request')") { null }
}
