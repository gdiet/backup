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
import net.diet_rich.dedup.core.FileSystem
import net.diet_rich.dedup.core.values.TreeEntry
import net.diet_rich.dedup.util.{CallLogging, Logging}

trait AbstractResource extends DigestResource with PropFindableResource with Logging with CallLogging {
  val treeEntry: TreeEntry
  val fileSystem: FileSystem

  // DigestResource
  override def authenticate(digestRequest: DigestResponse): Object =
    debug(s"authenticate(digestRequest: '$digestRequest')") { digestRequest getUser() }
  override def isDigestAllowed: Boolean = debug("isDigestAllowed()") { true }
  
  // PropFindableResource
  override def getCreateDate: Date = debug("getCreateDate()") { getModifiedDate() }
  
  // Resource
  override def getName(): String = debug("getName()") { treeEntry.name }
  override def getUniqueId: String = debug("getUniqueId()") { null }
  override def authenticate(user: String, password: String): Object =
    debug(s"authenticate(user: '$user', password: '$password')") { user }
  override def authorise(request: Request, method: Request.Method, auth: Auth): Boolean =
    debug(s"authorise(request: '$request', method: '$method', auth: '$auth')") { !method.isWrite }
  override def getRealm: String = debug("getRealm()") { "dedup@diet-rich.net" }
  override def checkRedirect(request: Request): String = debug(s"checkRedirect(request: '$request')") { null }
}
