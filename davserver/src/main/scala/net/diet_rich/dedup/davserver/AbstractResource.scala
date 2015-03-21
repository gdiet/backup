package net.diet_rich.dedup.davserver

import java.util.Date

import io.milton.http.{Auth, Request}
import io.milton.http.http11.auth.DigestResponse
import io.milton.resource.{DigestResource, PropFindableResource}
import net.diet_rich.dedup.core.meta.TreeEntry

import net.diet_rich.dedup.util.Logging

trait AbstractResource extends DigestResource with PropFindableResource with Logging {
  val treeEntry: TreeEntry
  val writeEnabled: Boolean

  // DigestResource
  override final def authenticate(digestRequest: DigestResponse): Object = log.call(s"authenticate(digestRequest: '$digestRequest')") { digestRequest getUser() }
  override final def isDigestAllowed: Boolean = log.call("isDigestAllowed") { true }

  // PropFindableResource
  override final def getCreateDate: Date = log.call("getCreateDate") { getModifiedDate }

  // Resource
  override final def authenticate(user: String, password: String): Object = log.call(s"authenticate(user: '$user', password: '$password')") { user }
  override final def authorise(request: Request, method: Request.Method, auth: Auth): Boolean = log.call(s"authorise(request: '$request', method: '$method', auth: '$auth')") { !writeEnabled || !method.isWrite }
  override final def checkRedirect(request: Request): String = log.call(s"checkRedirect(request: '$request')") { null }
  override final def getModifiedDate: Date = log.call("getModifiedDate") { treeEntry.changed map (new Date(_)) getOrElse new Date() }
  override final def getName: String = log.call("getName") { treeEntry.name }
  override final def getRealm: String = log.call("getRealm") { "dedup@diet-rich.net" }
  override final def getUniqueId: String = log.call("getUniqueId") { null }
}

//trait AbstractWriteResource extends DeletableResource with MoveableResource { _: AbstractResource =>
//  val writeAccess: RepositoryReadWrite
//
//  override final def delete(): Unit = { log info s"deleting $treeEntry"; if (!writeAccess.metaBackend.markDeleted(treeEntry id)) log warn s"could not mark deleted $treeEntry" }
//
//  override final def moveTo(destination: CollectionResource, newName: String): Unit = log.call(s"moveTo(destination: $destination, newName: $newName)") {
//    if (newName.isEmpty) throw new BadRequestException("Rename not possible: New name is empty.")
//    if (newName matches ".*[\\?\\*\\/\\\\].*") throw new BadRequestException(s"Rename not possible: New name '$newName' contains illegal characters, one of [?*/\\].")
//    destination match {
//// FIXME
////      case DirectoryResource(`fileSystem`, directory) =>
////        if (fileSystem.change(treeEntry id, directory id, newName, treeEntry changed, treeEntry data).isEmpty) log warn s"Could not move $treeEntry to $directory."
//      case other => throw new BadRequestException(s"Can't move $this to $other.")
//    }
//  }
//}
