/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.databricks.processing

import cats.effect.{Resource, Sync}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{CreateFlag, FSDataInputStream, FSDataOutputStream, FileStatus, FileSystem, Path}
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.util.Progressable

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, OutputStream}
import java.net.URI
import java.util.EnumSet

import com.snowplowanalytics.snowplow.databricks.Config

class InMemoryFileSystem extends FileSystem { fs =>
  @volatile private var inputStream: ByteArrayInputStream = _

  def getBytes: ByteArrayInputStream = inputStream

  override def create(
    f: Path,
    permission: FsPermission,
    overwrite: Boolean,
    bufferSize: Int,
    replication: Short,
    blockSize: Long,
    progress: Progressable
  ): FSDataOutputStream = {
    val estimateFileSize = fs.getConf.getInt(InMemoryFileSystem.bufferSizeKey, 0)
    val out: OutputStream = new ByteArrayOutputStream(estimateFileSize) { o =>
      override def close(): Unit = {
        super.close()
        fs.inputStream = new ByteArrayInputStream(o.buf, 0, o.count)
      }
    }
    new FSDataOutputStream(out, fs.statistics)
  }

  override def getUri(): URI = new URI(InMemoryFileSystem.baseUri)

  override def getWorkingDirectory(): Path =
    new Path(getUri())

  /** FileSystem functionality not required by this app */

  private def notImplemented[A]: A =
    throw new IllegalStateException("Unexpected FileSystem method called")

  override def append(
    f: Path,
    bufferSize: Int,
    progress: Progressable
  ): FSDataOutputStream =
    notImplemented

  override def create(
    f: Path,
    permission: FsPermission,
    flags: EnumSet[CreateFlag],
    bufferSize: Int,
    replication: Short,
    blockSize: Long,
    progress: Progressable
  ): FSDataOutputStream =
    notImplemented

  override def delete(f: Path, recursive: Boolean): Boolean =
    notImplemented

  override def getFileStatus(f: Path): FileStatus =
    notImplemented

  override def listStatus(f: Path): Array[FileStatus] =
    notImplemented

  override def mkdirs(f: Path, permission: FsPermission): Boolean =
    notImplemented

  override def open(path: Path, bufferSize: Int): FSDataInputStream =
    notImplemented

  override def rename(src: Path, dst: Path): Boolean =
    notImplemented

  override def setWorkingDirectory(path: Path): Unit =
    notImplemented

}

object InMemoryFileSystem {

  case class Configured[F[_]](hadoopConf: Configuration, getBytes: F[ByteArrayInputStream])

  private val scheme        = "inmem"
  private val baseUri       = s"$scheme:///"
  private val bufferSizeKey = s"fs.$scheme.bufferSize"

  def configure[F[_]: Sync](config: Config.Batching): Resource[F, Configured[F]] =
    for {
      c <- Resource.eval(Sync[F].delay {
             val c = new Configuration
             c.setClass(s"fs.$scheme.impl", classOf[InMemoryFileSystem], classOf[FileSystem])
             c.setInt(bufferSizeKey, config.maxBytes)
             c.set("fs.defaultFS", baseUri)
             c
           })
      fs <- Resource.make(Sync[F].delay(FileSystem.get(c).asInstanceOf[InMemoryFileSystem]))(fs => Sync[F].delay(fs.close()))
    } yield Configured(c, Sync[F].delay(fs.getBytes))

}
