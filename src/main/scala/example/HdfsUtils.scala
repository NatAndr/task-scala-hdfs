package example

import java.io.{FileSystem => _, _}
import java.net.URI

import org.apache.hadoop.conf._
import org.apache.hadoop.fs._

case object HdfsUtils {

  def getFileSystem: FileSystem = {
    val conf = new Configuration()
    FileSystem.get(new URI("hdfs://localhost:9000"), conf)
  }

  def listFiles(fileSystem: FileSystem, path: Path): List[Path] = {
    fileSystem
      .listStatus(path)
      .flatMap { obj =>
        if (obj.getPath.getName.contains("csv")) List(obj.getPath)
        else Nil
      }
      .toList
  }

  def listDirectories(fileSystem: FileSystem, folderPath: String): List[Path] = {
    fileSystem
      .listStatus(new Path(folderPath))
      .flatMap { obj =>
        if (obj.isDirectory) List(obj.getPath)
        else Nil
      }
      .toList
  }

  def createFolder(fileSystem: FileSystem, folderPath: String): Unit = {
    val path = new Path(folderPath)
    if (!fileSystem.exists(path)) {
      fileSystem.mkdirs(path)
    }
  }

  def createFile(fileSystem: FileSystem, path: String): OutputStream = {
    fileSystem.create(new Path(path))
  }

  def openFile(fileSystem: FileSystem, path: String): InputStream = {
    fileSystem.open(new Path(path))
  }

  def saveFile(fileSystem: FileSystem, filepath: String): Unit = {
    val file = new File(filepath)
    val out = fileSystem.create(new Path(file.getName))
    val in = new BufferedInputStream(new FileInputStream(file))
    var b = new Array[Byte](1024)
    var numBytes = in.read(b)
    while (numBytes > 0) {
      out.write(b, 0, numBytes)
      numBytes = in.read(b)
    }
    in.close()
    out.close()
  }

  def removeFile(fileSystem: FileSystem, filename: String): Boolean = {
    val path = new Path(filename)
    fileSystem.delete(path, true)
  }

  def getFile(fileSystem: FileSystem, filename: String): InputStream = {
    val path = new Path(filename)
    fileSystem.open(path)
  }
}
