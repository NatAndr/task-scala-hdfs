package example

import org.apache.commons.io.IOUtils

object Main extends App {
  val fileSystem = HdfsUtils.getFileSystem

  HdfsUtils.createFolder(fileSystem, "/ods")
  HdfsUtils.listDirectories(fileSystem, "/stage").foreach(path => {

    HdfsUtils.createFolder(fileSystem, s"/ods/${path.getName}")
    val files = HdfsUtils.listFiles(fileSystem, path)

    if (files.nonEmpty) {
      val host = path.getParent.getParent
      val targetFileName = s"${host}ods/${path.getName}/${files.head.getName}"
      val out = HdfsUtils.createFile(fileSystem, targetFileName)
      val stringBuilder = new StringBuilder

      files.foreach(file => {
        val sourceFileName = s"${host}stage/${path.getName}/${file.getName}"
        val in = HdfsUtils.getFile(fileSystem, sourceFileName)
        val stringLine = IOUtils.toString(in, "UTF-8")
        if (stringLine.nonEmpty) stringBuilder.append(stringLine)
        in.close()
        HdfsUtils.removeFile(fileSystem, sourceFileName)
      })

      out.write(stringBuilder.toString.getBytes)
      out.close()
    }
  }
  )

  fileSystem.close()
}

