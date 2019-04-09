package org.mapdb.ec.flat

import java.nio.file.FileSystems
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption

object run {
    @JvmStatic
    fun main(args: Array<String>) {
        val str = str

        println("XXXX "+java.io.File(".").absolutePath)

        val path:Path = FileSystems.getDefault().getPath(targetPath)
        Files.createDirectories(path.parent)
        Files.write(path, str.toByteArray(), StandardOpenOption.CREATE_NEW, StandardOpenOption.TRUNCATE_EXISTING)
    }
}