import java.io.File

object GenMarkers{

    val markers:Map<String,String> = linkedMapOf(
            Pair("//-WLOCK", """lock.writeLock().lock(); try{"""),
            Pair("//-WUNLOCK", """}finally{lock.writeLock().unlock();}"""),
            Pair("//-newRWLOCK", """java.util.concurrent.locks.ReadWriteLock lock = new java.util.concurrent.locks.ReentrantReadWriteLock();""")
    )

    fun recurJavaFiles(dir:File, f: (File) -> Unit){
        val allFiles = dir.listFiles();
        allFiles.filter { it.extension.equals("java") }.forEach(f)
        allFiles.filter{it.isDirectory}.forEach{recurJavaFiles(it,f)}
    }


    // process //*-WLOCk markers
    fun wlock(srcDir: File, genDir:File) {
        recurJavaFiles(srcDir) { f:File->
            var content = f.readText()

            if(markers.keys.none{content.contains(it)}) {
                return@recurJavaFiles
            }
            for ((marker, repl) in markers) {
                content = content.replace(marker, repl)
            }

            val oldClassName = f.nameWithoutExtension
            content = content.replace("class "+oldClassName, "class ${oldClassName}RWLock")
            content = content.replace(" "+oldClassName+"(", " ${oldClassName}RWLock(")

            val newFile = File(genDir.path + "/"+ f.relativeTo(srcDir).parent +"/"+ oldClassName + "RWLock.java")

            newFile.parentFile.mkdirs()
            newFile.writeText(content)

        }
    }

}