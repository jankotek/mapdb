import org.gradle.internal.impldep.org.apache.commons.io.FileUtils
import java.io.File

class MDBCodeGen{
        companion object {
                @JvmStatic
                fun main(args: Array<String>) {

                        org.mapdb.ec.flat.run.main(args)

                        val srcDir = File("../srcGen/main/java")
                        val testDir = File("../srcGen/test/java")


                        FileUtils.write(File(srcDir, "aaGen.kt"),"""
class AACodeGen{
}
                            """)
                }
        }
}