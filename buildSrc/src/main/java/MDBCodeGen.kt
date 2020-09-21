import org.gradle.internal.impldep.org.apache.commons.io.FileUtils
import java.io.File

class MDBCodeGen{
    companion object {
        @JvmStatic
        fun main(args: Array<String>) {

            val srcGenDir = File("../srcGen/main/java")
            val srcDir = File("../src/main/java")

            val testGenDir = File("../srcGen/test/java")


            FileUtils.write(File(srcGenDir, "AACodeGen.java"), """
public class AACodeGen{
}
                            """)

            val srcDirRecords = File(srcGenDir, "org/mapdb/record/")
            srcDirRecords.mkdirs()
            GenRecords.makeRecordMakers(srcDirRecords)

            GenMarkers.wlock(srcDir, srcGenDir);

        }
    }


}