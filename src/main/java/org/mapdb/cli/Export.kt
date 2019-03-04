package org.mapdb.cli

import com.xenomachina.argparser.ArgParser
import org.mapdb.db.DB
import org.mapdb.Exporter
import org.mapdb.io.DataOutput2ByteArray
import org.mapdb.ser.Serializers
import java.io.File

object Export{


    class ExportArgs(parser: ArgParser) {
        val verbose by parser.flagging(
                "-v", "--verbose",
                help = "enable verbose mode")

        val name by parser.storing(
                "-n", "--name",
                help = "name of the collection")

        val d by parser.storing(
                "-d", "--dbfile",
                help = "database file")

        val o by parser.storing(
                "-o", "--outfile",
                help = "output file")

    }

    @JvmStatic
    fun main(args: Array<String>) {
        ArgParser(args).parseInto(::ExportArgs).run {
            val f = File(d)
            if(!f.exists() || !f.isFile || !f.canRead())
                error("dbfile does not exist")

            val db = DB.Maker.appendFile(f).readOnly().make()
            //FIXME hardcoded queue
            val c = db.queue(name=name, serializer = Serializers.INTEGER).make()

            val exportOut = DataOutput2ByteArray() //TODO heap load
            (c as Exporter).exportToDataOutput2(exportOut)

            val outFile = File(o)
            if(outFile.exists())
                error("out file already exists")
            outFile.writeBytes(exportOut.copyBytes())
        }
    }
}