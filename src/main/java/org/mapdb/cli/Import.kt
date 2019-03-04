package org.mapdb.cli

import com.xenomachina.argparser.ArgParser
import org.mapdb.db.DB
import org.mapdb.io.DataInput2ByteArray
import org.mapdb.ser.Serializers
import java.io.File

object Import{


    class ImportArgs(parser: ArgParser) {
        val verbose by parser.flagging(
                "-v", "--verbose",
                help = "enable verbose mode")

        val name by parser.storing(
                "-n", "--name",
                help = "name of the collection")

        val d by parser.storing(
                "-d", "--dbfile",
                help = "database file")

        val i by parser.storing(
                "-i", "--inputfile",
                help = "input file")

    }

    @JvmStatic
    fun main(args: Array<String>) {
        ArgParser(args).parseInto(::ImportArgs).run {
            val ifile = File(i)
            if(!ifile.exists() || !ifile.isFile || !ifile.canRead())
                error("dbfile does not exist")


            val dbfile = File(d)
            val db = DB.Maker.appendFile(dbfile).make()
            val input = DataInput2ByteArray(ifile.readBytes()) //FIXME heap load
            //FIXME hardcoded queue
            val c = db.queue(name=name, serializer = Serializers.INTEGER)
                    .importFromDataInput2(input).make()

            //TODO commit
            db.close()
        }
    }
}