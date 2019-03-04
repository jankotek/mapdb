package org.mapdb

import io.kotlintest.should
import io.kotlintest.shouldBe
import org.junit.Assert.assertTrue
import org.mapdb.cli.Export
import org.mapdb.cli.Import
import org.mapdb.db.DB
import org.mapdb.io.DataInput2
import org.mapdb.io.DataInput2ByteArray
import org.mapdb.io.DataOutput2ByteArray
import org.mapdb.queue.LinkedFIFOQueue
import org.mapdb.ser.Serializer
import org.mapdb.ser.Serializers

class DBCollectionTest: DBWordSpec({

    for(a in adapters()){
        a.name should {
            "wrong serializer fail on reopen"{
                TT.withTempFile { f->
                    var db = DB.Maker.appendFile(f).make()
                    a.open(db, "name", Serializers.INTEGER)
                    db.close()

                    db = DB.Maker.appendFile(f).make()
                    TT.assertFailsWith(DBException.WrongSerializer::class) {
                        a.open(db, "name", Serializers.LONG)
                    }
                }
            }

            "use the same instance"{
                val db = DB.Maker.heapSer().make()
                val c1 = a.open(db, "name", Serializers.INTEGER)
                val c2 = a.open(db, "name", Serializers.INTEGER)
                val c3 = a.open(db, "name", Serializers.INTEGER)

                assertTrue(c1===c2 && c2===c3)
            }

            "import fails on existing"{
                TT.withTempFile { f ->
                    var db = DB.Maker.appendFile(f).make()
                    a.open(db, "name", Serializers.INTEGER)
                    db.close()

                    db = DB.Maker.appendFile(f).make()
                    val input = DataInput2ByteArray(ByteArray(0))
                    TT.assertFailsWith(DBException.WrongConfig::class){
                        a.import(db, "name", Serializers.INTEGER, input )
                    }
                }
            }

            "export"{
                val db1 = DB.Maker.heapSer().make()
                val c1 = a.open(db1, "aa", Serializers.INTEGER)

                for(i in 1..100)
                    a.add(c1, i)

                val output = DataOutput2ByteArray()
                (c1 as Exporter).exportToDataOutput2(output)

                val input = DataInput2ByteArray(output.copyBytes())

                val db2 = DB.Maker.heapSer().make()
                val c2 = a.import(db2, "aa", Serializers.INTEGER, input)

                val all = a.getAll(c2).toList()
                for(i in 1..100){
                    all[i-1] shouldBe i
                }
            }

            "cli export"{
                TT.withTempFile { dbf ->
                    var db = DB.Maker.appendFile(dbf).make()
                    var c = a.open(db, "name", Serializers.INTEGER)
                    for(i in 1..100)
                        a.add(c, i)
                    db.close()

                    val outf = TT.tempNotExistFile()
                    //export from cli
                    Export.main(arrayOf("-d",dbf.path, "-o", outf.path, "-n","name"))

                    outf.length() should {it>0L}

                    db = DB.Maker.heapSer().make()
                    val input = DataInput2ByteArray(outf.readBytes())
                    c = a.import(db, "name2", Serializers.INTEGER, input )

                    val all = a.getAll(c).toList()
                    for(i in 1..100){
                        all[i-1] shouldBe i
                    }
                }
            }


            "cli import"{
                TT.withTempFile { dbf ->
                    var db = DB.Maker.heapSer().make()
                    var c = a.open(db, "name", Serializers.INTEGER)
                    for(i in 1..100)
                        a.add(c, i)

                    val output = DataOutput2ByteArray()
                    (c as Exporter).exportToDataOutput2(output)

                    val outf = TT.tempFile()
                    outf.writeBytes(output.copyBytes())

                    //import from cli
                    Import.main(arrayOf("-d",dbf.path, "-i", outf.path, "-n","name"))

                    dbf.length() should {it>0L}

                    db = DB.Maker.appendFile(dbf).make()
                    c = a.open(db, "name", Serializers.INTEGER)

                    val all = a.getAll(c).toList()
                    for(i in 1..100){
                        all[i-1] shouldBe i
                    }
                }
            }

        }
    }

}){

    companion object {

        abstract class  Adapter<C>{
            abstract fun open(db: DB, name:String, serializer: Serializer<*>):C

            abstract fun import(db: DB, name:String, serializer: Serializer<*>, input: DataInput2):C

            abstract fun add(c:C, e:Any?)

            abstract fun getAll(c:C):Iterable<Any?>

            abstract val name:String
        }


        fun adapters():List<Adapter<Any>>{

            val qAdapter = object: Adapter<LinkedFIFOQueue<Any>>() {

                override fun import(db: DB, name: String, serializer: Serializer<*>, input: DataInput2): LinkedFIFOQueue<Any> {
                    return db.queue(name, serializer)
                            .importFromDataInput2(input)
                            .make() as LinkedFIFOQueue<Any>
                }


                override fun open(db: DB, name: String, serializer: Serializer<*>): LinkedFIFOQueue<Any> {
                    return db.queue(name, serializer).make() as LinkedFIFOQueue<Any>
                }

                override fun add(c: LinkedFIFOQueue<Any>, e: Any?) {
                    c.add(e)
                }

                override fun getAll(c: LinkedFIFOQueue<Any>): Iterable<Any?> {
                    return c
                }

                override val name = "queue"

            }

            return listOf(qAdapter) as List<Adapter<Any>>

        }
    }
}

