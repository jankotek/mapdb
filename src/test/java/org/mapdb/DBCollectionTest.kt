package org.mapdb

import io.kotlintest.shouldBe
import io.kotlintest.specs.WordSpec
import org.junit.Assert.assertTrue
import org.mapdb.io.DataInput2
import org.mapdb.io.DataInput2ByteArray
import org.mapdb.io.DataOutput2ByteArray
import org.mapdb.queue.LinkedQueue
import org.mapdb.serializer.Serializer
import org.mapdb.serializer.Serializers

class DBCollectionTest: WordSpec({

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

            "export/import"{
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
        }
    }



}){

    companion object {

        abstract class  Adapter<C>{
            abstract fun open(db:DB, name:String, serializer: Serializer<*>):C

            abstract fun import(db:DB, name:String, serializer: Serializer<*>, input: DataInput2):C

            abstract fun add(c:C, e:Any?)

            abstract fun getAll(c:C):Iterable<Any?>

            abstract val name:String
        }


        fun adapters():List<Adapter<Any>>{

            val qAdapter = object: Adapter<LinkedQueue<Any>>() {

                override fun import(db: DB, name: String, serializer: Serializer<*>, input: DataInput2): LinkedQueue<Any> {
                    return db.queue(name, serializer)
                            .importFromDataInput2(input)
                            .make() as LinkedQueue<Any>
                }


                override fun open(db: DB, name: String, serializer: Serializer<*>): LinkedQueue<Any> {
                    return db.queue(name, serializer).make() as LinkedQueue<Any>
                }

                override fun add(c: LinkedQueue<Any>, e: Any?) {
                    c.add(e)
                }

                override fun getAll(c: LinkedQueue<Any>): Iterable<Any?> {
                    return c
                }

                override val name = "queue"

            }

            return listOf(qAdapter) as List<Adapter<Any>>

        }
    }
}

