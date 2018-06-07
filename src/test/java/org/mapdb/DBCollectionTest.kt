package org.mapdb

import io.kotlintest.specs.WordSpec
import org.junit.Assert.assertTrue
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
        }
    }



}){

    companion object {

        abstract class  Adapter<C>{
            abstract fun open(db:DB, name:String, serializer: Serializer<*>):C

            abstract fun add(c:C, e:Any?)

            abstract fun getAll(c:C):Iterable<Any?>

            abstract val name:String
        }


        fun adapters():List<Adapter<*>>{

            val qAdapter = object: Adapter<LinkedQueue<Any>>() {

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

            return listOf(qAdapter)

        }
    }
}

