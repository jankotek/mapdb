package org.mapdb

import io.kotlintest.should
import io.kotlintest.shouldBe
import org.junit.Test
import org.mapdb.db.DB
import org.mapdb.ser.Serializers
import org.mapdb.store.StoreOnHeap
import org.mapdb.store.StoreOnHeapSer
import java.util.*

class DBTest{

    @Test
    fun store(){
        DB.newOnHeapDB().make().store should {it is StoreOnHeap}
        DB.newOnHeapSerDB().make().store should {it is StoreOnHeapSer }
    }

    @Test
    fun threadSafe(){
        fun DB.Maker.thSafe() = (this.make().store as StoreOnHeap).isThreadSafe
        DB.newOnHeapDB().thSafe() shouldBe  true
        DB.newOnHeapDB().threadSafeDisable().thSafe() shouldBe  false
        DB.newOnHeapDB().threadSafe(true).thSafe() shouldBe  true
        DB.newOnHeapDB().threadSafe(false).thSafe() shouldBe  false
    }

    @Test fun linkedList(){
        val s = DB.newOnHeapDB().make().linkedList("list", Serializers.LONG).make()
        s.put(1L)
        s.size shouldBe 1
    }

    fun randomParams():TreeMap<String,TreeMap<String,String>>{
        val params = TreeMap<String,TreeMap<String,String>>()
        val tName = TreeMap<String,String>()
        tName["aa"] = "bb"
        tName["aa2"] = "bb2"
        params["name"] = tName
        params["name2"] = tName

        return params
    }

    @Test fun paramsSer(){
        val db = DB.newOnHeapSerDB().make()
        val params = randomParams()
        db.paramsSave(params)

        db.paramsLoad() shouldBe params
    }

    @Test fun params_reopen(){
        TT.withTempFile { f->
            val params = randomParams()

            var db = DB.Maker.appendFile(f).make()
            db.paramsSave(params)
            db.close()

            db = DB.Maker.appendFile(f).make()
            db.paramsLoad() shouldBe  params
        }
    }
}