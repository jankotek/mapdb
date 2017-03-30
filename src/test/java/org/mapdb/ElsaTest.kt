package org.mapdb

import org.junit.Assert.assertTrue
import org.junit.Test
import java.io.Externalizable
import java.io.ObjectInput
import java.io.ObjectOutput
import java.io.Serializable

class ElsaTestMyClass: Serializable {
    val i = 11
    val s = "dqodoiwqido"
}

class ElsaTestExternalizable: Externalizable {

    override fun readExternal(input: ObjectInput) {
        input.readInt()
        input.readUTF()
    }

    override fun writeExternal(out: ObjectOutput) {
        out.writeInt(i)
        out.writeUTF(s)
    }

    val i = 11
    val s = "dqodoiwqido"
}

class ElsaTest{
    fun size(serializer: Serializer<*>, value:Any):Int{
        val out = DataOutput2()
        @Suppress("UNCHECKED_CAST")
        (serializer as Serializer<Any?>).serialize(out, value)
        return out.pos
    }

    @Test fun sizeSerializable(){
        val my = ElsaTestMyClass()
        val javaSize = size(Serializer.JAVA, my)
        val defSize = size(DBMaker.memoryDB().make().defaultSerializer, my)
        val regDB = DBMaker.memoryDB().make()
        regDB.defaultSerializerRegisterClass(ElsaTestMyClass::class.java)
        val defRegSize = size(regDB.defaultSerializer, my)

//        println("$javaSize - $defSize - $defRegSize")

        assertTrue(javaSize>defSize)
        assertTrue(defSize>defRegSize)
    }


    @Test fun sizeExtern(){
        val my = ElsaTestExternalizable()
        val javaSize = size(Serializer.JAVA, my)
        val defSize = size(DBMaker.memoryDB().make().defaultSerializer, my)
        val regDB = DBMaker.memoryDB().make()
        regDB.defaultSerializerRegisterClass(ElsaTestExternalizable::class.java)
        val defRegSize = size(regDB.defaultSerializer, my)

//        println("$javaSize - $defSize - $defRegSize")

        assertTrue(javaSize>defSize)
        assertTrue(defSize>defRegSize)
    }
}