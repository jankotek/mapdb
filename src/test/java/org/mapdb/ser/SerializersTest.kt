package org.mapdb.ser

import io.kotlintest.matchers.*
import io.kotlintest.properties.forAll
import io.kotlintest.should
import org.mapdb.DBWordSpec
import org.mapdb.TT

class SerializersTest : DBWordSpec({

    val sers = Serializers::class.java.fields
            .filter { it.name != "INSTANCE" } //TODO remove this field from java
            .map { Pair(it.name, it.get(null) as Serializer<Any?>) }

    assert(sers.isNotEmpty())


    for ((name, ser) in sers) {
        name should {
            val gen = TT.genFor(ser.serializedType())
            "equals after clone "{
                forAll(gen) { a ->
                    val b = Serializers.clone(a, ser)
                    ser.equals(a, b)
                }
            }
            "hash after clone"{
                forAll(gen) { a ->
                    val b = Serializers.clone(a, ser)
                    ser.hashCode(a) == ser.hashCode(b!!)
                }
            }

            "equals not constant"{
                forAll(gen, gen) { a, b ->
                    Serializers.binaryEqual(ser, a, b) == ser.equals(a, b)
                }
            }


            "hash not constant"{
                forAll(gen, gen) { a, b ->
                    Serializers.binaryEqual(ser, a, b) == (ser.hashCode(a) == ser.hashCode(b))
                }
            }


            if(!TT.shortTest()){
                "hash variance and collisions"{
                    val slotsCount = 100_000
                    val hashCount = 1e9.toLong()
                    val variance = LongArray(slotsCount)
                    val collisions = LongArray(slotsCount)
                    val iter = gen.random().iterator()
                    for(i in 0L until hashCount){
                        val r = iter.next()
                        val hash = r.hashCode()

                        // use div, to check its distributed from Integer.MIN_VALUE to Integer.MAX_VALUE
                        variance[Math.abs(hash / slotsCount)]++
                        // use modulo to check for hash collisions
                        collisions[Math.abs(hash % slotsCount)]++

                    }

                    for(c in collisions){
                         c.toDouble() should beGreaterThan(0.1 * hashCount/slotsCount)
                    }

                    val lowVarCount = variance.filter{it< 0.0001 * hashCount/slotsCount }.size
                    // 80% for almost empty hashes slots seems like too much, perhaps problem in random generator?
                    lowVarCount.toDouble() should beLessThan(0.8 * slotsCount)

                }
            }
        }
    }

})

fun serializersAll(): List<Serializer<*>> {
    return arrayListOf()
}