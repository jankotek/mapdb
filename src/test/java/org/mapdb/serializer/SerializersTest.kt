package org.mapdb.serializer

import io.kotlintest.matchers.beGreaterThan
import io.kotlintest.properties.forAll
import io.kotlintest.should
import io.kotlintest.specs.WordSpec
import org.mapdb.TT

class SerializersTest : WordSpec({


    val sers = Serializers::class.java.fields
            .filter { it.name != "INSTANCE" } //TODO remove this field from java
            .map { Pair(it.name, it.get(null) as Serializer<Any?>) }

    assert(sers.size > 0)


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
                "hash spread and collisions"{
                    val slotsCount = 100_000_000
                    val hashCount = 1e9.toLong()
                    val spread = IntArray(slotsCount)
                    val collisions = IntArray(slotsCount)
                    val iter = gen.random().iterator()
                    for(i in 0L until hashCount){
                        val r = iter.next()
                        val hash = r.hashCode()

                        // use div, to check its distributed from Integer.MIN_VALUE to Integer.MAX_VALUE
                        spread[Math.abs(hash / slotsCount)]++
                        // use modulo to check for hash collisions
                        collisions[Math.abs(hash % slotsCount)]++

                    }

                    for(i in 0 until slotsCount){
                        spread[i].toDouble() should beGreaterThan(0.1 * hashCount/slotsCount)
                        collisions[i].toDouble() should beGreaterThan(0.1 * hashCount/slotsCount)
                    }
                }
            }
        }
    }

})

fun serializersAll(): List<Serializer<*>> {
    return arrayListOf()
}