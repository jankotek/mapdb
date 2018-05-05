package org.mapdb.serializer

import io.kotlintest.properties.forAll
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
        }
    }

})

fun serializersAll(): List<Serializer<*>> {
    return arrayListOf()
}