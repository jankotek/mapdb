package org.mapdb

import org.junit.Test
import java.io.IOException
import java.io.Serializable
import java.math.BigDecimal
import java.math.BigInteger
import java.util.*
import org.junit.Assert.*

abstract class SerializerTest<E>{

    protected val random = Random();

    /* reused byte[] */
    private val dataOutput = DataOutput2()

    abstract fun randomValue():E

    abstract val serializer:Serializer<E>

    val max = 1000L + TT.testScale() * 1000*10
    val arraySize = 10 + TT.testScale() * 100

    fun assertSerEquals(v1: Any?, v2: Any?) {
        assertTrue(serializer.equals(v1 as E, v2 as E))
        assertEquals(serializer.hashCode(v1, 0), serializer.hashCode(v2, 0))
    }


    @Test fun cloneEquals(){
        for(i in 0..max){
            val e = randomValue()
            val e2 = TT.clone(e,serializer, out = dataOutput)
            assertSerEquals(e, e2)
        }
    }

    @Test(timeout = 1000L)
    fun randomNotEquals(){
        // two random values should not be equal,
        // test will eventually timeout if they are always equal
        while(serializer.equals(randomValue(), randomValue())){

        }
    }

    @Test(timeout = 1000L)
    fun randomNotEqualHashCode(){
        //two random values should not have equal hash code,
        // test will eventually timeout if they are always equal
        while(serializer.hashCode(randomValue(),0) == serializer.hashCode(randomValue(),0)){

        }
    }

    @Test fun trusted(){
        assertTrue(serializer.isTrusted || serializer==Serializer.JAVA)
    }

    @Test fun fixedSize(){
        val size = serializer.fixedSize();
        if(size<0)
            return;
        for(i in 0..max) {
            val e = randomValue()
            val out = DataOutput2()
            serializer.serialize(out, e);
            assertEquals(size,out.pos)
        }
    }

    @Test fun compare() {
        for (i in 0..max) {
            val v1 = randomValue()
            val v2 = randomValue()
            serializer.compare(v1, v2)
        }
    }

    @Test open fun valueArrayBinarySearc(){
        var v = ArrayList<E>()
        for (i in 0..max) {
            v.add(randomValue())
        }
        Collections.sort(v, serializer)
        val keys = serializer.valueArrayFromArray(v.toArray())

        fun check(keys:Any, binary:ByteArray, e:E){
            val v1 = serializer.valueArraySearch(keys, e)
            val v2 = serializer.valueArraySearch(keys, e, serializer)
            val v3 = Arrays.binarySearch(serializer.valueArrayToArray(keys), e as Any, serializer as Comparator<Any>)

            assertEquals(v1, v3);
            assertEquals(v1, v2);

            val v4 = serializer.valueArrayBinarySearch(e, DataInput2.ByteArray(binary), v.size, serializer)
            assertEquals(v1, v4)
        }

        val out = DataOutput2();
        serializer.valueArraySerialize(out, keys)
        val deserialized = serializer.valueArrayDeserialize(DataInput2.ByteArray(out.buf), v.size);
        assertTrue(Arrays.deepEquals(serializer.valueArrayToArray(keys), serializer.valueArrayToArray(deserialized)))

        for (i in 0..max*10) {
            val e = randomValue()
            check(keys, out.buf, e)
        }

        for(e in v){
            check(keys, out.buf, e)
        }
    }

    @Test open fun valueArrayGet(){
        var v = Array<Any>(max.toInt(), {i->randomValue() as Any})
        val keys = serializer.valueArrayFromArray(v)
        val out = DataOutput2()
        serializer.valueArraySerialize(out, keys)

        for(i in 0 until max.toInt()){
            val v1 = v[i] as E
            val v2 = serializer.valueArrayGet(keys, i)
            val v3 = serializer.valueArrayBinaryGet(DataInput2.ByteArray(out.buf), max.toInt(), i)

            assertTrue(serializer.equals(v1, v2))
            assertTrue(serializer.equals(v1, v3))
        }

    }

    fun randomValueArray():Any{
        var o = serializer.valueArrayEmpty()
        for(i in 0 until arraySize){
            val r = randomValue()
            o = serializer.valueArrayPut(o, random.nextInt(i+1),r)
        }
        return o
    }

    fun cloneValueArray(vals:Any):Any{
        val out = dataOutput;
        out.pos = 0
        val size = serializer.valueArraySize(vals)
        serializer.valueArraySerialize(out, vals);
        val input = DataInput2.ByteArray(out.buf)
        val ret = serializer.valueArrayDeserialize(input,size)

        assertEquals(out.pos, input.pos)

        return ret;
    }

    fun assertValueArrayEquals(vals1:Any, vals2:Any){
        val size = serializer.valueArraySize(vals1)
        assertEquals(size, serializer.valueArraySize(vals2))

        for(i in 0 until size){
            val v1 = serializer.valueArrayGet(vals1, i)
            val v2 = serializer.valueArrayGet(vals2, i)

            assertSerEquals(v1, v2)
        }
    }


    @Test open fun valueArraySerDeser(){
        if(serializer.needsAvailableSizeHint())
            return
        for(i in 0..max){
            val e = randomValueArray()
            val e2 = cloneValueArray(e)
            assertValueArrayEquals(e,e2)
        }
    }

    @Test open fun valueArrayDeleteValue(){
        for(i in 0..max){
            val vals = randomValueArray()
            val valsSize = serializer.valueArraySize(vals);
            if(valsSize==0)
                continue;
            val pos = 1+random.nextInt(valsSize-1);

            val vals2 = serializer.valueArrayDeleteValue(vals, pos);
            assertEquals(valsSize-1, serializer.valueArraySize(vals2))

            val arr1 = DataIO.arrayDelete(serializer.valueArrayToArray(vals), pos, 1);
            val arr2 = serializer.valueArrayToArray(vals2);

            arr1.forEachIndexed { i, any ->
                assertSerEquals(any, arr2[i])
            }
        }

    }

    @Test open fun valueArrayCopyOfRange(){
        for(i in 0..max){
            val vals = randomValueArray()
            val valsSize = serializer.valueArraySize(vals);
            if(valsSize<5)
                continue;
            val pos = 1+random.nextInt(valsSize-4);
            val vals2 = serializer.valueArrayCopyOfRange(vals,pos,pos+3);

            val arr1a = serializer.valueArrayToArray(vals);
            val arr1 = Arrays.copyOfRange(arr1a, pos, pos+3)

            val arr2 = serializer.valueArrayToArray(vals2);

            arr1.forEachIndexed { i, any ->
                assertSerEquals(any, arr2[i])
            }
        }

    }

}

class Serializer_CHAR:SerializerTest<Char>(){
    override fun randomValue() = random.nextInt().toChar()
    override val serializer = Serializer.CHAR
}

class Serializer_STRINGXXHASH:SerializerTest<String>(){
    override fun randomValue() = TT.randomString(random.nextInt(1000))
    override val serializer = Serializer.STRING_ORIGHASH
}

class Serializer_STRING:SerializerTest<String>(){
    override fun randomValue() = TT.randomString(random.nextInt(1000))
    override val serializer = Serializer.STRING
}

class Serializer_STRING_INTERN:SerializerTest<String>(){
    override fun randomValue() = TT.randomString(random.nextInt(1000))
    override val serializer = Serializer.STRING_INTERN
}

class Serializer_STRING_ASCII:SerializerTest<String>(){
    override fun randomValue() = TT.randomString(random.nextInt(1000))
    override val serializer = Serializer.STRING_ASCII
}

class Serializer_STRING_NOSIZE:SerializerTest<String>(){
    override fun randomValue() = TT.randomString(random.nextInt(1000))
    override val serializer = Serializer.STRING_NOSIZE

    override fun valueArrayBinarySearc() {
    }

    override fun valueArrayCopyOfRange() {
    }

    override fun valueArrayDeleteValue() {
    }

    override fun valueArrayGet() {
    }

    override fun valueArraySerDeser() {
    }
}

class Serializer_LONG:SerializerTest<Long>(){
    override fun randomValue() = random.nextLong()
    override val serializer = Serializer.LONG
}

class Serializer_LONG_PACKED:SerializerTest<Long>(){
    override fun randomValue() = random.nextLong()
    override val serializer = Serializer.LONG_PACKED
}


class Serializer_INTEGER:SerializerTest<Int>(){
    override fun randomValue() = random.nextInt()
    override val serializer = Serializer.INTEGER
}

class Serializer_INTEGER_PACKED:SerializerTest<Int>(){
    override fun randomValue() = random.nextInt()
    override val serializer = Serializer.INTEGER_PACKED
}
//
//class Serializer_LONG_PACKED_ZIGZAG:SerializerTest<Long>(){
//    override fun randomValue() = random.nextLong()
//    override val serializer = Serializer.LONG_PACKED_ZIGZAG
//}
//
//class Serializer_INTEGER_PACKED_ZIGZAG:SerializerTest<Int>(){
//    override fun randomValue() = random.nextInt()
//    override val serializer = Serializer.INTEGER_PACKED_ZIGZAG
//}

class Serializer_BOOLEAN:SerializerTest<Boolean>(){
    override fun randomValue() = random.nextBoolean()
    override val serializer = Serializer.BOOLEAN
}

class Serializer_RECID:SerializerTest<Long>(){
    override fun randomValue() = random.nextLong().and(0xFFFFFFFFFFFFL) //6 bytes
    override val serializer = Serializer.RECID
}

class Serializer_RECID_ARRAY:SerializerTest<LongArray>(){
    override fun randomValue():LongArray {
        val ret = LongArray(random.nextInt(50));
        for(i in 0 until ret.size){
            ret[i] = random.nextLong().and(0xFFFFFFFFFFFFL) //6 bytes
        }
        return ret
    }

    override val serializer = Serializer.RECID_ARRAY
}

class Serializer_BYTE_ARRAY:SerializerTest<ByteArray>(){
    override fun randomValue() = TT.randomByteArray(random.nextInt(50))
    override val serializer = Serializer.BYTE_ARRAY
}

class Serializer_BYTE_ARRAY_NOSIZE:SerializerTest<ByteArray>(){
    override fun randomValue() = TT.randomByteArray(random.nextInt(50))
    override val serializer = Serializer.BYTE_ARRAY_NOSIZE


    override fun valueArrayBinarySearc() {
    }

    override fun valueArrayCopyOfRange() {
    }

    override fun valueArrayDeleteValue() {
    }

    override fun valueArrayGet() {
    }

    override fun valueArraySerDeser() {
    }
}


class Serializer_BYTE:SerializerTest<Byte>(){
    override fun randomValue() = random.nextInt().toByte()
    override val serializer = Serializer.BYTE
}

class Serializer_CHAR_ARRAY:SerializerTest<CharArray>(){
    override fun randomValue():CharArray {
        val ret = CharArray(random.nextInt(50));
        for(i in 0 until ret.size){
            ret[i] = random.nextInt().toChar()
        }
        return ret
    }
    override val serializer = Serializer.CHAR_ARRAY
}

class Serializer_INT_ARRAY:SerializerTest<IntArray>(){
    override fun randomValue():IntArray {
        val ret = IntArray(random.nextInt(50));
        for(i in 0 until ret.size){
            ret[i] = random.nextInt()
        }
        return ret
    }
    override val serializer = Serializer.INT_ARRAY
}


class Serializer_LONG_ARRAY:SerializerTest<LongArray>(){
    override fun randomValue():LongArray {
        val ret = LongArray(random.nextInt(30));
        for(i in 0 until ret.size){
            ret[i] = random.nextLong()
        }
        return ret
    }
    override val serializer = Serializer.LONG_ARRAY
}

class Serializer_DOUBLE_ARRAY:SerializerTest<DoubleArray>(){
    override fun randomValue():DoubleArray {
        val ret = DoubleArray(random.nextInt(30));
        for(i in 0 until ret.size){
            ret[i] = random.nextDouble()
        }
        return ret
    }
    override val serializer = Serializer.DOUBLE_ARRAY
}


class Serializer_JAVA:SerializerTest<Any>(){
    override fun randomValue() = TT.randomString(1000)
    override val serializer = Serializer.JAVA

    internal class Object2 : Serializable

    open internal class CollidingObject(val value: String) : Serializable {
        override fun hashCode(): Int {
            return this.value.hashCode() and 1
        }

        override fun equals(obj: Any?): Boolean {
            return obj is CollidingObject && obj.value == value
        }
    }

    internal class ComparableCollidingObject(value: String) : CollidingObject(value), Comparable<ComparableCollidingObject>, Serializable {
        override fun compareTo(o: ComparableCollidingObject): Int {
            return value.compareTo(o.value)
        }
    }

    @Test fun clone1(){
        val v = TT.clone(Object2(), Serializer.JAVA)
        assertTrue(v is Object2)
    }

    @Test fun clone2(){
        val v = TT.clone(CollidingObject("111"), Serializer.JAVA)
        assertTrue(v is CollidingObject)
        assertSerEquals("111", (v as CollidingObject).value)
    }

    @Test fun clone3(){
        val v = TT.clone(ComparableCollidingObject("111"), Serializer.JAVA)
        assertTrue(v is ComparableCollidingObject)
        assertSerEquals("111", (v as ComparableCollidingObject).value)

    }

}

class Serializer_UUID:SerializerTest<UUID>(){
    override fun randomValue() = UUID(random.nextLong(), random.nextLong())
    override val serializer = Serializer.UUID
}

class Serializer_FLOAT:SerializerTest<Float>(){
    override fun randomValue() = random.nextFloat()
    override val serializer = Serializer.FLOAT
}

class Serializer_FLOAT_ARRAY:SerializerTest<FloatArray>(){
    override fun randomValue():FloatArray {
        val ret = FloatArray(random.nextInt(50));
        for(i in 0 until ret.size){
            ret[i] = random.nextFloat()
        }
        return ret
    }
    override val serializer = Serializer.FLOAT_ARRAY
}



class Serializer_DOUBLE:SerializerTest<Double>(){
    override fun randomValue() = random.nextDouble()
    override val serializer = Serializer.DOUBLE
}

class Serializer_SHORT:SerializerTest<Short>(){
    override fun randomValue() = random.nextInt().toShort()
    override val serializer = Serializer.SHORT
}

class Serializer_SHORT_ARRAY:SerializerTest<ShortArray>(){
    override fun randomValue():ShortArray {
        val ret = ShortArray(random.nextInt(50));
        for(i in 0 until ret.size){
            ret[i] = random.nextInt().toShort()
        }
        return ret
    }
    override val serializer = Serializer.SHORT_ARRAY
}

class Serializer_BIG_INTEGER:SerializerTest<BigInteger>(){
    override fun randomValue() = BigInteger(random.nextInt(50),random)
    override val serializer = Serializer.BIG_INTEGER
}

class Serializer_BIG_DECIMAL:SerializerTest<BigDecimal>(){
    override fun randomValue() = BigDecimal(BigInteger(random.nextInt(50),random), random.nextInt(100))
    override val serializer = Serializer.BIG_DECIMAL
}

class Serializer_DATE:SerializerTest<Date>(){
    override fun randomValue() = Date(random.nextLong())
    override val serializer = Serializer.DATE
}


class SerializerCompressionWrapperTest():SerializerTest<ByteArray>(){
    override fun randomValue() = TT.randomByteArray(random.nextInt(1000))

    override val serializer = Serializer.CompressionWrapper(Serializer.BYTE_ARRAY)

    @Test
    fun compression_wrapper() {
        var b = ByteArray(100)
        Random().nextBytes(b)
        assertTrue(Serializer.BYTE_ARRAY.equals(b, TT.clone(b, serializer)))

        b = Arrays.copyOf(b, 10000)
        assertTrue(Serializer.BYTE_ARRAY.equals(b, TT.clone(b, serializer)))

        val out = DataOutput2()
        serializer.serialize(out, b)
        assertTrue(out.pos < 1000)
    }

}

class Serializer_DeflateWrapperTest():SerializerTest<ByteArray>() {
    override fun randomValue() = TT.randomByteArray(random.nextInt(1000))
    override val serializer = Serializer.CompressionDeflateWrapper(Serializer.BYTE_ARRAY)


    @Test fun deflate_wrapper() {
        val c = Serializer.CompressionDeflateWrapper(Serializer.BYTE_ARRAY, -1,
                byteArrayOf(1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 23, 4, 5, 6, 7, 8, 9, 65, 2))

        val b = byteArrayOf(1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 4, 5, 6, 3, 3, 3, 3, 35, 6, 67, 7, 3, 43, 34)

        assertTrue(Arrays.equals(b, TT.clone(b, c)))
    }

}


class Serializer_Array(): SerializerTest<Array<Any>>(){
    override fun randomValue() = Array<Any>(random.nextInt(30), {TT.randomString(random.nextInt(30))})

    override val serializer = Serializer.ArraySer(Serializer.STRING as Serializer<Any>)

    @Test fun array() {
        val s:Serializer<Array<Any>> = Serializer.ArraySer(Serializer.INTEGER as Serializer<Any>)

        val a:Array<Any> = arrayOf(1, 2, 3, 4)

        assertTrue(Arrays.equals(a, TT.clone(a, s)))
    }

}


class SerializerLookup(){
    @Test fun lookup(){
        assertEquals(Serializer.LONG, Serializer.serializerForClass(Long::class.java))
        assertEquals(Serializer.LONG_ARRAY, Serializer.serializerForClass(LongArray::class.java))
        assertEquals(Serializer.UUID, Serializer.serializerForClass(UUID::class.java))
        assertEquals(Serializer.STRING, Serializer.serializerForClass(String::class.java))
        assertNull(Serializer.serializerForClass(SerializerLookup::class.java))
    }

}