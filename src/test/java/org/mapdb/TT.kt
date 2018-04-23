package org.mapdb

import org.junit.Assert.*
import org.junit.Test
import org.mapdb.io.*
import org.mapdb.serializer.Serializer
import org.mapdb.serializer.Serializers
import org.mapdb.store.MutableStore
import java.io.*
import java.util.*
import java.util.concurrent.*
import java.util.concurrent.atomic.AtomicReference

/**
 * Unit tests utils
 */
object TT{

    val bools = booleanArrayOf(false,true)
    val boolsTrue = booleanArrayOf(true)
    val boolsFalse = booleanArrayOf(false)

    @JvmStatic fun randomByteArray(size: Int, seed: Int= Random().nextInt()): ByteArray {
        var randomSeed = seed
        val ret = ByteArray(size)
        for (i in ret.indices) {
            ret[i] = randomSeed.toByte()
            randomSeed = 269 * randomSeed + DataIO.intHash(randomSeed)
        }
        return ret
    }

    @JvmStatic fun randomFillStore(store: MutableStore, size:Int=1000, seed:Long=Random().nextLong()){
        val random = Random(seed)
        for(i in 0..size){
            val bytes = randomByteArray(random.nextInt(100),seed=random.nextInt());
            store.put(
                    bytes,
                    Serializers.BYTE_ARRAY_NOSIZE);
        }
    }

    @JvmStatic fun randomString(size: Int, seed: Int=Random().nextInt()): String {
        val chars = "0123456789abcdefghijklmnopqrstuvwxyz !@#$%^&*()_+=-{}[]:\",./<>?|\\".toCharArray()
        var seed2 = seed
        val b = StringBuilder(size)
        for (i in 0..size - 1) {
            b.append(chars[Math.abs(seed2) % chars.size])
            seed2 = 31 * seed2 + DataIO.intHash(seed2)
        }
        return b.toString()
    }

    private val tempDir = System.getProperty("java.io.tmpdir");

    /*
     * Create temporary file in temp folder. All associated db files will be deleted on JVM exit.
     */
    @JvmStatic fun tempFile(): File {
        fun sanitize(name:String) = java.lang.String(name).replaceAll("[^a-zA-Z0-9_\\.]+","")

        val stackTrace = Thread.currentThread().stackTrace;
        val elem = stackTrace[2];
        val prefix = "mapdbTest_"+sanitize(elem.className)+"-"+sanitize(elem.methodName)+"-"+elem.lineNumber+"_"
        while(true){
            val file = File(tempDir+File.separator+prefix+System.currentTimeMillis()+"_"+Math.random());
            if(file.exists().not()) {
                file.deleteOnExit()
                return file
            }
        }
    }

    @JvmStatic fun tempDir(): File {
        val ret = tempFile()
        ret.mkdir()
        return ret
    }

    @JvmStatic fun tempDelete(file: File){
        val name = file.getName()
        for (f2 in file.getParentFile().listFiles()!!) {
            if (f2.name.startsWith(name))
                tempDeleteRecur(f2)
        }
        tempDeleteRecur(file)
    }

    @JvmStatic fun tempDeleteRecur(file: File) {
        if(file.isDirectory){
            for(child in file.listFiles())
                tempDeleteRecur(child)
        }
        file.delete()
    }


    object Serializer_ILLEGAL_ACCESS: Serializer<Any> {
        override fun serialize(value: Any, out: DataOutput2) {
            throw AssertionError("Should not access this serializer")
        }

        override fun deserialize(dataIn: DataInput2): Any {
            throw AssertionError("Should not access this serializer")
        }

    }

    /** how many hours should unit tests run? Controlled by:
     * `mvn test -Dmdbtest=2`
     * @return test scale
     */
    @JvmStatic fun testScale(): Int {
        val prop = System.getProperty("mdbtest")?:"0";
        try {
            return Integer.valueOf(prop);
        } catch(e:NumberFormatException) {
            return 0;
        }
    }

    @JvmStatic fun testRuntime(minutes:Int): Long = 3000L + minutes * 60 * 1000 * testScale()


    @JvmStatic fun nowPlusMinutes(minutes: Double): Long {
        return System.currentTimeMillis() + 2000 + (testScale() * 1000.0 * 60.0 * minutes).toLong()
    }


    @JvmStatic fun shortTest(): Boolean {
        return testScale() == 0
    }

    /* clone value using serialization */
    @JvmStatic fun <E> clone(value: E, serializer: Serializer<*>, out:DataOutput2 = DataOutput2ByteArray()): E {
        @Suppress("UNCHECKED_CAST")
        (serializer as Serializer<E>).serialize(value, out)
        val in2 = DataInput2ByteArray(out.copyBytes())
        return serializer.deserialize(in2)
    }

    /* clone value using java serialization */
    @JvmStatic fun <E> cloneJavaSerialization(value: E): E {
        val out = ByteArrayOutputStream()
        val out2 = ObjectOutputStream(out)
        out2.writeObject(value)
        out2.flush()

        val in2 = ByteArrayInputStream(out.toByteArray())
        @Suppress("UNCHECKED_CAST")
        return ObjectInputStream(in2).readObject() as E
    }

    @JvmStatic fun <E> serializedSize(value: E, serializer: Serializer<*>, out:DataOutput2 = DataOutput2ByteArray()): Int {
        @Suppress("UNCHECKED_CAST")
        (serializer as Serializer<E>).serialize(value, out)
        return out.copyBytes().size;
    }


    fun fork(count:Int=1, body:(i:Int)->Unit){
        val finish = async(count=count, body=body)
        finish()
    }


    fun async(count:Int=1, body:(i:Int)->Unit):()->Unit{
        val exec = executor(count)
        val wait = CountDownLatch(1)
        val exception = AtomicReference<Throwable>()
        for(i in 0 until count){
            exec.submit {
                try{
                    wait.await()
                    body(i)
                }catch(e:Throwable){
                    e.printStackTrace()
                    exception.set(e)
                }
            }
        }
        wait.countDown()
        exec.shutdown()
        return {

            while(!exec.awaitTermination(1, TimeUnit.MILLISECONDS)){
                val e = exception.get()
                if(e!=null)
                    throw AssertionError(e)
            }
        }
    }



    fun forkExecutor(exec: ExecutorService, count:Int=1, body:(i:Int)->Unit){
        val exception = AtomicReference<Throwable>()
        val wait = CountDownLatch(1)
        val tasks = (0 until count).map{i->
            exec.submit {
                try{
                    wait.await()
                    body(i)
                }catch(e:Throwable){
                    exception.set(e)
                }
            }
        }.toMutableSet()
        wait.countDown()

        //await for all tasks to finish
        while(!tasks.isEmpty()){
            val iter = tasks.iterator()
            while(iter.hasNext()){
                if(iter.next().isDone)
                    iter.remove()
            }

            val e = exception.get()
            if(e!=null)
                throw AssertionError(e)
            Thread.sleep(1)
        }
    }


    fun assertAllZero(old: ByteArray) {
        val z = 0.toByte()
        for( o in old){
            if(o!=z)
                throw AssertionError()
        }
    }

    /** executor service with deamon threads, so Unit Test JVM can exit */
    fun executor(threadCount:Int=1): ScheduledExecutorService {
        return Executors.newScheduledThreadPool(threadCount) { r->
            val t = Thread(r)
            t.isDaemon = true
            t
        }
    }


    fun <E> reflectionInvokeMethod(obj:Any, name:String, clazz:Class<*> = obj.javaClass):E{
        val method = clazz.getDeclaredMethod(name)
        method.isAccessible = true
        return method.invoke(obj) as E
    }


    fun <E> reflectionGetField(obj:Any, name:String, clazz:Class<*> = obj.javaClass):E{
        val field = clazz.getDeclaredField(name)
        field.isAccessible = true
        return field.get(obj) as E
    }

    /**
     * Catches expected exception, rethrows everything else.
     *
     * Compared to [kotlin.test.assertFailsWith] does not swallow wrong exceptions, better for debuging
     *
     */
    inline fun <T : Throwable> assertFailsWith(exceptionClass: kotlin.reflect.KClass<T>, block: () -> Unit) {
        try {
            block()
            fail("Expected exception ${exceptionClass}")
        } catch (e: Throwable) {
            if (exceptionClass.isInstance(e)) {
                @Suppress("UNCHECKED_CAST")
                return
            }
            throw e
        }
    }


    inline fun withTempFile(f:(file:File)->Unit){
        val file = TT.tempFile()
        file.delete();
        try{
            f(file)
        }finally {
            if(!file.delete())
                file.deleteOnExit()
        }
    }


    inline fun withTempDir(f:(dir:File)->Unit){
        val dir = TT.tempDir()
        try{
            f(dir)
        }finally {
            dir.deleteRecursively()
        }
    }
}

class TTTest{
    @Test fun _test_recur_delete(){
        val f = TT.tempDir();
        val f2 = File(f.path+"/aa/bb")
        f2.mkdirs();
        val raf = RandomAccessFile(f2.path+"/aaa","rw");
        raf.writeInt(111)
        raf.close()

        val f0 = File(f.path+".wal23432")
        val raf2 = RandomAccessFile(f0,"rw");
        raf2.writeInt(111)
        raf2.close()


        TT.tempDelete(f)
        assertFalse(f.exists())
        assertFalse(f0.exists())
    }

    @Test fun clone2(){
        val s = "djwqoidjioqwdjiqw 323423";
        assertEquals(s, TT.clone(s, Serializers.STRING))
        assertEquals(s, TT.cloneJavaSerialization(s))
    }

    @Test fun tempFileName_textets(){
        val f = TT.tempFile()
        assertTrue(f.name,f.name.startsWith("mapdbTest_org.mapdb.TTTest-tempFileName_textets-"))
    }
}
