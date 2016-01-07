package org.mapdb.benchmark

import java.io.File
import java.util.*

/**
 * Benchmark utilities
 */
object Bench{

    val propFile = File("target/bench.log")

    fun bench(benchName:String=callerName(), body:()->Long){
        val many = testScale()>0;
        val metric =
                if(!many) {
                    body()
                }else{
                    //run many times and do average
                    var t=0L;
                    for(i in 0 until 100)
                        t += body()
                    t/100
                }

        println("BENCH: $benchName - $metric")

        //load, update and save property file with results
        val props = Properties()
        if(propFile.exists())
            props.load(propFile.inputStream().buffered())
        props.put(benchName, metric.toString())
        val out = propFile.outputStream()
        val out2 = out.buffered()
        props.store(out,"mapdb benchmark")
        out2.flush()
        out.close()
    }

    fun stopwatch(body:()->Unit):Long{
        val start = System.currentTimeMillis()
        body()
        return System.currentTimeMillis() - start
    }

    /** returns class name and method name of caller from previous stack trace frame */
    inline fun callerName():String{
        val t = Thread.currentThread().stackTrace
        val t0 = t[2]
        return t0.className+"."+t0.methodName
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
}
