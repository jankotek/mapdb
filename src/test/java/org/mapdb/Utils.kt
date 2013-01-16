package org.mapdb


/**
 * Package level Test Utilities
 */

private val runtime = Runtime.getRuntime()!!;

fun assertHeapMemAvail(mem:Double){
    if(runtime.maxMemory() <mem)
        throw Error("Not enought HEAP memory, expected ${mem/1e6}, found ${runtime.maxMemory()/1e6}. \nUse '-Xmx2G' JVM option");
}

fun ensureFreeHeap(){
    for (i in 0..1000) {
        if(runtime.freeMemory()<16e6)
            return;
        System.gc();
        Thread.sleep(1);
    }
    throw Error("Too much HEAP memory is used ${runtime.freeMemory()/1e6}");
}

fun assertDirectMemAvail(mem:Double){
    val maxMem = sun.misc.VM.maxDirectMemory();
    if(maxMem < mem)
        throw Error("Not enought DIRECT memory, expected ${mem/1e6}, found ${maxMem/1e6}. \n"+
                "Use JVM option '-XX:MaxDirectMemorySize2G'");
    runnable{

    }

}

fun testEngine():Engine = DBMaker.newMemoryDB().journalDisable().makeEngine();

public inline fun runnable2<A,B>(action: (a:A?,b:B?)-> Unit): Fun.Runnable2<A?,B?> {
    return object: Fun.Runnable2<A?,B?> {
        public override fun run(a: A?, b: B?) {
            action(a,b)
        }

    }
}

public inline fun runnable3<A,B,C>(action: (a:A?,b:B?,c:C?)-> Unit): Fun.Runnable3<A?,B?,C?> {
    return object: Fun.Runnable3<A?,B?,C?> {
        public override fun run(a: A?, b: B?, c:C?) {
            action(a,b,c)
        }

    }
}

public inline fun mapListener<K,V>(action: (key:K?,oldVal:V?,newVal:V?)-> Unit): Bind.MapListener<K?,V?> {
    return object: Bind.MapListener<K?,V?> {
        public override fun update(key: K?, oldVal: V?, newVal:V?) {
            action(key, oldVal, newVal)
        }

    }
}