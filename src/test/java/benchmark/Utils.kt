package benchmark


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
}
