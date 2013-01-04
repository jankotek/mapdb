package benchmark

import java.util.*
import org.mapdb.*


/**
 * Compares performance of in-memory maps from `Java.util`, Heap memory and Direct memory maps
 */

fun main(args : Array<String>) {

    val memReq = 25e9
    val v = args[0].substring(0,1);

    val m:MutableMap<String, String> =
        if(v == "j"){
            assertHeapMemAvail(memReq)
            TreeMap();
        }else if (v == "h"){
            assertHeapMemAvail(memReq)
            DBMaker.newMemoryDB().journalDisable().make().getTreeMap("test");
        }else if (v == "d"){
            assertHeapMemAvail(memReq)
            DBMaker.newDirectMemoryDB().journalDisable().make().getTreeMap("test");
        }else{
            throw Error("Unknown option: "+v)
        }

    var i = 1.toLong()
    while(true){
        m.put(i.toString(), i.toString());
        i++;
        if (i % 1e6.toLong()==0.toLong())
            println(i);
    }

}
