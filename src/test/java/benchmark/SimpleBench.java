package benchmark;

import com.carrotsearch.junitbenchmarks.AbstractBenchmark;
import org.junit.Test;
import org.mapdb.CC;
import org.mapdb.DBMaker;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

import static org.junit.Assume.assumeTrue;

public class SimpleBench extends AbstractBenchmark{

    static final int size = (int) 1e5;

    protected void testMap(Map<Object,Object> m){
        for(int i=0;i<size;i++){
            m.put(i, ""+i);
        }

        for(int j=0;j<10;j++){
            for(int i=0;i<size;i++){
                if(m.get(i)=="aaa")
                    throw new Error();
            }
        }


    }

    @Test public void HTreeMap(){
        assumeTrue(CC.FULL_TEST);
        testMap(
                DBMaker.newMemoryDB().journalDisable().make().getHashMap("test")
        );
    }

    @Test public void BTreeMap(){
        assumeTrue(CC.FULL_TEST);
        testMap(
                DBMaker.newMemoryDB().journalDisable().make().getTreeMap("test")
        );
    }

    @Test public void HTreeMap_direct(){
        assumeTrue(CC.FULL_TEST);
        testMap(
                DBMaker.newDirectMemoryDB().journalDisable().make().getHashMap("test")
        );
    }

    @Test public void BTreeMap_direct(){
        assumeTrue(CC.FULL_TEST);
        testMap(
                DBMaker.newDirectMemoryDB().journalDisable().make().getTreeMap("test")
        );
    }

    @Test public void HTreeMap_file(){
        assumeTrue(CC.FULL_TEST);
        testMap(
                DBMaker.newTempFileDB().journalDisable().make().getHashMap("test")
        );
    }

    @Test public void BTreeMap_file(){
        assumeTrue(CC.FULL_TEST);
        testMap(
                DBMaker.newTempFileDB().journalDisable().make().getTreeMap("test")
        );
    }

    @Test public void SkipList(){
        assumeTrue(CC.FULL_TEST);
        testMap(
                new ConcurrentSkipListMap<Object, Object>()
        );
    }

    @Test public void HashMap(){
        assumeTrue(CC.FULL_TEST);
        testMap(
                new ConcurrentHashMap<Object, Object>()
        );
    }

}
