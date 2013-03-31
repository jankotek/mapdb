package examples;

import org.mapdb.DB;
import org.mapdb.DBMaker;

import java.io.File;
import java.util.Map;
import java.util.Random;

/**
 * Demonstrates how BTree parameters affects performance. BTreeMap has two key parameters
 * which affects its performance:
 * <h4>Maximal node size</h4>
 * Controls how but BTree node can get, before it splits.
 * All keys and values in BTree node are stored and deserialized together.
 * Large nodes means fewer disk access (tree structure is shallower),
 * but also more data to read (more keys to be deserialized).
 *
 * <h4>Store values inside node</h4>
 * Value may be stored inside or outside of BTree node.
 * It is recommended to store large values outside nodes.
 *
 *
 *
 * <p/>
 * Sample output
 * <pre>
 *  Node size |  small vals  |  large vals  |  large vals outside node
 *     6      |       25 s   |       89 s   |    49 s   |
 *    18      |       25 s   |      144 s   |    50 s   |
 *    32      |       57 s   |      175 s   |    31 s   |
 *    64      |       53 s   |      231 s   |    49 s   |
 *   120      |       73 s   |       98 s   |    49 s   |
 * </pre>
 */
public class TreeMap_Performance_Tunning {


    static final int[] nodeSizes = {6, 18, 32, 64, 120};


    public static void main(String[] args) {
        Random r = new Random();



        System.out.println(" Node size |  small vals  |  large vals  |  large vals outside node" );

        for(int nodeSize:nodeSizes){

            System.out .print("    "+nodeSize+"      |");

            for(int j=0;j<3;j++){

                boolean useSmallValues = (j==0);
                boolean valueOutsideOfNodes = (j==2);

                DB db = DBMaker
                        .newFileDB(new File("/mnt/big/adsasd"))
                        .deleteFilesAfterClose()
                        .closeOnJvmShutdown()
                        .writeAheadLogDisable()
                        .cacheSize(10) //use small cache size, to simulate much larger store with relatively small cache.
                        .make();


                Map<Long,String> map = db.createTreeMap("test",nodeSize,false,  valueOutsideOfNodes, null, null, null );

                long startTime = System.currentTimeMillis();

                for(int i=0;i<1e6;i++){
                    long key = r.nextLong();
                    String value =  useSmallValues?
                            //small value
                            "abc"+key:
                            //large value
                            "qwdkqwdoqpwfwe-09fewkljklcejewfcklajewjkleawckjlaweklcwelkcwecklwecjwekecklwecklaa"
                                    +"kvlskldvklsdklcklsdvkdflvvvvvvvvvvvvvvvvvvvvvvvsl;kzlkvlksdlkvklsdklvkldsklk"
                                    +key;
                    map.put(key, value);
                }

                System.out.print("    ");
                System.out.print((System.currentTimeMillis()-startTime)/1000+" s");
                System.out.print("   |");
                db.close();
            }
            System.out.println("");
        }
    }


}
