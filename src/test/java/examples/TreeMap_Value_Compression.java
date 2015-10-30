package examples;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;

import java.util.Map;
import java.util.zip.Deflater;

/*
 * Values in BTreeMap Leaf Nodes are serialized in two ways:
 *
 *  1) In separate record, in that case only small pointer is stored.
 *     This mode is activated with `valuesOutsideNodesEnable()` option
 *
 *  2) In Object[] as part of node.
 *
 *  Second mode is good for compression. Instead of compressing each value separately,
 *  Object[] can be compressed together. If values have many repeating values
 *  this leads to better compression ratio and faster compression.
 *
 *  This example shows how to compress values in BTreeMap
 *
 */
public class TreeMap_Value_Compression {

    public static void main(String[] args) {
        DB db = DBMaker.memoryDB().make(); //any DB config will do

        /*
         * Create BTreeMap with maximal node size 64,
         * where values are byte[] and are compressed together with LZV compression.
         * This type of compression is very good for text.
         */
        Map<Long,byte[]> map = db.treeMapCreate("map")
                .keySerializer(Serializer.LONG) //not relevant here, but good practice to set key serializer

                // set maximal node size. Larger size means better compression,
                // but slower read/writes. Default value is 32
                .nodeSize(64)

                        //value serializer is used to convert values in binary form
                .valueSerializer(
                        //this bit creates byte[] serializer with LZV compression
                        new Serializer.CompressionWrapper<byte[]>( //apply compression wrapper
                                Serializer.BYTE_ARRAY  //and serializer used on data,
                        )
                )
                .makeOrGet(); // apply configuration and create map


        /*
         * Another option for Value Serializer is to use Deflate compression instead of LZV.
         * It is slower, but provides better compression ratio.
         */
        new Serializer.CompressionDeflateWrapper<byte[]>(
                Serializer.BYTE_ARRAY
        );

        /*
         * Deflate compression also supports Shared Dictionary.
         * That works great for XML messages and other small texts with many repeated strings.
         */
        new Serializer.CompressionDeflateWrapper<byte[]>(
                Serializer.BYTE_ARRAY,
                Deflater.BEST_COMPRESSION, //set maximal compression
                new byte[]{'m','a','p','d','b'} // set Shared Dictionary
        );

        /*
         * Shared Dictionary can be upto 32KB in size. It should contain repeated values from text.
         * More about its advantages can be found here:
         *   https://blog.cloudflare.com/improving-compression-with-preset-deflate-dictionary/
         *
         * We will integrate Dictionary trainer into MapDB (and Data Pump) in near future.
         * For now there 3td party is utility written in Go which creates this Dictionary from files:
         *
         *  https://github.com/vkrasnov/dictator
         *
         * To use it:
         *   1) download dictator.go into your computer
         *
         *   2) install `gccgo` package
         *
         *   3) run it. First parameter is dict size (max 32K), second is folder with training text,
         *      third is file where dictionary is saved:
         *      go run dictator.go 32000 /some/path/with/text /save/dictionary/here
         *
         *   4) Copy dictionary content and use it with CompressionDeflateWrapper
         */

    }
}
