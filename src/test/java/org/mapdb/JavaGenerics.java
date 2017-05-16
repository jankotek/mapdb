//package org.mapdb;
//
//import org.junit.Test;
//import org.mapdb.volume.FileChannelVol;
//import org.mapdb.volume.Volume;
//
//import java.util.Map;
//
//public class JavaGenerics {
//
//    DB db = DBMaker.heapDB().make();
//
//    @Test public void treemap(){
//        BTreeMap<String,Integer> m = db.treeMap("map",String.class, Integer.class).createOrOpen();
//
//        for(String key:m.keySet()){
//        }
//
//
//        for(String key:m.navigableKeySet()){
//        }
//
//        for(Integer v:m.values()){
//        }
//
//        for(Map.Entry<String,Integer> e:m.entrySet()){
//
//        }
//    }
//
//
//    @Test public void hashmap(){
//        HTreeMap<String,Integer> m = db.hashMap("map",String.class, Integer.class).createOrOpen();
//
//        for(String key:m.keySet()){
//        }
//
//        for(Integer v:m.values()){
//        }
//
//        for(Map.Entry<String,Integer> e:m.entrySet()){
//
//        }
//    }
//
//
//    @Test public void sortedtablemap(){
//        //create memory mapped volume
//        String file = TT.tempFile().getPath();
//        Volume volume = FileChannelVol.FACTORY.makeVolume(file, false);
//
//        SortedTableMap.Sink<Integer,String> sink =
//                SortedTableMap.create(
//                        volume,
//                        Serializer.INTEGER,
//                        Serializer.STRING
//                ).createFromSink();
//
//        SortedTableMap<Integer, String> m = sink.create();
//
//        for(String key:m.keySet()){
//        }
//
//        for(Integer v:m.values()){
//        }
//
//        for(Map.Entry<String,Integer> e:m.entrySet()){
//
//        }
//    }
//
//}
