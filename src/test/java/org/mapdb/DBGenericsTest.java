package org.mapdb;

import org.junit.Test;
import org.mapdb.tree.BTreeMap;
import org.mapdb.tree.HTreeMap;
import org.mapdb.tuple.Tuple2;
import org.mapdb.tuple.Tuple2Serializer;

public class DBGenericsTest {


    DB db = DBMaker.memoryDB().make();


    @Test public void treemap_0(){
        DBConcurrentNavigableMap m;
        m = db.treeMap("a").maxNodeSize(11).create();
        m = db.treeMap("a").maxNodeSize(11).createOrOpen();
        m = db.treeMap("a").maxNodeSize(11).open();
    }


    @Test public void treemap_1(){
        DBConcurrentNavigableMap<Long,String> m;
        m = db.treeMap("a", Long.class, String.class).maxNodeSize(11).create();
        m = db.treeMap("a", Long.class, String.class).maxNodeSize(11).createOrOpen();
        m = db.treeMap("a", Long.class, String.class).maxNodeSize(11).open();
    }


    @Test public void treemap_2(){
        DBConcurrentNavigableMap<Long,String> m;
        m = db.treeMap("a", Serializer.LONG, Serializer.STRING).maxNodeSize(11).create();
        m = db.treeMap("a", Serializer.LONG, Serializer.STRING).maxNodeSize(11).createOrOpen();
        m = db.treeMap("a", Serializer.LONG, Serializer.STRING).maxNodeSize(11).open();
    }

    @Test public void treemap_3(){
        DBConcurrentNavigableMap<Long,String> m;
        m = db.treeMap("a").keySerializer(Serializer.LONG).valueSerializer(Serializer.STRING).maxNodeSize(11).create();
        m = db.treeMap("a").keySerializer(Serializer.LONG).valueSerializer(Serializer.STRING).maxNodeSize(11).createOrOpen();
        m = db.treeMap("a").keySerializer(Serializer.LONG).valueSerializer(Serializer.STRING).maxNodeSize(11).open();
    }


    @Test public void treemap_4(){
        DBConcurrentNavigableMap<Tuple2<String,Long>,String> m;
        m = db.treeMap("a", new Tuple2Serializer(db.getDefaultSerializer(), Serializer.LONG), Serializer.STRING).maxNodeSize(11).create();
        m = db.treeMap("a", new Tuple2Serializer(db.getDefaultSerializer(), Serializer.LONG), Serializer.STRING).maxNodeSize(11).createOrOpen();
        m = db.treeMap("a", new Tuple2Serializer(db.getDefaultSerializer(), Serializer.LONG), Serializer.STRING).maxNodeSize(11).open();
    }

///////////////////////
    
    
    @Test public void hashmap_0(){
        DBConcurrentMap m;
        m = db.hashMap("a").valueInline().create();
        m = db.hashMap("a").valueInline().createOrOpen();
        m = db.hashMap("a").valueInline().open();
    }


    @Test public void hashmap_1(){
        DBConcurrentMap<Long,String> m;
        m = db.hashMap("a", Long.class, String.class).valueInline().create();
        m = db.hashMap("a", Long.class, String.class).valueInline().createOrOpen();
        m = db.hashMap("a", Long.class, String.class).valueInline().open();
    }


    @Test public void hashmap_2(){
        DBConcurrentMap<Long,String> m;
        m = db.hashMap("a", Serializer.LONG, Serializer.STRING).valueInline().create();
        m = db.hashMap("a", Serializer.LONG, Serializer.STRING).valueInline().createOrOpen();
        m = db.hashMap("a", Serializer.LONG, Serializer.STRING).valueInline().open();
    }

    @Test public void hashmap_3(){
        DBConcurrentMap<Long,String> m;
        m = db.hashMap("a").keySerializer(Serializer.LONG).valueSerializer(Serializer.STRING).valueInline().create();
        m = db.hashMap("a").keySerializer(Serializer.LONG).valueSerializer(Serializer.STRING).valueInline().createOrOpen();
        m = db.hashMap("a").keySerializer(Serializer.LONG).valueSerializer(Serializer.STRING).valueInline().open();
    }
}
