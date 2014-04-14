package org.mapdb;

import org.junit.Test;

import java.util.Map;
import java.util.UUID;

@SuppressWarnings({"rawtypes","unchecked"})
public class Issue170Test {

    @Test
    public void test(){
        Map m = DBMaker.newMemoryDB()
                .compressionEnable()
                .transactionDisable()
                .make().createTreeMap("test").make();
        for(int i=0;i<1e5;i++){
            m.put(UUID.randomUUID().toString(),UUID.randomUUID().toString());
        }

    }
}
