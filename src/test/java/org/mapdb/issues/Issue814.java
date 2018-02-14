package org.mapdb.issues;

import org.junit.Test;
import org.mapdb.*;
import org.mapdb.serializer.SerializerArrayDelta;
import org.mapdb.serializer.Serializers;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentNavigableMap;

public class Issue814 {

    @Test
    public void issue814(){
        DB db = DBMaker.memoryDB().make();
        DBConcurrentNavigableMap<String[], String> treeMap = db.treeMap("preingest")
                .keySerializer(new SerializerArrayDelta<>(Serializers.STRING))
                .valueSerializer(Serializers.STRING).createOrOpen();
        //When getting a submap and getting the entrySet as follows:

        ConcurrentNavigableMap<String[], String> subMap = treeMap.
                subMap(new String[] { "key"}, new String[] { "key", null});
        Set<Map.Entry<String[],String>> entrySet = subMap.entrySet();
        for (Map.Entry<String[], String> entry : entrySet) {
            String[] keyParts = entry.getKey();
        }
    }


}
