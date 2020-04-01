package org.mapdb.list;

import harmony.ArrayListTest;
import org.mapdb.ser.Serializers;
import org.mapdb.store.HeapBufStore;
import org.mapdb.store.Store;

import java.util.List;


//TODO this test is slow, move to longtest
public class MonolithListHarmonyTest extends ArrayListTest {

    @Override
    public <E> List<E> newList() {
        Store store = new HeapBufStore();
        return (List<E>) MonolithList.Maker
                .newList(store, Serializers.JAVA)
                .make();
    }

}
