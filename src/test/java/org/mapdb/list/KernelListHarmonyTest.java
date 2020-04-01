package org.mapdb.list;

import harmony.ArrayListTest;
import org.mapdb.ser.Serializers;
import org.mapdb.store.HeapBufStore;
import org.mapdb.store.Store;

import java.util.List;


public class KernelListHarmonyTest extends ArrayListTest {

    @Override
    public <E> List<E> newList() {
        Store store = new HeapBufStore();
        return (List<E>) KernelList.Maker
                .newList(store, Serializers.JAVA)
                .entryStore(new HeapBufStore())
                .make();
    }



}
