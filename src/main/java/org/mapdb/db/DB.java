package org.mapdb.db;

import org.jetbrains.annotations.NotNull;
import org.mapdb.store.HeapBufStore;
import org.mapdb.store.Store;

import java.io.File;

public class DB {
    private final Store store;

    public DB(Store store) {
        this.store = store;
    }

    public Store getStore(){
        return store;
    }

    public void close() {

    }

    public static class Maker {

        private final Store store;

        private Maker(Store store) {
            this.store = store;
        }

        @NotNull
        public static Maker appendFile(@NotNull File f) {
            return null;
        }

        @NotNull
        public DB make() {
            return new DB(store);
        }

        @NotNull
        public static Maker heapSer() {
            return new Maker(new HeapBufStore());
        }

        @NotNull
        public static Maker memoryDB() {
            return new Maker(new HeapBufStore());
        }
    }
}
