package org.mapdb.issues;

import org.junit.Test;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.TT;

import java.io.Serializable;
import java.util.Map;

/*
 *
 * @author M.Y. Developers
 */
public class Issue86Test {
    public static DB createFileStore() {
        return DBMaker
                .tempFileDB()
                .transactionDisable()
                .make();
    }

    @Test
    public void Array() {
        DB createFileStore = createFileStore();
        Map map = createFileStore.treeMap("testMap");
        int maxSize = 1000* TT.scale();
        for (int i = 1; i < maxSize; i++) {
            String[] array = new String[i];
            for (int j = 0; j < i; j++) {
                array[j] = TT.randomString(100);
            }
            map.put(i, array);
        }
    }

    @Test
    public void FieldArray() {
        DB createFileStore = createFileStore();
        Map map = createFileStore.treeMap("testMap");
        int maxSize =  1000* TT.scale();
        for (int i = 1; i < maxSize; i++) {
            map.put(i, new StringContainer(i));
        }
    }

    private static class StringContainer implements Serializable {

        public String[] container;

        public StringContainer() {
        }

        public String[] getContainer() {
            return container;
        }

        public void setContainer(String[] container) {
            this.container = container;
        }

        public StringContainer(int size) {
            container = new String[size];
            for (int i = 0; i < size; i++) {
                container[i] = TT.randomString(100);
            }
        }
    }
}
