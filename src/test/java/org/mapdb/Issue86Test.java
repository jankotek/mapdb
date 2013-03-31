package org.mapdb;

import org.junit.Test;

import java.io.Serializable;
import java.util.Map;

/**
 *
 * @author M.Y. Developers
 */
public class Issue86Test {
    public static DB createFileStore() {
        return DBMaker
                .newTempFileDB()
                .writeAheadLogDisable()
                .make();
    }

    @Test
    public void Array() {
        DB createFileStore = createFileStore();
        Map map = createFileStore.getTreeMap("testMap");
        int maxSize = 1000;
        for (int i = 1; i < maxSize; i++) {
            String[] array = new String[i];
            for (int j = 0; j < i; j++) {
                array[j] = Utils.randomString(100);
            }
            map.put(i, array);
        }
    }

    @Test
    public void FieldArray() {
        DB createFileStore = createFileStore();
        Map map = createFileStore.getTreeMap("testMap");
        int maxSize = 1000;
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
                container[i] = Utils.randomString(100);
            }
        }
    }
}
