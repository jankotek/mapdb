package org.mapdb;

import org.junit.Test;
import org.mapdb.serializer.Serializers;
import org.mapdb.store.StoreTrivial;

/**
 * Tests jave interoperability
 */
public class UseFromJava {
    @Test
    public void basic_store() {
        StoreTrivial st = new StoreTrivial();
        st.put(1L, Serializers.LONG);
        st.close();
    }
}