package org.mapdb;

import org.junit.Test;
import org.mapdb.*;

import java.util.Map;

import static org.junit.Assert.assertEquals;

public class Issue656Test {

    @Test
    public void main() {
        DB db = DBMaker.newTempFileDB().make();

        //Build a map with the counterEnable option
        Map<Object, Object> mCounterEnabled = db.createHashMap("mCounterEnabled")
                .counterEnable()
                .makeOrGet();
        mCounterEnabled.put(1, 1);
        assertEquals(1,mCounterEnabled.size());
        assertEquals(false, mCounterEnabled.isEmpty());

        //Build a map without the counterEnable option
        Map<Object, Object> mCounterDisabled = db.getHashMap("mCounterDisabled");
        mCounterDisabled.put(1, 1);

        assertEquals(1,mCounterDisabled.size());
        assertEquals(false, mCounterDisabled.isEmpty());
    }
}
