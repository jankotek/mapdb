package org.mapdb;

import java.io.DataInput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

import static org.mapdb.BTreeKeySerializer.BASIC;
import static org.mapdb.BTreeMap.COMPARABLE_COMPARATOR;
import static org.mapdb.BTreeMap.createRootRef;

public class BTreeMapContainsKeyTest extends JSR166TestCase {

    public static class OutsideNot extends BTreeMapContainsKeyTest{
        {
            valsOutsideNodes=false;
        }
    }

	boolean valsOutsideNodes = true;
	Engine r;
	RecordingSerializer valueSerializer = new RecordingSerializer();

    Map<Integer, String> map;


    @Override
    protected void setUp() throws Exception {
        r = new StoreDirect(Volume.memoryFactory(false, 0L,CC.VOLUME_SLICE_SHIFT));
        map = new BTreeMap(r, createRootRef(r,BASIC, Serializer.BASIC, COMPARABLE_COMPARATOR,0),
                6, valsOutsideNodes, 0, BASIC, valueSerializer, COMPARABLE_COMPARATOR, 0,false);
    }

    /**
     * When valsOutsideNodes is true should not deserialize value during .containsKey
     */
    public void testContainsKeySkipsValueDeserialisation() {

    	map.put(1, "abc");

    	boolean contains = map.containsKey(1);

		assertEquals(true, contains );
    	assertEquals("Deserialize was called", !valsOutsideNodes, valueSerializer.isDeserializeCalled() );
    }

    static class RecordingSerializer extends SerializerBase implements Serializable {

		private static final long serialVersionUID = 1L;
		private boolean deserializeCalled = false;

		@Override
    	public Object deserialize(DataInput is, int capacity) throws IOException {
			deserializeCalled = true;
    		return super.deserialize(is, capacity);
    	}

		public boolean isDeserializeCalled() {
			return deserializeCalled;
		}
    }
}
