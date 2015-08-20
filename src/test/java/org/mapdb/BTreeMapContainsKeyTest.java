package org.mapdb;

import org.junit.After;

import java.io.DataInput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

import static org.mapdb.BTreeKeySerializer.BASIC;
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
        r = DBMaker.memoryDB().transactionDisable().makeEngine();
        map = new BTreeMap(
                r,false,
                createRootRef(r,BASIC, Serializer.BASIC,valsOutsideNodes, 0),
                6, valsOutsideNodes, 0, BASIC, valueSerializer, 0);
    }


    @After
    public void close(){
        r.close();
    }

    /*
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
