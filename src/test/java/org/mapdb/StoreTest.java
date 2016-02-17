package org.mapdb;

import org.junit.Test;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

import static org.junit.Assert.*;

public class StoreTest {

    @Test public void compression(){
        Store s = (Store)DBMaker.memoryDB()
                .transactionDisable()
                .compressionEnable()
                .makeEngine();

        long size = s.getCurrSize();
        long recid = s.put(new byte[10000],Serializer.BYTE_ARRAY);
        assertTrue(s.getCurrSize() - size < 200);
        assertTrue(Serializer.BYTE_ARRAY.equals(new byte[10000], s.get(recid, Serializer.BYTE_ARRAY)));
    }


    @Test public void compression_random(){
        Random r = new Random();

        for(int i=100;i<100000;i=i*2){
        Store s = (Store)DBMaker.memoryDB()
                .transactionDisable()
                .compressionEnable()
                .makeEngine();

            long size = s.getCurrSize();
            byte[] b = new byte[i];
            r.nextBytes(b);
            //grow so there is something to compress
            b = Arrays.copyOfRange(b,0,i);
            b = Arrays.copyOf(b,i*5);
            long recid = s.put(b,Serializer.BYTE_ARRAY);
            assertTrue(s.getCurrSize() - size < i*2+100);
            assertTrue(Serializer.BYTE_ARRAY.equals(b, s.get(recid, Serializer.BYTE_ARRAY)));
        }
    }

    static final Serializer<byte[]> untrusted = new Serializer<byte[]>(){

        @Override
        public void serialize(DataOutput out, byte[] value) throws IOException {
            out.write(value);
        }

        @Override
        public byte[] deserialize(DataInput in, int available) throws IOException {
            byte[] ret = new byte[available+1];
            in.readFully(ret);
            return ret;
        }
    };

    @Test(expected = ArrayIndexOutOfBoundsException.class)
    public void untrusted_serializer_beyond(){
        Store s = (Store)DBMaker.memoryDirectDB()
                .transactionDisable()
                .makeEngine();
        long recid = s.put(new byte[1000], untrusted);
        s.get(recid,untrusted);
    }
    
    @Test
    public void testSerializeNull(){
    	Store store = (Store)DBMaker.memoryDirectDB()
    			.transactionDisable()
    			.makeEngine();	
    	assertNull(store.serialize(null, untrusted));
    }

    @Test
    public void testSerializeEmptyBytes(){
    	Store store = (Store)DBMaker.memoryDirectDB()
    			.transactionDisable()
    			.makeEngine();	
    	// Test if serializer returns the next power of 2 bytes when any number of empty 
    	// bytes are serialized
    	for (int size=1; size<=100000; size++) {
    		DataIO.DataOutputByteArray serialized = store.serialize(new byte[size], untrusted);
    		int nextPowerOfTwo = Math.max(128, (int)Math.pow(2, Math.ceil(Math.log(size) / Math.log(2))));
    		byte expected[] = new byte[nextPowerOfTwo];
    		assertTrue("Size mismatch: expected "+nextPowerOfTwo+", actual "+serialized.buf.length,
    				Arrays.equals(expected, serialized.buf));
    	}
    }

    @Test
    public void testSerializePadding(){
    	Store store = (Store)DBMaker.memoryDirectDB()
    			.transactionDisable()
    			.makeEngine();	
    	// Test that passing in a byte[] of size < 128 just pads trailing 0 bytes & returns 128 bytes
    	byte mydata[] = new byte[] {1, 2, 3, 4, 5};
    	DataIO.DataOutputByteArray serialized = store.serialize(mydata, untrusted);
    	byte expected[] = new byte[128];
    	for (int i=0; i<mydata.length; i++) {
    		expected[i] = mydata[i];
    	}
    	assertTrue("Expected "+Arrays.toString(expected)+", actual "+serialized.buf,
    			Arrays.equals(expected, serialized.buf));
    }

    @Test
    public void testSerialize256Bytes(){
    	Store store = (Store)DBMaker.memoryDirectDB()
    			.transactionDisable()
    			.makeEngine();
    	// Test that 256 bytes of data is serialized into a 256 length byte array.
    	byte mydata[] = new byte[256];
    	for (int i=0; i<256; i++) {
    		mydata[i] = (byte)i;
    	}
    	DataIO.DataOutputByteArray serialized = store.serialize(mydata, untrusted);
    	byte expected[] = new byte[256];
    	for (int i=0; i<mydata.length; i++) {
    		expected[i] = mydata[i];
    	}
    	assertTrue("Expected "+Arrays.toString(expected)+", actual "+serialized.buf,
    			Arrays.equals(expected, serialized.buf));
    }

    @Test
    public void testCompareAndSwapMemoryDirectDB(){
    	Store store = (Store)DBMaker.memoryDirectDB()
    			.transactionDisable()
    			.makeEngine();
    	testCompareAndSwap(store);
    }

    @Test
    public void testCompareAndSwapHeapDB(){
    	Store store = (Store)DBMaker.heapDB()
    			.transactionDisable()
    			.makeEngine();
    	testCompareAndSwap(store);
    }

    @Test
    public void testCompareAndSwapAppendFileDB(){
    	File mapdbFile = TT.tempDbFile();
    	Store store = (Store)DBMaker.appendFileDB(mapdbFile)
    			.transactionDisable()
    			.makeEngine();
    	testCompareAndSwap(store);
    }

    @Test
    public void testCompareAndSwapFileDB(){
    	File mapdbFile = TT.tempDbFile();
    	Store store = (Store)DBMaker.fileDB(mapdbFile)
    			.transactionDisable()
    			.makeEngine();
    	testCompareAndSwap(store);
    }
    
    @Test
    public void testCompareAndSwapMemoryDB(){
    	Store store = (Store)DBMaker.memoryDB()
    			.transactionDisable()
    			.makeEngine();
    	testCompareAndSwap(store);
    }
    
    @Test
    public void testCompareAndSwapMemoryUnsafeDB(){
    	Store store = (Store)DBMaker.memoryUnsafeDB()
    			.transactionDisable()
    			.makeEngine();
    	testCompareAndSwap(store);
    }
    
    private void testCompareAndSwap(Store store) {
    	// Compare and swap of strings
    	long recordId = store.put("mapdb", Serializer.STRING);
    	assertFalse("compareAndSet worked even with a wrong expectedOldValue",
    			store.compareAndSwap(recordId, "hello", "world", Serializer.STRING));
    	assertTrue("compareAndSet didn't work with the correct expectedOldValue",
    			store.compareAndSwap(recordId, "mapdb", "world", Serializer.STRING));
    	assertEquals("compareAndSet didn't work, since final value doesn't match",
    			"world", store.get(recordId, Serializer.STRING));

    	// Compare and swap of integers
    	Random random = new Random(System.currentTimeMillis());
    	int number = random.nextInt();
    	int anotherNumber = number+1;
    	recordId = store.put(number, Serializer.INTEGER);
    	assertFalse("compareAndSet worked even with a wrong expectedOldValue",
    			store.compareAndSwap(recordId, anotherNumber, 800, Serializer.INTEGER));
    	assertTrue("compareAndSet didn't work with the correct expectedOldValue",
    			store.compareAndSwap(recordId, number, 800, Serializer.INTEGER));
    	assertEquals("compareAndSet didn't work, since final value doesn't match",
    			(Integer)800, (Integer)store.get(recordId, Serializer.INTEGER));
    }

    @Test public void testUpdateHeapDB(){
    	Store store = (Store)DBMaker.heapDB()
    			.transactionDisable()
    			.makeEngine();
    	testUpdate(store);
    }

    @Test public void testUpdateMemoryDirectDB(){
    	Store store = (Store)DBMaker.memoryDirectDB()
    			.transactionDisable()
    			.makeEngine();
    	testUpdate(store);
    }

    @Test public void testUpdateAppendFileDB(){
    	File mapdbFile = TT.tempDbFile();
    	Store store = (Store)DBMaker.appendFileDB(mapdbFile)
    			.transactionDisable()
    			.makeEngine();
    	testUpdate(store);
    }

    @Test public void testUpdateFileDB(){
    	File mapdbFile = TT.tempDbFile();
    	Store store = (Store)DBMaker.fileDB(mapdbFile)
    			.transactionDisable()
    			.makeEngine();
    	testUpdate(store);
    }

    @Test public void testUpdateMemoryDB(){
    	Store store = (Store)DBMaker.memoryDB()
    			.transactionDisable()
    			.makeEngine();
    	testUpdate(store);
    }

    @Test public void testUpdateMemoryUnsafeDB(){
    	Store store = (Store)DBMaker.memoryUnsafeDB()
    			.transactionDisable()
    			.makeEngine();
    	testUpdate(store);
    }

    private void testUpdate(Store store) {
    	Random random = new Random(System.currentTimeMillis());
    	for(int counter=0; counter<100; counter++) {
    		String initialString = TT.randomString(random.nextInt(10000));
    		String updatedString = TT.randomString(random.nextInt(10000));
    		long recordId = store.put(initialString, Serializer.STRING);
    		store.update(recordId, updatedString, Serializer.STRING);
    		assertEquals("Expected string didn't match updated string. Initial string was: "
    				+ initialString, updatedString, store.get(recordId, Serializer.STRING));
    	}
    }

    @Test public void testPreallocate() {
    	Store store = (Store)DBMaker.heapDB()
    			.transactionDisable()
    			.makeEngine();
    	long recordId = store.put("A", Serializer.STRING);
    	store.preallocate();
    	long recordId2 = store.put("B", Serializer.STRING);
    	assertEquals("Record ids do not match.", recordId+2, recordId2);
    }
}
