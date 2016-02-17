package org.mapdb;

import org.junit.Test;

import java.io.*;
import java.util.*;

import static org.junit.Assert.*;
import org.mapdb.issues.Issue332Test.TestSerializer;

@SuppressWarnings({"rawtypes","unchecked"})
public class SerializerTest {

    @Test public void UUID2(){
        UUID u = UUID.randomUUID();
        assertEquals(u, SerializerBaseTest.clone2(u,Serializer.UUID));
    }

    @Test public void string_ascii(){
        String s = "adas9 asd9009asd";
        assertEquals(s, SerializerBaseTest.clone2(s, Serializer.STRING_ASCII));
        s = "";
        assertEquals(s, SerializerBaseTest.clone2(s, Serializer.STRING_ASCII));
        s = "    ";
        assertEquals(s, SerializerBaseTest.clone2(s, Serializer.STRING_ASCII));
    }

    @Test public void compression_wrapper() throws IOException {
        byte[] b = new byte[100];
        new Random().nextBytes(b);
        Serializer<byte[]> ser = new Serializer.CompressionWrapper(Serializer.BYTE_ARRAY);
        assertTrue(Serializer.BYTE_ARRAY.equals(b, SerializerBaseTest.clone2(b, ser)));

        b = Arrays.copyOf(b, 10000);
        assertTrue(Serializer.BYTE_ARRAY.equals(b, SerializerBaseTest.clone2(b, ser)));

        DataIO.DataOutputByteArray out = new DataIO.DataOutputByteArray();
        ser.serialize(out, b);
        assertTrue(out.pos < 1000);
    }

    @Test public void java_serializer_issue536(){
        Long l = 1111L;
        assertEquals(l, SerializerBaseTest.clone2(l, Serializer.JAVA));
    }


    @Test public void java_serializer_issue536_with_engine(){
        DB db = DBMaker.memoryDB().transactionDisable().make();
        Long l = 1111L;
        long recid = db.engine.put(l,Serializer.JAVA);
        assertEquals(l, db.engine.get(recid, Serializer.JAVA));
    }


    @Test public void java_serializer_issue536_with_map() {
        DB db = DBMaker.memoryDB().transactionDisable().make();
        Map m = db.hashMapCreate("map")
                .keySerializer(Serializer.JAVA)
                .make();
        Long l = 1111L;
        m.put(l, l);
        assertEquals(l, m.get(l));
    }

    @Test public void array(){
        Serializer.Array s = new Serializer.Array(Serializer.INTEGER);

        Object[] a = new Object[]{1,2,3,4};

        assertTrue(Arrays.equals(a, (Object[]) TT.clone(a, s)));
        assertEquals(s, TT.clone(s, Serializer.BASIC));
    }

    void testLong(Serializer<Long> ser){
        for(Long i= (long) -1e5;i<1e5;i++){
            assertEquals(i, TT.clone(i, ser));
        }

        for(Long i=0L;i>0;i+=1+i/10000){
            assertEquals(i, TT.clone(i, ser));
            assertEquals(new Long(-i), TT.clone(-i, ser));
        }

        Random r = new Random();
        for(int i=0;i<1e6;i++){
            Long a = r.nextLong();
            assertEquals(a, TT.clone(a, ser));
        }

    }

    @Test public void Long(){
        testLong(Serializer.LONG);
    }


    @Test public void Long_packed(){
        testLong(Serializer.LONG_PACKED);
    }



    void testInt(Serializer<Integer> ser){
        for(Integer i= (int) -1e5;i<1e5;i++){
            assertEquals(i, TT.clone(i, ser));
        }

        for(Integer i=0;i>0;i+=1+i/10000){
            assertEquals(i, TT.clone(i, ser));
            assertEquals(new Long(-i), TT.clone(-i, ser));
        }

        Random r = new Random();
        for(int i=0;i<1e6;i++){
            Integer a = r.nextInt();
            assertEquals(a, TT.clone(a, ser));
        }
    }

    @Test public void Int(){
        testInt(Serializer.INTEGER);
    }


    @Test public void Int_packed(){
        testInt(Serializer.INTEGER_PACKED);
    }

    @Test public void deflate_wrapper(){
        Serializer.CompressionDeflateWrapper c =
                new Serializer.CompressionDeflateWrapper(Serializer.BYTE_ARRAY, -1,
                        new byte[]{1,1,1,1,1,1,1,1,1,1,1,23,4,5,6,7,8,9,65,2});

        byte[] b = new byte[]{1,1,1,1,1,1,1,1,1,1,1,1,4,5,6,3,3,3,3,35,6,67,7,3,43,34};

        assertTrue(Arrays.equals(b, TT.<byte[]>clone(b, c)));
    }

    @Test public void deflate_wrapper_values(){
        DB db = DBMaker.memoryDB().transactionDisable().make();
        Map m = db.treeMapCreate("a")
                .valueSerializer(new Serializer.CompressionDeflateWrapper(Serializer.LONG))
                .keySerializer(Serializer.LONG)
                .make();

        for(long i=0;i<1000;i++){
            m.put(i,i*10);
        }

        for(long i=0;i<1000;i++){
            assertEquals(i*10,m.get(i));
        }
    }


    @Test public void compress_wrapper_values(){
        DB db = DBMaker.memoryDB().transactionDisable().make();
        Map m = db.treeMapCreate("a")
                .valueSerializer(new Serializer.CompressionWrapper(Serializer.LONG))
                .keySerializer(Serializer.LONG)
                .make();

        for(long i=0;i<1000;i++){
            m.put(i,i*10);
        }

        for(long i=0;i<1000;i++){
            assertEquals(i * 10, m.get(i));
        }
    }


    static final class StringS implements Comparable<StringS>{
        final String s;

        StringS(String s) {
            this.s = s;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            StringS stringS = (StringS) o;

            return !(s != null ? !s.equals(stringS.s) : stringS.s != null);

        }

        @Override
        public int hashCode() {
            return s != null ? s.hashCode() : 0;
        }

        @Override
        public int compareTo(StringS o) {
            return s.compareTo(o.s);
        }
    }

    static final class StringSSerializer extends Serializer<StringS> implements Serializable {

		private static final long serialVersionUID = 4930213105522089451L;

		@Override
        public void serialize(DataOutput out, StringS value) throws IOException {
            out.writeUTF(value.s);
        }

        @Override
        public StringS deserialize(DataInput in, int available) throws IOException {
            return new StringS(in.readUTF());
        }
    }
    @Test public void issue546() throws IOException {
        File f = File.createTempFile("mapdbTest","mapdb");
        DB db = DBMaker
                .fileDB(f)
                .transactionDisable()
                .make();



        BTreeKeySerializer XYZ_SERIALIZER = new BTreeKeySerializer.ArrayKeySerializer(
                new Comparator[]{Fun.COMPARATOR,Fun.COMPARATOR},
                new Serializer[]{new StringSSerializer(), new StringSSerializer()}
        );

        NavigableSet<Object[]> multiMap = db.treeSetCreate("xyz")
                .serializer(XYZ_SERIALIZER)
                .make();

        multiMap.add(new Object[]{new StringS("str1"), new StringS("str2")});
        db.close();

        db = DBMaker
                .fileDB(f)
                .transactionDisable()
                .asyncWriteEnable()
                .make();


        multiMap = db.treeSetCreate("xyz")
                .serializer(XYZ_SERIALIZER)
                .makeOrGet();

        assertEquals(1, multiMap.size());
        assertTrue(multiMap.contains(new Object[]{new StringS("str1"), new StringS("str2")}));
        db.close();

    }

	@Test
	public void testLongUnpack() {
		final Serializer<java.lang.Long> serializer = Serializer.LONG;
		final int TEST_DATA_SIZE = 5;
		final long[] testData = new long[TEST_DATA_SIZE];

		for (int testDataIndex = 0; testDataIndex < TEST_DATA_SIZE; testDataIndex++) {
			testData[testDataIndex] = (long) (testDataIndex + 1);
		}

		for (int testDataIndex = 0; testDataIndex < TEST_DATA_SIZE; testDataIndex++) {
			assertEquals("The returned data for the indexed key for Serializer did not match the data for the key.", 
					(long)serializer.valueArrayGet(testData, testDataIndex), testData[testDataIndex]);
		}
	}


	@Test public void testCharSerializer() {
		for (char character = 0; character < Character.MAX_VALUE; character++) {
			assertEquals("Serialized and de-serialized characters do not match the original", (int) character,
					(int) TT.clone(character, Serializer.CHAR));
		}
	}

	@Test public void testStringXXHASHSerializer() {
		String randomString = UUID.randomUUID().toString();
		for (int executionCount = 0; executionCount < 100; randomString = UUID.randomUUID()
				.toString(), executionCount++) {
			assertEquals("Serialized and de-serialized Strings do not match the original", randomString,
					TT.clone(randomString, Serializer.STRING_XXHASH));
		}
	}
	

	@Test public void testStringInternSerializer() {
		String randomString = UUID.randomUUID().toString();
		for (int executionCount = 0; executionCount < 100; randomString = UUID.randomUUID()
				.toString(), executionCount++) {
			assertEquals("Serialized and de-serialized Strings do not match the original", randomString,
					TT.clone(randomString, Serializer.STRING_INTERN));
		}
	}
	
	@Test public void testBooleanSerializer() {
		assertTrue("When boolean value 'true' is serialized and de-serialized, it should still be true",
				TT.clone(true, Serializer.BOOLEAN));
		assertFalse("When boolean value 'false' is serialized and de-serialized, it should still be false",
				TT.clone(false, Serializer.BOOLEAN));
	}
	
	@Test public void testRecIDSerializer() {
		for (Long positiveLongValue = 0L; positiveLongValue > 0; positiveLongValue += 1 + positiveLongValue / 10000) {
			assertEquals("Serialized and de-serialized record ids do not match the original", positiveLongValue,
					TT.clone(positiveLongValue, Serializer.RECID));
		}
	}
    
	@Test public void testLongArraySerializer(){
		(new ArraySerializerTester<long[]>() {

			@Override
			void populateValue(long[] array, int index) {
				array[index] = random.nextLong();
			}

			@Override
			long[] instantiateArray(int size) {
				return new long[size];
			}

			@Override
			void verify(long[] array) {
				assertArrayEquals("Serialized and de-serialized long arrays do not match the original", array,
						TT.clone(array, Serializer.LONG_ARRAY));				
			}
		
		}).test();
	}
	
	@Test public void testCharArraySerializer(){
		(new ArraySerializerTester<char[]>() {

			@Override
			void populateValue(char[] array, int index) {
				array[index] = (char) (random.nextInt(26) + 'a');
			}

			@Override
			char[] instantiateArray(int size) {
				return new char[size];
			}

			@Override
			void verify(char[] array) {
				assertArrayEquals("Serialized and de-serialized char arrays do not match the original", array,
						TT.clone(array, Serializer.CHAR_ARRAY));
			}
		}).test();
	}
	
	@Test public void testIntArraySerializer(){
		(new ArraySerializerTester<int[]>() {

			@Override
			void populateValue(int[] array, int index) {
				array[index] = random.nextInt();
			}

			@Override
			int[] instantiateArray(int size) {
				return new int[size];
			}

			@Override
			void verify(int[] array) {
				assertArrayEquals("Serialized and de-serialized int arrays do not match the original", array,
						TT.clone(array, Serializer.INT_ARRAY));
			}
		}).test();
	}
	
	@Test public void testDoubleArraySerializer() {
		(new ArraySerializerTester<double[]>() {

			@Override
			void populateValue(double[] array, int index) {
				array[index] = random.nextDouble();
			}

			@Override
			double[] instantiateArray(int size) {
				return new double[size];
			}

			void verify(double[] array) {
				assertArrayEquals("Serialized and de-serialized double arrays do not match the original", array,
						TT.clone(array, Serializer.DOUBLE_ARRAY), 0);
			}
		}).test();
	}
	
	@Test public void testBooleanArraySerializer(){
		(new ArraySerializerTester<boolean[]>() {

			@Override
			void populateValue(boolean[] array, int index) {
				array[index] = random.nextBoolean();
			}

			@Override
			boolean[] instantiateArray(int size) {
				return new boolean[size];
			}

			@Override
			void verify(boolean[] array) {
				assertArrayEquals("Serialized and de-serialized boolean arrays do not match the original", array,
						TT.clone(array, Serializer.BOOLEAN_ARRAY));
			}
		}).test();
	}

	@Test public void testShortArraySerializer() {
		(new ArraySerializerTester<short[]>() {

			@Override
			void populateValue(short[] array, int index) {
				array[index] = (short) random.nextInt();
			}

			@Override
			short[] instantiateArray(int size) {
				return new short[size];
			}

			@Override
			void verify(short[] array) {
				assertArrayEquals("Serialized and de-serialized short arrays do not match the original", array,
						TT.clone(array, Serializer.SHORT_ARRAY));
			}
		}).test();
	}
	
	@Test public void testFloatArraySerializer() {
		(new ArraySerializerTester<float[]>() {

			@Override
			void populateValue(float[] array, int index) {
				array[index] = random.nextFloat();
			}

			@Override
			float[] instantiateArray(int size) {
				return new float[size];
			}

			@Override
			void verify(float[] array) {
				assertArrayEquals("Serialized and de-serialized float arrays do not match the original", array,
						TT.clone(array, Serializer.FLOAT_ARRAY), 0);
			}

		}).test();
	}

	private abstract class ArraySerializerTester<A> {
		Random random = new Random();
		abstract void populateValue(A array, int index);

		abstract A instantiateArray(int size);

		abstract void verify(A array);
		
		void test() {
			verify(getArray());
		}

		private A getArray() {
			int size = random.nextInt(100);
			A array = instantiateArray(size);
			for (int i = 0; i < size; i++) {
				populateValue(array, i);
			}
			return array;
		}
	}

    @Test public void testValueArrayDeleteValue_WhenArraySizeIsOne(){
		Object[] array = new Object[1];
		array[0] = new Object();
		Object[] result = (Object[]) new TestSerializer().valueArrayDeleteValue(array, 1);
		assertEquals("When the only element is deleted from array, it's length should be zero", 0, result.length);
	}
    
	@Test public void testValueArrayDeleteValue_WhenArraySizeIsTwo() {
		int arraySize = 2;
		Object[] array = new Object[arraySize];
		array[0] = new Object();
		array[1] = new Object();
		Object[] result = (Object[]) new TestSerializer().valueArrayDeleteValue(array, 1);
		assertEquals("When an element is deleted, the array size should be one less the original size", arraySize - 1,
				result.length);
		assertEquals("When first element is deleted from array, the second should become the first", array[1],
				result[0]);

		result = (Object[]) new TestSerializer().valueArrayDeleteValue(array, arraySize);
		assertEquals("When an element is deleted, the array size should be one less the original size", arraySize - 1,
				result.length);
		assertEquals("When last element is deleted from array, the one before last should become the first",
				array[arraySize - 2], result[result.length - 1]);
	}
	
	@Test public void testValueArrayDeleteValue_DeleteElementFromMiddleOfArray() {
		int arraySize = 10;
		Object[] array = new Object[arraySize];
		for (int i = 0; i < array.length; i++) {
			array[i] = new Object();
		}
		
		Object[] result = (Object[]) new TestSerializer().valueArrayDeleteValue(array, 5);
		assertEquals("Deleting element should not have an effect on the previous element", array[3], result[3]);
		assertEquals("When element is deleted, next element should take its place", array[5], result[4]);
		
		result = (Object[]) new TestSerializer().valueArrayDeleteValue(array, 1);
		assertEquals("When an element is deleted, the array size should be one less the original size", arraySize - 1,
				result.length);
		assertEquals("When first element is deleted from array, the second should become the first", array[1],
				result[0]);

		result = (Object[]) new TestSerializer().valueArrayDeleteValue(array, arraySize);
		assertEquals("When an element is deleted, the array size should be one less the original size", arraySize - 1,
				result.length);
		assertEquals("When last element is deleted from array, the one before last should become the first",
				array[arraySize - 2], result[result.length - 1]);
	}

	@Test public void testValueArrayUpdateValue() {
		int arraySize = 10;
		Object[] array = new Object[arraySize];
		for (int index = 0; index < array.length; index++) {
			array[index] = ""+index;
		}
		TestSerializer testSerializer = new TestSerializer();
		Object[] expectedArray = new Object[arraySize];
		for (int index = 0; index < expectedArray.length; index++) {
			expectedArray[index] = ""+(index + 1);
			array = (Object[]) testSerializer.valueArrayUpdateVal(array, index, (String) expectedArray[index]);
		}
		assertArrayEquals("Array should contain updated values after values are updated", expectedArray, array);
	}

}
