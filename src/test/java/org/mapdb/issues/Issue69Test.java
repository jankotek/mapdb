package org.mapdb.issues;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.TT;

import java.util.Map;

import static org.junit.Assert.fail;

/*
 * https://github.com/jankotek/MapDB/issues/69
 *
 * @author Konstantin Zadorozhny
 *
 */
public class Issue69Test {

	private DB db;

	@Before
	public void setUp() {
		db = DBMaker.tempFileDB()
				.transactionDisable()
				.checksumEnable()
				.deleteFilesAfterClose()
				.make();
	}

	@After
	public void tearDown() throws InterruptedException {
		db.close();
	}

	@Test
	public void testStackOverflowError() throws Exception {

        try{
		Map<String, String> map = db.hashMap("test");

		StringBuilder buff = new StringBuilder();

		long maxIterations = 1000000* TT.scale();
		int valueLength = 1024;
		long maxKeys = 1000;
		long i = 1;
		while (i < maxIterations) {

			if (i % 10000 == 0) {
				valueLength ++;
//				System.out.println("Iteration: " + i + "; Value length: " + valueLength);
			}

			String key = "key" + (int)(Math.random() * maxKeys);
			buff.setLength(valueLength);
			map.put(key, buff.toString());

			i++;

		}
        }catch(Throwable e){
            while(e!=null){
                for(StackTraceElement ee: e.getStackTrace()){
                    System.out.println(ee);
                }
                System.out.println();
                e = e.getCause();
            }
            fail();
        }


	}


}
