package org.mapdb;
import java.util.Iterator;

import org.junit.Test;

public class Issue452Test {
	
	@Test(expected = IllegalArgumentException.class)
	public void test_throws_exception_if_source_iterator_is_empty(){
		Iterator<Fun.Pair<String,String>> data = new Iterator<Fun.Pair<String,String>> () {
			@Override
			public boolean hasNext() {
				return false;
			}
	
			@Override
			public Fun.Pair<String,String> next() {
				return null;
			}
	
			@Override
			public void remove() {
				// TODO Auto-generated method stub
			}	
		};
		
		DB db = DBMaker.newTempFileDB()
				.make();
		
		db.createTreeMap("test")
			.pumpSource(data)
			.make();
	}

}
