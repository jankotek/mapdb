package examples;

import org.mapdb.BTreeMap;
import org.mapdb.Bind;
import org.mapdb.DBMaker;
import org.mapdb.LongConcurrentLRUMap;
import org.mapdb.LongMap;

public class Cache {

	static int defaultCacheSize = 1024;
	int size;
	BTreeMap<Long, String> primaryStore;
	LongMap<Object> lruMap;

	static Object vv = new Object();

	public Cache() {
		this(defaultCacheSize);
	}

	public Cache(int size) {
		this.size = size;

		primaryStore = DBMaker.newTempTreeMap();
		lruMap = new LongConcurrentLRUMap<Object>(size, (int) (size * 0.8)) {
			protected void evictedEntry(long key, Object value) {
				primaryStore.remove(key);
				System.out.println("EVICTING: " + key);
			}
		};

		// hook lruMap as a listener
		primaryStore.addModificationListener(new Bind.MapListener<Long, String>() {
			@Override
			public void update(Long key, String oldVal, String newVal) {
				if (newVal == null) {
					// removal
					lruMap.remove(key);
				} else {
					lruMap.put(key, vv);
				}
			}
		});
	}

	public String set(Long key, String value) {
		return primaryStore.put(key, value);
	}

	public String get(Long key) {
		return primaryStore.get(key);
	}

	public static void main(String[] args) {
		Cache cache = new Cache();
		for (long k = 0; k < defaultCacheSize * 2; ++k) {
			cache.set(k, "value:" + k);
			assert cache.get(1L).equals("value:" + 1);
		}

		for (long k = 1; k < defaultCacheSize + 1; ++k) {
			String result = cache.get(k);
			if (result == null || !result.equals("value:" + k)) {
				// System.out.println("MISS:" + k);
			} else {
				System.out.println("HIT:" + k);
			}
		}
	}

}
