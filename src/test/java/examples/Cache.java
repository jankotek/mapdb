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

		primaryStore = DBMaker.newTempFileDB().writeAheadLogDisable()
				.deleteFilesAfterClose().closeOnJvmShutdown()
				.make().getTreeMap("cache");

		lruMap = new LongConcurrentLRUMap<Object>(size, (int) (size * 0.8)) {
			protected void evictedEntry(long key, Object value) {
				primaryStore.remove(key);
				// System.out.println("EVICTING: " + key);
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
		int cacheSize = 1024000;
		int putSize = cacheSize * 2;
		int getSize = putSize;
		int hit = 0, miss = 0;

		long start = System.nanoTime();

		Cache cache = new Cache(cacheSize);

		long init = System.nanoTime();

		for (long k = 0; k < putSize; ++k) {
			cache.set(k, "value:" + k);
		}

		long set = System.nanoTime();

		for (long k = 0; k < getSize; ++k) {
			String result = cache.get(k);
			if (result == null || !result.equals("value:" + k)) {
				// System.out.println("MISS:" + k);
				miss++;
			} else {
				// System.out.println("HIT:" + k);
				hit++;
			}
		}

		long end = System.nanoTime();

		System.out.printf("cache size: %d; set: %d; get: %d\n", cacheSize, putSize, getSize);
		System.out.printf("init time: %d\n", init - start);
		System.out.printf("hit: %d; miss: %d; time: %d ns(set) / %d ns(get)\n ", hit, miss, (set - init) / putSize,
				(end - set) / getSize);

	}

}
