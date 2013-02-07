package org.mapdb;

import java.util.Iterator;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

/**
 * Collection binding
 *
 * @author Jan Kotek
 */
public final class Bind {

    private Bind(){}

    public static <K2,K1> Iterable<K1> findSecondaryKeys(final NavigableSet<Fun.Tuple2<K2,K1>> secondaryKeys, final K2 secondaryKey) {
        return new Iterable<K1>(){
            @Override
            public Iterator<K1> iterator() {
                //use range query to get all values
                final Iterator<Fun.Tuple2<K2,K1>> iter =
                    ((NavigableSet)secondaryKeys) //cast is workaround for generics
                        .subSet(
                                Fun.t2(secondaryKey,null), //NULL represents lower bound, everything is larger than null
                                Fun.t2(secondaryKey,Fun.HI) // HI is upper bound everything is smaller then HI
                        ).iterator();

                return new Iterator<K1>() {
                    @Override
                    public boolean hasNext() {
                        return iter.hasNext();
                    }

                    @Override
                    public K1 next() {
                        return iter.next().b;
                    }

                    @Override
                    public void remove() {
                        iter.remove();
                    }
                };
            }
        };

    }


    public interface MapListener<K,V>{
        void update(K key, V oldVal, V newVal);
    }

    public interface MapWithModificationListener<K,V> extends Map<K,V> {
        public void addModificationListener(MapListener<K,V> listener);
        public void removeModificationListener(MapListener<K,V> listener);
    }

    public static void size(MapWithModificationListener map, final Atomic.Long size){
        //set initial value first if necessary
        if(size.get() == 0 && map.isEmpty())
            size.set(map.size()); //TODO long overflow?

        map.addModificationListener(new MapListener() {
            @Override
            public void update(Object key, Object oldVal, Object newVal) {
                if(oldVal == null && newVal!=null)
                    size.incrementAndGet();
                else if(oldVal!=null && newVal == null)
                    size.decrementAndGet();
                else{
                    //update does not change collection size
                }
            }
        });
    }

    public static <K,V, V2> void secondaryValue(MapWithModificationListener<K, V> map,
                                              final Map<K, V2> secondary,
                                              final Fun.Function2<V2, K, V> fun){
        //fill if empty
        if(secondary.isEmpty()){
            for(Map.Entry<K,V> e:map.entrySet())
                secondary.put(e.getKey(), fun.run(e.getKey(),e.getValue()));
        }
        //hook listener
        map.addModificationListener(new MapListener<K, V>() {
            @Override
            public void update(K key, V oldVal, V newVal) {
                if(newVal == null){
                    //removal
                    secondary.remove(key);
                }else{
                    secondary.put(key, fun.run(key,newVal));
                }
            }
        });
    }

    public static <K,V, K2> void secondaryKey(MapWithModificationListener<K, V> map,
                                                final NavigableSet<Fun.Tuple2<K2, K>> secondary,
                                                final Fun.Function2<K2, K, V> fun){
        //fill if empty
        if(secondary.isEmpty()){
            for(Map.Entry<K,V> e:map.entrySet()){
                secondary.add(Fun.t2(fun.run(e.getKey(),e.getValue()), e.getKey()));
            }
        }
        //hook listener
        map.addModificationListener(new MapListener<K, V>() {
            @Override
            public void update(K key, V oldVal, V newVal) {
                if(newVal == null){
                    //removal
                    secondary.remove(Fun.t2(fun.run(key, oldVal), key));
                }else if(oldVal==null){
                    //insert
                    secondary.add(Fun.t2(fun.run(key,newVal), key));
                }else{
                    //update, must remove old key and insert new
                    K2 oldKey = fun.run(key, oldVal);
                    K2 newKey = fun.run(key, newVal);
                    if(oldKey == newKey || oldKey.equals(newKey)) return;
                    secondary.remove(Fun.t2(oldKey, key));
                    secondary.add(Fun.t2(newKey,key));
                }
            }
        });
    }

    public static <K,V> void mapInverse(MapWithModificationListener<K,V> primary,
                                        NavigableSet<Fun.Tuple2<V, K>> inverse) {
        Bind.secondaryKey(primary,inverse, new Fun.Function2<V, K,V>(){
            @Override public V run(K key, V value) {
                return value;
            }
        });
    }


    public static <K,V,C> void histogram(MapWithModificationListener<K,V> primary, final ConcurrentMap<C,Long> histogram,
                                  final Fun.Function2<C, K, V> entryToCategory){

        MapListener<K,V> listener = new MapListener<K, V>() {
            @Override public void update(K key, V oldVal, V newVal) {
                if(newVal == null){
                    //removal
                    C category = entryToCategory.run(key,oldVal);
                    incrementHistogram(category, -1);
                }else if(oldVal==null){
                    //insert
                    C category = entryToCategory.run(key,newVal);
                    incrementHistogram(category, 1);
                }else{
                    //update, must remove old key and insert new
                    C oldCat = entryToCategory.run(key, oldVal);
                    C newCat = entryToCategory.run(key, newVal);
                    if(oldCat == newCat || oldCat.equals(newCat)) return;
                    incrementHistogram(oldCat,-1);
                    incrementHistogram(oldCat,1);
                }

            }

            /** atomically update counter in histogram*/
            private void incrementHistogram(C category, long i) {
                for(;;){
                    Long oldCount = histogram.get(category);
                    if(oldCount == null){
                        //insert new count
                        if(histogram.putIfAbsent(category,i) == null)
                            return;
                    }else{
                        //increase existing count
                        Long newCount = oldCount+i;
                        if(histogram.replace(category,oldCount, newCount))
                            return;
                    }
                }
            }
        };

        primary.addModificationListener(listener);
    }




}
