package org.mapdb;

import java.util.Map;

/**
 * Collection binding
 *
 * @author Jan Kotek
 */
public final class Bind {

    private Bind(){}

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

    public static <K,V,S> void secondaryMap(MapWithModificationListener<K,V> map,
                                            final Map<K,S> secondary,
                                            final Fun.Function2<S,K,V> fun){
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


}
