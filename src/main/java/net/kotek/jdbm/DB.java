package net.kotek.jdbm;

import java.lang.ref.WeakReference;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentMap;

/**
 * A database with easy access to named maps and other collections.
 */
@SuppressWarnings("unchecked")
public class DB {

    protected RecordManager recman;
    protected Map<String, WeakReference<?>> collections = new HashMap<String, WeakReference<?>>();

    public DB(RecordManager recman){
        this.recman = recman;
    }

    /**
     * Opens existing or creates new Hash Tree Map.
     * This collection perform well under concurrent access.
     * Is best for large keys and large values.
     *
     * @param name of map
     * @param <K> key
     * @param <V> value
     * @return map
     */
    synchronized public <K,V> ConcurrentMap<K,V> getHashMap(String name){
        checkNotClosed();
        HTreeMap<K,V> ret = (HTreeMap<K, V>) getFromWeakCollection(name);
        if(ret!=null) return ret;
        Long recid = recman.getNamedRecid(name);
        if(recid!=null){
            //open existing map
            ret = new HTreeMap<K,V>(recman, recid);
            if(CC.ASSERT && !ret.hasValues) throw new ClassCastException("Collection is Set, not Map");
        }else{
            //create new map
            ret = new HTreeMap<K,V>(recman,true);
            recman.setNamedRecid(name, ret.rootRecid);
        }
        collections.put(name, new WeakReference<Object>(ret));
        return ret;
    }


    /**
     *  Opens existing or creates new Hash Tree Set.
     *
     * @param name of Set
     * @param <K> values in set
     * @return set
     */
    synchronized public <K> Set<K> getHashSet(String name){
        checkNotClosed();
        Set<K> ret = (Set<K>) getFromWeakCollection(name);
        if(ret!=null) return ret;
        Long recid = recman.getNamedRecid(name);
        if(recid!=null){
            //open existing map
            HTreeMap<K,Object> m = new HTreeMap<K,Object>(recman, recid);
            if(CC.ASSERT && m.hasValues) throw new ClassCastException("Collection is Map, not Set");
            ret = m.keySet();
        }else{
            //create new map
            HTreeMap<K,Object> m = new HTreeMap<K,Object>(recman, false);
            ret = m.keySet();
            recman.setNamedRecid(name, m.rootRecid);
        }
        collections.put(name, new WeakReference<Object>(ret));
        return ret;

    }

    /**
     * Opens existing or creates new B-linked-tree Map.
     * This collection performs well under concurrent access.
     * Only trade-off are deletes, which causes tree fragmentation.
     * It is ordered and best suited for small keys and values.
     *
     * @param name of map
     * @param <K> key
     * @param <V> value
     * @return map
     */
    synchronized public <K,V> ConcurrentSortedMap<K,V> getTreeMap(String name){
        checkNotClosed();
        BTreeMap<K,V> ret = (BTreeMap<K,V>) getFromWeakCollection(name);
        if(ret!=null) return ret;
        Long recid = recman.getNamedRecid(name);
        if(recid!=null){
            //open existing map
            ret = new BTreeMap<K,V>(recman, recid);
            if(CC.ASSERT && !ret.hasValues) throw new ClassCastException("Collection is Set, not Map");
        }else{
            //create new map
            ret = new BTreeMap<K,V>(recman,BTreeMap.DEFAULT_MAX_NODE_SIZE, true);
            recman.setNamedRecid(name, ret.treeRecid);
        }
        collections.put(name, new WeakReference<Object>(ret));
        return ret;
    }

    /**
     * Opens existing or creates new B-linked-tree Set.
     *
     * @param name of set
     * @param <K> values in set
     * @return set
     */
    synchronized public <K> SortedSet<K> getTreeSet(String name){
        checkNotClosed();
        SortedSet<K> ret = (SortedSet<K>) getFromWeakCollection(name);
        if(ret!=null) return ret;
        Long recid = recman.getNamedRecid(name);
        if(recid!=null){
            //open existing map
            BTreeMap<K,Object> m = new BTreeMap<K,Object>(recman,  recid);
            if(CC.ASSERT && m.hasValues) throw new ClassCastException("Collection is Map, not Set");
            ret = m.keySet();
        }else{
            //create new map
            BTreeMap<K,Object> m =  new BTreeMap<K,Object>(recman,BTreeMap.DEFAULT_MAX_NODE_SIZE,false);
            recman.setNamedRecid(name, m.treeRecid);
            ret = m.keySet();
        }

        collections.put(name, new WeakReference<Object>(ret));
        return ret;
    }

    /**
     * Closes database.
     * All other methods will throw 'IllegalAccessError' after this method was called.
     * <p/>
     * !! it is necessary to call this method before JVM exits!!
     */
    synchronized public void close(){
        recman.close();
        //dereference db to prevent memory leaks
        recman = null;
        collections = null;
    }

    /**
     * All collections are weakly referenced to prevent two instances of the same collection in memory.
     * This is mainly for locking, two instances of the same lock would not simply work.
     */
    protected Object getFromWeakCollection(String name){

        WeakReference<?> r = collections.get(name);
        if(r==null) return null;
        Object o = r.get();
        if(o==null) collections.remove(name);
        return o;
    }


    protected void checkNotClosed() {
        if(recman == null) throw new IllegalAccessError("DB was already closed");
    }

    public void commit() {
        checkNotClosed();
        recman.commit();
    }

    public void rollback() {
        checkNotClosed();
        recman.rollback();
    }

}
