package org.mapdb.list;

import org.mapdb.ser.ArrayListSerializer;
import org.mapdb.ser.Serializer;
import org.mapdb.store.Store;

import java.util.*;

public class MonolithList<E>
        extends AbstractList<E>
        implements List<E>, RandomAccess {

    public static class Maker<E> {
        private final Store store;
        private final Serializer<E> ser;
        private final long recid;

        public Maker(Store store, long recid, Serializer<E> ser) {
            this.store = store;
            this.recid = recid;
            this.ser = ser;
        }

        public static <E> Maker<E> newList(Store store, Serializer<E> ser) {
            long recid = store.put(new ArrayList(), new ArrayListSerializer(ser));
            return new Maker(store, recid, ser);
        }

        public MonolithList<E> make(){
            return new MonolithList<E>(store, recid, ser);
        }
    }


    private final Store store;
    private final long recid;
    private final Serializer<E> ser;
    private final Serializer<ArrayList<E>> listSer;

    public MonolithList(Store store, long recid, Serializer<E> ser) {
        this.store = store;
        this.recid = recid;
        this.ser = ser;
        this.listSer = new ArrayListSerializer(ser);
    }


    @Override
    public E get(int index) {
        ArrayList<E> list = store.get(recid, listSer);
        return list.get(index);
    }

    @Override
    public int size() {
        ArrayList<E> list = store.get(recid, listSer);
        return list.size();
    }

    @Override
    public boolean add(E e) {
        ArrayList<E> list = getClone();
        list.add(e);
        store(list);
        return true;
    }

    @Override
    public E set(int index, E element) {
        ArrayList<E> list = getClone();
        E e=list.set(index,element);
        store(list);
        return e;
    }

    @Override
    public void add(int index, E element) {
        ArrayList<E> list = getClone();
        list.add(index,element);
        store(list);
    }

    @Override
    public boolean addAll(int index, Collection<? extends E> c) {
        ArrayList<E> list = getClone();
        list.addAll(index, c);
        store(list);
        return true;
    }

    @Override
    public boolean addAll(Collection<? extends E> c) {
        ArrayList<E> list = getClone();
        list.addAll(c);
        store(list);
        return true;
    }

    @Override
    public E remove(int index) {
        ArrayList<E> list = getClone();
        E e=list.remove(index);
        store(list);
        return e;
    }

    protected ArrayList<E> getClone() {
        return (ArrayList<E>) store.get(recid, listSer).clone();
    }

    protected void store(ArrayList<E> list) {
        store.update(recid, listSer, list);
    }

}
