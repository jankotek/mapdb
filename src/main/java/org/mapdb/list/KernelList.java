package org.mapdb.list;

import org.mapdb.ser.Serializer;
import org.mapdb.ser.Serializers;
import org.mapdb.store.Store;

import java.util.*;

public class KernelList<E>
        extends AbstractList<E>
        implements List<E>, RandomAccess {

    private static final Serializer<long[]> KERNEL_SER = Serializers.LONG_ARRAY;

    private final Store kernelStore;
    private final Store entryStore;
    private final long recid;
    private final Serializer<E> ser;

    public KernelList(Store kernelStore, Store entryStore, long recid, Serializer<E> ser) {
        this.kernelStore = kernelStore;
        this.entryStore = entryStore;
        this.recid = recid;
        this.ser = ser;
    }


    public static class Maker<E> {
        private final Store store;

        private Store entryStore = null;
        private final Serializer<E> ser;
        private final long recid;

        public Maker(Store store, long recid, Serializer<E> ser) {
            this.store = store;
            this.entryStore = store;
            this.recid = recid;
            this.ser = ser;
        }

        public static <E> KernelList.Maker<E> newList(Store store, Serializer<E> ser) {
            long recid = store.put(new long[]{}, KERNEL_SER);
            return new KernelList.Maker(store, recid, ser);
        }

        public Maker<E> entryStore(Store store){
            this.entryStore = store;
            return this;
        }

        public KernelList<E> make(){
            return new KernelList<E>(store, entryStore, recid, ser);
        }
    }


    @Override
    public E get(int index) {
        long entryRecid = getKernel()[index];
        return entryStore.get(entryRecid, ser);
    }

    @Override
    public int size() {
        return getKernel().length;
    }

    @Override
    public boolean add(E e) {
        long[] kernel = getKernel();
        kernel = Arrays.copyOf(kernel, kernel.length+1);
        long newRecid = entryStore.put(e, ser);
        kernel[kernel.length-1] = newRecid;
        storeKernel(kernel);
        return true;
    }

    @Override
    public void add(int index, E e) {
        long[] kernel = getKernel();
        if(index<0 || index>kernel.length)
            throw new IndexOutOfBoundsException();

        long[] kernel2 = Arrays.copyOf(kernel, kernel.length+1);
        System.arraycopy(kernel, index, kernel2, index+1, kernel.length-index);

        long newRecid = entryStore.put(e, ser);
        kernel2[index] = newRecid;
        storeKernel(kernel2);
    }

    @Override
    public boolean addAll(Collection<? extends E> c) {
        long[] kernel = getKernel();
        int pos = kernel.length;
        kernel = Arrays.copyOf(kernel, kernel.length+c.size());
        for(E e:c){
            long newRecid = entryStore.put(e, ser);
            kernel[pos++] = newRecid;
        }
        if(pos!=kernel.length)
            throw new ConcurrentModificationException();
        storeKernel(kernel);
        return true;
    }

    @Override
    public boolean addAll(int index, Collection<? extends E> c) {
        long[] kernel = getKernel();
        if(index<0 || index>kernel.length)
            throw new IndexOutOfBoundsException();
        int csize = c.size();
        long[] kernel2 = Arrays.copyOf(kernel, kernel.length+csize);
        System.arraycopy(kernel, index, kernel2, index+csize, kernel.length-index);
        kernel=null; //release memory for GC
        int pos = index;
        for(E e:c){
            long newRecid = entryStore.put(e, ser);
            kernel2[pos++] = newRecid;
        }
        if(pos!=csize+index)
            throw new ConcurrentModificationException();
        storeKernel(kernel2);
        return true;
    }

    @Override
    public E remove(int index) {
        long[] kernel = getKernel();
        long eRecid = kernel[index];
        E ret = entryStore.getAndDelete(eRecid, ser);
        long[] kernel2 = Arrays.copyOf(kernel, kernel.length-1);
        System.arraycopy(kernel, index+1, kernel2, index, kernel2.length-index);
        storeKernel(kernel2);
        return ret;
    }

    @Override
    public E set(int index, E element) {
        long[] kernel = getKernel();
        return entryStore.getAndUpdate(kernel[index], ser, element);
    }

    protected void storeKernel(long[] kernel) {
        for(long recid:kernel) {
            if (recid <= 0)
                throw new ConcurrentModificationException();
        }
        kernelStore.update(recid, KERNEL_SER, kernel);
    }

    protected long[] getKernel(){
        return kernelStore.get(recid, KERNEL_SER);
    }

}
