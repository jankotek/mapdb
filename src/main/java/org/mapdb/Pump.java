package org.mapdb;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Data Pump moves data from one source to other.
 * It can be used to import data from text file, or copy store from memory to disk.
 */
public class Pump {

    /** copies all data from first DB to second DB */
    public static void copy(DB db1, DB db2){
        copy(storeForDB(db1), storeForDB(db2));
        db2.engine.clearCache();
        db2.reinit();
    }

    /** copies all data from first store to second store */
    public static void copy(Store s1, Store s2){
        long maxRecid =s1.getMaxRecid();
        for(long recid=1;recid<=maxRecid;recid++){
            ByteBuffer bb = s1.getRaw(recid);
            //System.out.println(recid+" - "+(bb==null?0:bb.remaining()));
            s2.updateRaw(recid, bb);
        }

        //now release unused recids
        for(Iterator<Long> iter = s1.getFreeRecids(); iter.hasNext();){
            s2.delete(iter.next(), null);
        }
    }


    /** traverses {@link EngineWrapper}s and returns underlying {@link Store}*/
    public static Store storeForDB(DB db){
        Engine e = db.getEngine();
        while(e instanceof EngineWrapper) e = ((EngineWrapper) e).getWrappedEngine();
        return (Store) e;
    }

    /**
     * Sorts large data set by given `Comparator`. Data are sorted with in-memory cache and temporary files.
     *
     * @param source iterator over unsorted data
     * @param batchSize how much items can fit into heap memory
     * @param comparator used to sort data
     * @param serializer used to store data in temporary files
     * @param <E> type of data
     * @return iterator over sorted data set
     */
    public static <E> Iterator<E> sort(final Iterator<E> source, final int batchSize,
            final Comparator comparator, final Serializer serializer){
        if(batchSize<=0) throw new IllegalArgumentException();

        int counter = 0;
        final SortedSet<E> presort = new TreeSet<E>(comparator);
        final List<File> presortFiles = new ArrayList<File>();

        try{
            while(source.hasNext()){
                counter++;
                presort.add(source.next());

                if(counter>=batchSize){
                    //flush presort into temporary file
                    File f = Utils.tempDbFile();
                    f.deleteOnExit();
                    presortFiles.add(f);
                    DataOutputStream out = new DataOutputStream(new FileOutputStream(f));
                    for(E e:presort){
                        serializer.serialize(out,e);
                    }
                    out.close();
                    presort.clear();
                    counter = 0;
                }
            }
            //now all records from source are fetch
            if(presortFiles.isEmpty()){
                //no presort files were created, so on-heap sorting is enough
                return presort.iterator();
            }

            final int[] presortCount = new int[presortFiles.size()];
            Arrays.fill(presortCount, batchSize);
            //compose iterators which will iterate over data saved in files
            Iterator[] iterators = new Iterator[presortFiles.size()+1];
            final DataInputStream[] ins = new DataInputStream[presortFiles.size()];
            for(int i=0;i<presortFiles.size();i++){
                ins[i] = new DataInputStream(new FileInputStream(presortFiles.get(i)));
                final int pos = i;
                iterators[i] = new Iterator(){

                    @Override public boolean hasNext() {
                        return presortCount[pos]>0;
                    }

                    @Override public Object next() {
                        try {
                            Object ret =  serializer.deserialize(ins[pos],-1);
                            if(--presortCount[pos]==0){
                                ins[pos].close();
                                presortFiles.get(pos).delete();
                            }
                            return ret;
                        } catch (IOException e) {
                            throw new IOError(e);
                        }
                    }

                    @Override public void remove() {
                        //ignored
                    }

                };
            }

            //and add iterator over data on-heap
            iterators[iterators.length-1] = presort.iterator();


            //and finally sort presorted iterators and return iterators over them
            return sort(comparator, iterators);

        }catch(IOException e){
            throw new IOError(e);
        }finally{
            for(File f:presortFiles) f.delete();
        }
    }


    /**
     * Merge presorted iterators into single sorted iterator.
     *
     * @param comparator used to compare data
     * @param iterators array of already sorted iterators
     * @param <E> type of data
     * @return sorted iterator
     */
    public static <E> Iterator<E> sort(final Comparator comparator, final Iterator[] iterators) {

        return new Iterator<E>(){

            final Object[] items = new Object[iterators.length];
            E next = null;

            {
                for(int i=0;i<iterators.length;i++){
                    if(iterators[i].hasNext()){
                        items[i] = iterators[i].next();
                    }
                }
                next();
            }


            @Override public boolean hasNext() {
                return next!=null;
            }

            @Override public E next() {
                E oldNext = next;

                int hi = findHighest(items,comparator);
                next = (E) items[hi];

                items[hi] = iterators[hi].hasNext()?iterators[hi].next():null;

                return oldNext;
            }

            @Override public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }


    protected static <E> int findHighest(Object[] items, Comparator<E> comparator) {
        int min=0;
        for(int i=1;i<items.length;i++){
            if(items[i]==null) continue;
            if(items[min]==null|| comparator.compare((E)items[i],(E)items[min])<0)
                min = i;
        }
        return min;
    }



}
