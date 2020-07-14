package org.mapdb.list;

import org.mapdb.ser.Serializer;
import org.mapdb.store.StoreTxWrap;

import java.util.AbstractCollection;
import java.util.Iterator;
import java.util.List;

public class ListTxWrap<E> extends AbstractCollection<E> {

    final StoreTxWrap tx;
    final Serializer<E> ser;

    ThreadLocal<List<E>> threadLocal = new ThreadLocal<>(){
        @Override
        protected List<E> initialValue() {
            return MonolithList.Maker
                    .newList(tx.newTx(), ser)
                    .make();
        }
    };

    public ListTxWrap(StoreTxWrap tx, Serializer<E> ser) {
        this.tx = tx;
        this.ser = ser;
    }

    @Override
    public boolean add(E e) {
        return threadLocal.get().add(e);
    }

    @Override
    public Iterator<E> iterator() {
        return threadLocal.get().iterator();
    }

    @Override
    public int size() {
        return threadLocal.get().size();
    }
}
