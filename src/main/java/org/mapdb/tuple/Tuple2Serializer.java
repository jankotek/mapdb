package org.mapdb.tuple;

import org.jetbrains.annotations.NotNull;
import org.mapdb.DB;
import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;
import org.mapdb.hasher.Hasher;
import org.mapdb.serializer.Serializer;
import org.mapdb.serializer.GroupSerializerObjectArray;

import java.io.IOException;
import java.io.Serializable;
import java.util.Comparator;

import static org.mapdb.tuple.Tuple.compare2;
import static org.mapdb.tuple.Tuple.hiIfNull;

/**
 * Applies delta compression on array of tuple. First tuple value may be shared between consequentive tuples, so only
 * first occurrence is serialized. An example:
 *
 * <pre>
 *     Value            Serialized as
 *     -------------------------
 *     Tuple(1, 1)       1, 1
 *     Tuple(1, 2)          2
 *     Tuple(1, 3)          3
 *     Tuple(1, 4)          4
 * </pre>
 *
 * @param <A> first tuple value
 * @param <B> second tuple value
 */
public final class Tuple2Serializer<A,B> extends GroupSerializerObjectArray<Tuple2<A,B>>
        implements Serializable, DB.DBAware, Hasher<Tuple2<A,B>> {

    private static final long serialVersionUID = 2183804367032891772L;
    protected Comparator<A> aComparator;
    protected Comparator<B> bComparator;
    protected Serializer<A> aSerializer;
    protected Serializer<B> bSerializer;

    public Tuple2Serializer(){
        this(null, null, null, null);
    }


    public Tuple2Serializer(Serializer serializer){
        this(serializer, serializer);
    }

    public Tuple2Serializer(
            Serializer<A> aSerializer, Serializer<B> bSerializer){
        this(
                aSerializer, bSerializer,
                aSerializer.defaultHasher(), bSerializer.defaultHasher()
        );
    }
    /**
     * Construct new TupleSerializer. You may pass null for some value,
     * In that case 'default' value will be used, Comparable comparator and Default Serializer from DB.
     *
     */
    public Tuple2Serializer(
            Serializer<A> aSerializer, Serializer<B> bSerializer,
            Comparator<A> aComparator, Comparator<B> bComparator){
        this.aComparator = aComparator;
        this.bComparator = bComparator;
        this.aSerializer = aSerializer;
        this.bSerializer = bSerializer;
    }



    @Override
    public void valueArraySerialize(DataOutput2 out, Object vals) throws IOException {
        Object[] keys = (Object[]) vals;
        int end = keys.length;
        int acount=0;
        for(int i=0;i<end;i++){
            Tuple2<A,B> t = (Tuple2<A, B>) keys[i];
            if(acount==0){
                //write new A
                aSerializer.serialize(out,t.a);
                //count how many A are following
                acount=1;
                while(i+acount<end && aComparator.compare(t.a, ((Tuple2<A, B>) keys[i+acount]).a)==0){
                    acount++;
                }
                out.packInt(acount);
            }
            bSerializer.serialize(out,t.b);

            acount--;
        }
    }

    @Override
    public Object[] valueArrayDeserialize(DataInput2 in, int size) throws IOException {
        Object[] ret = new Object[size];
        A a = null;
        int acount = 0;

        for(int i=0;i<size;i++){
            if(acount==0){
                //read new A
                a = aSerializer.deserialize(in,-1);
                acount = in.unpackInt();
            }
            B b = bSerializer.deserialize(in,-1);
            ret[i]= Tuple.t2(a,b);
            acount--;
        }
        assert(acount==0);

        return ret;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Tuple2Serializer t = (Tuple2Serializer) o;

        return
                Tuple.eq(aComparator, t.aComparator) &&
                        Tuple.eq(aSerializer, t.aSerializer) &&
                        Tuple.eq(bSerializer, t.bSerializer);
    }

    @Override
    public int hashCode() {
        int result = aComparator != null ? aComparator.hashCode() : 0;
        result =  -1640531527 * result + (aSerializer != null ? aSerializer.hashCode() : 0);
        result =  -1640531527 * result + (bSerializer != null ? bSerializer.hashCode() : 0);
        return result;
    }


    @Override
    public void serialize(@NotNull DataOutput2 out, @NotNull Tuple2<A, B> value) throws IOException {
        aSerializer.serialize(out, value.a);
        bSerializer.serialize(out, value.b);
    }

    @Override
    public Tuple2<A, B> deserialize(@NotNull DataInput2 input, int available) throws IOException {
        return new Tuple2(
                aSerializer.deserialize(input, -1),
                bSerializer.deserialize(input, -1)
                );
    }


    @Override
    public int compare(final Tuple2<A,B> o1, final Tuple2<A,B> o2) {
        int i = compare2(aComparator,o1.a,o2.a);
        if(i!=0) return i;
        return compare2(bComparator,o1.b,o2.b);
    }

    @Override
    public boolean equals(Tuple2<A, B> first, Tuple2<A, B> second) {
        return 0==compare(first,second);
    }


    @Override
    public int hashCode(@NotNull Tuple2<A, B> o, int seed) {
        seed =  -1640531527 * seed + aSerializer.defaultHasher().hashCode(o.a, seed);
        seed =  -1640531527 * seed + bSerializer.defaultHasher().hashCode(o.b, seed);
        return seed;
    }

    @Override
    public void callbackDB(@NotNull DB db) {
        if(aComparator==null) aComparator = (Comparator<A>) db.getDefaultSerializer().defaultHasher();
        if(bComparator==null) bComparator = (Comparator<B>) db.getDefaultSerializer().defaultHasher();
        if(aSerializer==null) aSerializer = (Serializer<A>) db.getDefaultSerializer();
        if(bSerializer==null) bSerializer = (Serializer<B>) db.getDefaultSerializer();
    }

    @Override
    public Tuple2<A, B> nextValue(Tuple2<A, B> v) {
        return new Tuple2(hiIfNull(v.a), hiIfNull(v.b));
    }

    @Override
    public Hasher<Tuple2<A, B>> defaultHasher() {
        //TODO separate class
        return this;
    }
}
