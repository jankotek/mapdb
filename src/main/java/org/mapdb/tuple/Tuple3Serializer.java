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
 * Applies delta compression on array of tuple. First and second tuple value may be shared between consequentive tuples, so only
 * first occurrence is serialized. An example:
 *
 * <pre>
 *     Value            Serialized as
 *     ----------------------------
 *     Tuple(1, 2, 1)       1, 2, 1
 *     Tuple(1, 2, 2)             2
 *     Tuple(1, 3, 3)          3, 3
 *     Tuple(1, 3, 4)             4
 * </pre>
 *
 * @param <A> first tuple value
 * @param <B> second tuple value
 * @param <C> third tuple value
 */
public class Tuple3Serializer<A,B,C> extends GroupSerializerObjectArray<Tuple3<A,B,C>> 
        implements Serializable, DB.DBAware, Hasher<Tuple3<A,B,C>> {

    private static final long serialVersionUID = 2932442956138713885L;
    protected Comparator<A> aComparator;
    protected Comparator<B> bComparator;
    protected Comparator<C> cComparator;
    protected Serializer<A> aSerializer;
    protected Serializer<B> bSerializer;
    protected Serializer<C> cSerializer;


    public Tuple3Serializer(){
        this(null, null, null, null, null, null);
    }


    public Tuple3Serializer(Serializer serializer){
        this(serializer, serializer, serializer);
    }

    public Tuple3Serializer(
            Serializer<A> aSerializer, Serializer<B> bSerializer, Serializer<C> cSerializer){
        this(
                aSerializer, bSerializer, cSerializer,
                aSerializer.defaultHasher(), bSerializer.defaultHasher(), cSerializer.defaultHasher()
        );
    }
    /**
     * Construct new TupleSerializer. You may pass null for some value,
     * In that case 'default' value will be used, Comparable comparator and Default Serializer from DB.
     *
     */
    public Tuple3Serializer(
            Serializer<A> aSerializer, Serializer<B> bSerializer, Serializer<C> cSerializer,
            Comparator<A> aComparator, Comparator<B> bComparator, Comparator<C> cComparator){
        this.aComparator = aComparator;
        this.bComparator = bComparator;
        this.cComparator = cComparator;
        this.aSerializer = aSerializer;
        this.bSerializer = bSerializer;
        this.cSerializer = cSerializer;
    }

    @Override
    public void valueArraySerialize(DataOutput2 out, Object vals) throws IOException {
        Object[] keys = (Object[]) vals;
        int end = keys.length;
        int acount=0;
        int bcount=0;
        for(int i=0;i<end;i++){
            Tuple3<A,B,C> t = (Tuple3<A, B,C>) keys[i];
            if(acount==0){
                //write new A
                aSerializer.serialize(out,t.a);
                //count how many A are following
                acount=1;
                while(i+acount<end && aComparator.compare(t.a, ((Tuple3<A, B, C>) keys[i+acount]).a)==0){
                    acount++;
                }
                out.packInt(acount);
            }
            if(bcount==0){
                //write new B
                bSerializer.serialize(out,t.b);
                //count how many B are following
                bcount=1;
                while(i+bcount<end && bComparator.compare(t.b, ((Tuple3<A, B,C>) keys[i+bcount]).b)==0){
                    bcount++;
                }
                out.packInt(bcount);
            }


            cSerializer.serialize(out,t.c);

            acount--;
            bcount--;
        }


    }

    @Override
    public Object[] valueArrayDeserialize(DataInput2 in, int size) throws IOException {
        Object[] ret = new Object[size];
        A a = null;
        int acount = 0;
        B b = null;
        int bcount = 0;

        for(int i=0;i<size;i++){
            if(acount==0){
                //read new A
                a = aSerializer.deserialize(in,-1);
                acount = in.unpackInt();
            }
            if(bcount==0){
                //read new B
                b = bSerializer.deserialize(in,-1);
                bcount = in.unpackInt();
            }
            C c = cSerializer.deserialize(in,-1);
            ret[i]= Tuple.t3(a, b, c);
            acount--;
            bcount--;
        }
        assert(acount==0);
        assert(bcount==0);

        return ret;
    }



    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Tuple3Serializer t = (Tuple3Serializer) o;

        return
                Tuple.eq(aComparator, t.aComparator) &&
                        Tuple.eq(bComparator, t.bComparator) &&
                        Tuple.eq(aSerializer, t.aSerializer) &&
                        Tuple.eq(bSerializer, t.bSerializer) &&
                        Tuple.eq(cSerializer, t.cSerializer);
    }

    @Override
    public int hashCode() {
        int result = aComparator != null ? aComparator.hashCode() : 0;
        result =  -1640531527 * result + (bComparator != null ? bComparator.hashCode() : 0);
        result =  -1640531527 * result + (aSerializer != null ? aSerializer.hashCode() : 0);
        result =  -1640531527 * result + (bSerializer != null ? bSerializer.hashCode() : 0);
        result =  -1640531527 * result + (cSerializer != null ? cSerializer.hashCode() : 0);
        return result;
    }

    @Override
    public void serialize(@NotNull DataOutput2 out, @NotNull Tuple3<A, B, C> value) throws IOException {
        aSerializer.serialize(out, value.a);
        bSerializer.serialize(out, value.b);
        cSerializer.serialize(out, value.c);
    }

    @Override
    public Tuple3<A, B, C> deserialize(@NotNull DataInput2 input, int available) throws IOException {
        return new Tuple3(
            aSerializer.deserialize(input, -1),
            bSerializer.deserialize(input, -1),
            cSerializer.deserialize(input, -1)
        );
    }

    @Override
    public int compare(final Tuple3<A,B,C> o1, final Tuple3<A,B,C> o2) {
        int i = compare2(aComparator,o1.a,o2.a);
        if(i!=0) return i;
        i = compare2(bComparator,o1.b,o2.b);
        if(i!=0) return i;
        return compare2(cComparator,o1.c,o2.c);
    }

    @Override
    public boolean equals(Tuple3<A, B, C> first, Tuple3<A, B, C> second) {
        return 0==compare(first,second);
    }


    @Override
    public int hashCode(@NotNull Tuple3<A, B, C> o, int seed) {
        seed =  -1640531527 * seed + aSerializer.defaultHasher().hashCode(o.a, seed);
        seed =  -1640531527 * seed + bSerializer.defaultHasher().hashCode(o.b, seed);
        seed =  -1640531527 * seed + cSerializer.defaultHasher().hashCode(o.c, seed);
        return seed;
    }


    @Override
    public void callbackDB(@NotNull DB db) {
        if(aComparator==null) aComparator = (Comparator<A>) db.getDefaultSerializer().defaultHasher();
        if(bComparator==null) bComparator = (Comparator<B>) db.getDefaultSerializer().defaultHasher();
        if(cComparator==null) cComparator = (Comparator<C>) db.getDefaultSerializer().defaultHasher();
        if(aSerializer==null) aSerializer = (Serializer<A>) db.getDefaultSerializer();
        if(bSerializer==null) bSerializer = (Serializer<B>) db.getDefaultSerializer();
        if(cSerializer==null) cSerializer = (Serializer<C>) db.getDefaultSerializer();
    }

    @Override
    public Tuple3<A, B, C> nextValue(Tuple3<A, B, C> v) {
        return new Tuple3(hiIfNull(v.a), hiIfNull(v.b), hiIfNull(v.c));
    }

    @Override
    public Hasher<Tuple3<A, B, C>> defaultHasher() {
        //TODO separate class
        return this;
    }
}
