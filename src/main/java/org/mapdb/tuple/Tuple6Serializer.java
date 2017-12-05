package org.mapdb.tuple;

import org.jetbrains.annotations.NotNull;
import org.mapdb.DB;
import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;
import org.mapdb.serializer.Serializer;
import org.mapdb.serializer.GroupSerializerObjectArray;

import java.io.IOException;
import java.io.Serializable;
import java.util.Comparator;

import static org.mapdb.tuple.Tuple.compare2;
import static org.mapdb.tuple.Tuple.hiIfNull;

/**
 * Applies delta compression on array of tuple. First, second and third tuple value may be shared between consequential tuples,
 * so only first occurrence is serialized. An example:
 *
 * <pre>
 *     Value                Serialized as
 *     ----------------------------------
 *     Tuple(1, 2, 1, 1)       1, 2, 1, 1
 *     Tuple(1, 2, 1, 2)                2
 *     Tuple(1, 3, 3, 3)          3, 3, 3
 *     Tuple(1, 3, 4, 4)             4, 4
 * </pre>
 *
 * @param <A> first tuple value
 * @param <B> second tuple value
 * @param <C> third tuple value
 */
public class Tuple6Serializer<A,B,C,D,E,F> extends GroupSerializerObjectArray<Tuple6<A,B,C,D,E,F>>
        implements Serializable, DB.DBAware {

    private static final long serialVersionUID = 3666600849149868404L;
    protected Comparator<A> aComparator;
    protected Comparator<B> bComparator;
    protected Comparator<C> cComparator;
    protected Comparator<D> dComparator;
    protected Comparator<E> eComparator;
    protected Comparator<F> fComparator;
    protected Serializer<A> aSerializer;
    protected Serializer<B> bSerializer;
    protected Serializer<C> cSerializer;
    protected Serializer<D> dSerializer;
    protected Serializer<E> eSerializer;
    protected Serializer<F> fSerializer;


    public Tuple6Serializer(){
        this(null, null, null, null, null, null, null, null, null, null, null, null);
    }

    public Tuple6Serializer(Serializer serializer){
        this(serializer, serializer, serializer, serializer, serializer, serializer);
    }

    public Tuple6Serializer(
            Serializer<A> aSerializer, Serializer<B> bSerializer, Serializer<C> cSerializer, Serializer<D> dSerializer, Serializer<E> eSerializer,Serializer<F> fSerializer){
        this(
             aSerializer, bSerializer, cSerializer, dSerializer, eSerializer, fSerializer,
             aSerializer, bSerializer, cSerializer, dSerializer, eSerializer, fSerializer
        );
    }
    /**
     * Construct new TupleSerializer. You may pass null for some value,
     * In that case 'default' value will be used, Comparable comparator and Default Serializer from DB.
     *
     */
    public Tuple6Serializer(
                               Serializer<A> aSerializer, Serializer<B> bSerializer, Serializer<C> cSerializer, Serializer<D> dSerializer, Serializer<E> eSerializer,Serializer<F> fSerializer,
                               Comparator<A> aComparator, Comparator<B> bComparator, Comparator<C> cComparator, Comparator<D> dComparator,Comparator<E> eComparator,Comparator<F> fComparator){
        this.aComparator = aComparator;
        this.bComparator = bComparator;
        this.cComparator = cComparator;
        this.dComparator = dComparator;
        this.eComparator = eComparator;
        this.fComparator = fComparator;
        this.aSerializer = aSerializer;
        this.bSerializer = bSerializer;
        this.cSerializer = cSerializer;
        this.dSerializer = dSerializer;
        this.eSerializer = eSerializer;
        this.fSerializer = fSerializer;
    }


    @Override
    public void valueArraySerialize(DataOutput2 out, Object vals) throws IOException {
        Object[] keys = (Object[]) vals;
        int end = keys.length;
        int acount=0;
        int bcount=0;
        int ccount=0;
        int dcount=0;
        int ecount=0;
        for(int i=0;i<end;i++){
            Tuple6<A,B,C,D,E,F> t = (Tuple6<A, B,C,D,E,F>) keys[i];
            if(acount==0){
                //write new A
                aSerializer.serialize(out,t.a);
                //count how many A are following
                acount=1;
                while(i+acount<end && aComparator.compare(t.a, ((Tuple6<A, B, C,D, E,F>) keys[i+acount]).a)==0){
                    acount++;
                }
                out.packInt(acount);
            }
            if(bcount==0){
                //write new B
                bSerializer.serialize(out,t.b);
                //count how many B are following
                bcount=1;
                while(i+bcount<end && bComparator.compare(t.b, ((Tuple6<A, B,C,D, E,F>) keys[i+bcount]).b)==0){
                    bcount++;
                }
                out.packInt(bcount);
            }
            if(ccount==0){
                //write new C
                cSerializer.serialize(out,t.c);
                //count how many C are following
                ccount=1;
                while(i+ccount<end && cComparator.compare(t.c, ((Tuple6<A, B,C,D, E,F>) keys[i+ccount]).c)==0){
                    ccount++;
                }
                out.packInt(ccount);
            }

            if(dcount==0){
                //write new C
                dSerializer.serialize(out,t.d);
                //count how many D are following
                dcount=1;
                while(i+dcount<end && dComparator.compare(t.d, ((Tuple6<A, B,C,D,E,F>) keys[i+dcount]).d)==0){
                    dcount++;
                }
                out.packInt(dcount);
            }

            if(ecount==0){
                //write new C
                eSerializer.serialize(out,t.e);
                //count how many E are following
                ecount=1;
                while(i+ecount<end && eComparator.compare(t.e, ((Tuple6<A, B,C,D,E,F>) keys[i+ecount]).e)==0){
                    ecount++;
                }
                out.packInt(ecount);
            }


            fSerializer.serialize(out,t.f);

            acount--;
            bcount--;
            ccount--;
            dcount--;
            ecount--;
        }
    }

    @Override
    public Object[] valueArrayDeserialize(DataInput2 in, int size) throws IOException {
        Object[] ret = new Object[size];
        A a = null;
        int acount = 0;
        B b = null;
        int bcount = 0;
        C c = null;
        int ccount = 0;
        D d = null;
        int dcount = 0;
        E e = null;
        int ecount = 0;


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
            if(ccount==0){
                //read new C
                c = cSerializer.deserialize(in,-1);
                ccount = in.unpackInt();
            }
            if(dcount==0){
                //read new D
                d = dSerializer.deserialize(in,-1);
                dcount = in.unpackInt();
            }

            if(ecount==0){
                //read new E
                e = eSerializer.deserialize(in,-1);
                ecount = in.unpackInt();
            }


            F f = fSerializer.deserialize(in,-1);
            ret[i]= Tuple.t6(a, b, c, d, e, f);
            acount--;
            bcount--;
            ccount--;
            dcount--;
            ecount--;
        }
        assert(acount==0);
        assert(bcount==0);
        assert(ccount==0);
        assert(dcount==0);
        assert(ecount==0);

        return ret;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Tuple6Serializer t = (Tuple6Serializer) o;

        return
                Tuple.eq(aComparator, t.aComparator) &&
                        Tuple.eq(bComparator, t.bComparator) &&
                        Tuple.eq(cComparator, t.cComparator) &&
                        Tuple.eq(dComparator, t.dComparator) &&
                        Tuple.eq(eComparator, t.eComparator) &&
                        Tuple.eq(aSerializer, t.aSerializer) &&
                        Tuple.eq(bSerializer, t.bSerializer) &&
                        Tuple.eq(cSerializer, t.cSerializer) &&
                        Tuple.eq(dSerializer, t.dSerializer) &&
                        Tuple.eq(eSerializer, t.eSerializer) &&
                        Tuple.eq(fSerializer, t.fSerializer);

    }

    @Override
    public int hashCode() {
        int result = aComparator != null ? aComparator.hashCode() : 0;
        result =  -1640531527 * result + (bComparator != null ? bComparator.hashCode() : 0);
        result =  -1640531527 * result + (cComparator != null ? cComparator.hashCode() : 0);
        result =  -1640531527 * result + (dComparator != null ? dComparator.hashCode() : 0);
        result =  -1640531527 * result + (eComparator != null ? eComparator.hashCode() : 0);
        result =  -1640531527 * result + (aSerializer != null ? aSerializer.hashCode() : 0);
        result =  -1640531527 * result + (bSerializer != null ? bSerializer.hashCode() : 0);
        result =  -1640531527 * result + (cSerializer != null ? cSerializer.hashCode() : 0);
        result =  -1640531527 * result + (dSerializer != null ? dSerializer.hashCode() : 0);
        result =  -1640531527 * result + (eSerializer != null ? eSerializer.hashCode() : 0);
        result =  -1640531527 * result + (fSerializer != null ? fSerializer.hashCode() : 0);
        return result;
    }


    @Override
    public void serialize(@NotNull DataOutput2 out, @NotNull Tuple6<A, B, C, D, E, F> value) throws IOException {
        aSerializer.serialize(out, value.a);
        bSerializer.serialize(out, value.b);
        cSerializer.serialize(out, value.c);
        dSerializer.serialize(out, value.d);
        eSerializer.serialize(out, value.e);
        fSerializer.serialize(out, value.f);
    }

    @Override
    public Tuple6<A, B, C, D, E, F> deserialize(@NotNull DataInput2 input, int available) throws IOException {
        return new Tuple6(
                aSerializer.deserialize(input, -1),
                bSerializer.deserialize(input, -1),
                cSerializer.deserialize(input, -1),
                dSerializer.deserialize(input, -1),
                eSerializer.deserialize(input, -1),
                fSerializer.deserialize(input, -1)
                );
    }


    @Override
    public int compare(final Tuple6<A,B,C,D,E,F> o1, final Tuple6<A,B,C,D,E,F> o2) {
        int i = compare2(aComparator,o1.a, o2.a);
        if (i != 0) return i;
        i = compare2(bComparator,o1.b, o2.b);
        if (i != 0) return i;
        i = compare2(cComparator,o1.c, o2.c);
        if (i != 0) return i;
        i = compare2(dComparator,o1.d, o2.d);
        if (i != 0) return i;
        i = compare2(eComparator,o1.e, o2.e);
        if (i != 0) return i;
        return compare2(fComparator,o1.f, o2.f);
    }

    @Override
    public boolean equals(Tuple6<A, B, C, D, E, F> first, Tuple6<A, B, C, D, E, F> second) {
        return 0==compare(first, second);
    }

    @Override
    public int hashCode(@NotNull Tuple6<A, B, C, D, E, F> o, int seed) {
        seed =  -1640531527 * seed + aSerializer.hashCode(o.a, seed);
        seed =  -1640531527 * seed + bSerializer.hashCode(o.b, seed);
        seed =  -1640531527 * seed + cSerializer.hashCode(o.c, seed);
        seed =  -1640531527 * seed + dSerializer.hashCode(o.d, seed);
        seed =  -1640531527 * seed + eSerializer.hashCode(o.e, seed);
        seed =  -1640531527 * seed + fSerializer.hashCode(o.f, seed);
        return seed;
    }


    @Override
    public void callbackDB(@NotNull DB db) {
        if(aComparator==null) aComparator = (Comparator<A>) db.getDefaultSerializer();
        if(bComparator==null) bComparator = (Comparator<B>) db.getDefaultSerializer();
        if(cComparator==null) cComparator = (Comparator<C>) db.getDefaultSerializer();
        if(dComparator==null) dComparator = (Comparator<D>) db.getDefaultSerializer();
        if(eComparator==null) eComparator = (Comparator<E>) db.getDefaultSerializer();
        if(fComparator==null) fComparator = (Comparator<F>) db.getDefaultSerializer();
        if(aSerializer==null) aSerializer = (Serializer<A>) db.getDefaultSerializer();
        if(bSerializer==null) bSerializer = (Serializer<B>) db.getDefaultSerializer();
        if(cSerializer==null) cSerializer = (Serializer<C>) db.getDefaultSerializer();
        if(dSerializer==null) dSerializer = (Serializer<D>) db.getDefaultSerializer();
        if(eSerializer==null) eSerializer = (Serializer<E>) db.getDefaultSerializer();
        if(fSerializer==null) fSerializer = (Serializer<F>) db.getDefaultSerializer();
    }

    @Override
    public Tuple6<A, B, C, D, E, F> nextValue(Tuple6<A, B, C, D, E, F> v) {
        return new Tuple6(hiIfNull(v.a), hiIfNull(v.b), hiIfNull(v.c), hiIfNull(v.d), hiIfNull(v.e), hiIfNull(v.f));
    }
}
