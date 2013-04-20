/*
 *  Copyright (c) 2012 Jan Kotek
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.mapdb;

import java.io.Serializable;

/**
 * Functional stuff. Tuples, function, callback methods etc..
 *
 * @author Jan Kotek
 */
@SuppressWarnings({ "unchecked", "rawtypes" })
public final class Fun {

    private Fun(){}

    /** positive infinity object. Is larger than anything else. Used in tuple comparators.
     * Negative infinity is represented by 'null' */
    public static final Object HI = new Object(){
        @Override public String toString() {
            return "HI";
        }
    };

    /** autocast version of `HI`*/
    public static final <A> A HI(){
        return (A) HI;
    }

    public static <A,B> Tuple2<A,B> t2(A a, B b) {
        return new Tuple2<A, B>(a,b);
    }

    public static <A,B,C> Tuple3<A,B,C> t3(A a, B b, C c) {
        return new Tuple3<A, B, C>((A)a, (B)b, (C)c);
    }

    public static <A,B,C,D> Tuple4<A,B,C,D> t4(A a, B b, C c, D d) {
        return new Tuple4<A, B, C, D>(a,b,c,d);
    }


    static public final class Tuple2<A,B> implements Comparable, Serializable {
		
    	private static final long serialVersionUID = -8816277286657643283L;
		
		final public A a;
        final public B b;

        public Tuple2(A a, B b) {
            this.a = a;
            this.b = b;
        }

        @Override public int compareTo(Object o) {
            final Tuple2 oo = (Tuple2) o;
            if(a!=oo.a){
                if(a==null || oo.a==HI) return -1;
                if(a==HI || oo.a==null) return 1;

                final int c = ((Comparable)a).compareTo(oo.a);
                if(c!=0) return c;
            }

            if(b!=oo.b){
                if(b==null || oo.b==HI) return -1;
                if(b==HI || oo.b==null) return 1;

                final int i = ((Comparable)b).compareTo(oo.b);
                if(i!=0) return i;
            }
            return 0;
        }

        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            final Tuple2 tuple2 = (Tuple2) o;

            if (a != null ? !a.equals(tuple2.a) : tuple2.a != null) return false;
            if (b != null ? !b.equals(tuple2.b) : tuple2.b != null) return false;

            return true;
        }

        @Override public int hashCode() {
            int result = a != null ? a.hashCode() : 0;
            result = 31 * result + (b != null ? b.hashCode() : 0);
            return result;
        }

        @Override public String toString() {
            return "Tuple2[" + a +", "+b+"]";
        }
    }

    final static public class Tuple3<A,B,C> implements Comparable, Serializable{

    	private static final long serialVersionUID = 11785034935947868L;
    	
		final public A a;
        final public B b;
        final public C c;

        public Tuple3(A a, B b, C c) {
            this.a = a;
            this.b = b;
            this.c = c;
        }

        @Override public int compareTo(Object o) {
            final Tuple3 oo = (Tuple3) o;
            if(a!=oo.a){
                if(a==null || oo.a==HI) return -1;
                if(a==HI ||  oo.a==null) return 1;

                final int c = ((Comparable)a).compareTo(oo.a);
                if(c!=0) return c;
            }

            if(b!=oo.b){
                if(b==null || oo.b==HI) return -1;
                if(b==HI || oo.b==null) return 1;

                final int i = ((Comparable)b).compareTo(oo.b);
                if(i!=0) return i;
            }

            if(c!=oo.c){
                if(c==null || oo.c==HI) return -1;
                if(c==HI || oo.c==null) return 1;

                final int i = ((Comparable)c).compareTo(oo.c);
                if(i!=0) return i;
            }

            return 0;
        }


        @Override public String toString() {
            return "Tuple3[" + a +", "+b+", "+c+"]";
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Tuple3 tuple3 = (Tuple3) o;

            if (a != null ? !a.equals(tuple3.a) : tuple3.a != null) return false;
            if (b != null ? !b.equals(tuple3.b) : tuple3.b != null) return false;
            if (c != null ? !c.equals(tuple3.c) : tuple3.c != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = a != null ? a.hashCode() : 0;
            result = 31 * result + (b != null ? b.hashCode() : 0);
            result = 31 * result + (c != null ? c.hashCode() : 0);
            return result;
        }
    }

    final static public class Tuple4<A,B,C,D> implements Comparable, Serializable{

    	private static final long serialVersionUID = 1630397500758650718L;
    	
		final public A a;
        final public B b;
        final public C c;
        final public D d;

        public Tuple4(A a, B b, C c, D d) {
            this.a = a;
            this.b = b;
            this.c = c;
            this.d = d;
        }

        @Override public int compareTo(Object o) {
            final Tuple4 oo = (Tuple4) o;
            if(a!=oo.a){
                if(a==null || oo.a==HI) return -1;
                if(a==HI || oo.a==null) return 1;

                final int c = ((Comparable)a).compareTo(oo.a);
                if(c!=0) return c;
            }

            if(b!=oo.b){
                if(b==null || oo.b==HI) return -1;
                if(b==HI || oo.b==null) return 1;

                final int i = ((Comparable)b).compareTo(oo.b);
                if(i!=0) return i;
            }

            if(c!=oo.c){
                if(c==null || oo.c==HI) return -1;
                if(c==HI || oo.c==null) return 1;

                final int i = ((Comparable)c).compareTo(oo.c);
                if(i!=0) return i;
            }

            if(d!=oo.d){
                if(d==null || oo.d==HI) return -1;
                if(d==HI || oo.d==null) return 1;

                final int i = ((Comparable)d).compareTo(oo.d);
                if(i!=0) return i;
            }


            return 0;
        }


        @Override public String toString() {
            return "Tuple4[" + a +", "+b+", "+c+", "+d+"]";
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Tuple4 tuple4 = (Tuple4) o;

            if (a != null ? !a.equals(tuple4.a) : tuple4.a != null) return false;
            if (b != null ? !b.equals(tuple4.b) : tuple4.b != null) return false;
            if (c != null ? !c.equals(tuple4.c) : tuple4.c != null) return false;
            if (d != null ? !d.equals(tuple4.d) : tuple4.d != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = a != null ? a.hashCode() : 0;
            result = 31 * result + (b != null ? b.hashCode() : 0);
            result = 31 * result + (c != null ? c.hashCode() : 0);
            result = 31 * result + (d != null ? d.hashCode() : 0);
            return result;
        }
    }


    public interface Function1<R,A>{
        R run(A a);
    }

    public interface Function2<R,A,B>{
        R run(A a, B b);
    }

    public interface Runnable2<A,B>{
        void run(A a, B b);
    }

    public interface Runnable3<A,B,C>{
        void run(A a, B b, C c);
    }



}
