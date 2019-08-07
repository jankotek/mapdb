package org.mapdb.util;

import java.util.Comparator;

public class JavaUtils {


    public static final Comparator COMPARABLE_COMPARATOR = new Comparator<Comparable>() {
        @Override
        public int compare(Comparable o1, Comparable o2) {
            return o1.compareTo(o2);
        }
    };
}
