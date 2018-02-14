package org.mapdb.hasher;

import org.jetbrains.annotations.NotNull;
import org.mapdb.util.DataIO;

import java.util.Comparator;
import java.util.Objects;

/** hash code, comparation and equality */
public interface Hasher<A> extends Comparator<A> {


    /**
     * Returns if the first and second arguments are equal to each other.
     * Consequently, if both arguments are {@code null}, {@code true} is
     * returned and if exactly one argument is {@code null}, {@code false} is
     * returned.
     *
     * @param first an object
     * @param second another object to be compared with the first object for
     * equality
     *
     * @return if the first and second arguments are equal to each other
     * @see Object#equals(Object)
     */
    boolean equals(A first, A second);

    /**
     * Returns a hash code of a given non-null argument. The output of the
     * method is affected by the given seed, allowing protection against crafted
     * hash attacks and to provide a better distribution of hashes.
     *
     * @param o an object
     * @param seed used to "scramble" the
     * @return a hash code of a non-null argument
     * @see Object#hashCode
     * @throws NullPointerException if the provided object is null
     */
    int hashCode(@NotNull A o, int seed);

}
