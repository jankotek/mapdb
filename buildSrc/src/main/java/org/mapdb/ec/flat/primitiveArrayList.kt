package org.mapdb.ec.flat


import org.mapdb.ec.flat.copyrightAndOthers.*
import org.mapdb.ec.flat.primitiveEquals.*
import org.mapdb.ec.flat.primitiveHashCode.*
import org.mapdb.ec.flat.primitiveLiteral.*

val prim = Primitive.LONG
val name="Long"
val type="long"

fun fileName(primitive:String) = name+"ArrayList.java"

val packageName = "org.mapdb.ec.flat.primitive"



val targetPath = "../srcGen/main/java/"+(packageName.replace('.','/')+"/") + fileName(name)

val str = """${copyrightAndOthers()}

package ${packageName};

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.eclipse.collections.api.${name}Iterable;
import org.eclipse.collections.api.Lazy${name}Iterable;
import org.eclipse.collections.api.RichIterable;
import org.eclipse.collections.api.block.function.primitive.Object${name}IntToObjectFunction;
import org.eclipse.collections.api.block.function.primitive.Object${name}ToObjectFunction;
import org.eclipse.collections.api.block.function.primitive.${name}ToObjectFunction;
import org.eclipse.collections.api.block.predicate.primitive.${name}Predicate;
import org.eclipse.collections.api.block.procedure.primitive.${name}IntProcedure;
import org.eclipse.collections.api.block.procedure.primitive.${name}Procedure;
import org.eclipse.collections.api.collection.primitive.Mutable${name}Collection;
import org.eclipse.collections.api.iterator.Mutable${name}Iterator;
import org.eclipse.collections.api.iterator.${name}Iterator;
import org.eclipse.collections.api.list.MutableList;
import org.eclipse.collections.api.list.primitive.${name}List;
import org.eclipse.collections.api.list.primitive.Immutable${name}List;
import org.eclipse.collections.api.list.primitive.Mutable${name}List;
import org.eclipse.collections.api.set.primitive.${name}Set;
import org.eclipse.collections.api.set.primitive.Mutable${name}Set;
import org.eclipse.collections.api.tuple.primitive.${name}${name}Pair;
import org.eclipse.collections.api.tuple.primitive.${name}ObjectPair;
import org.eclipse.collections.impl.factory.Lists;
import org.eclipse.collections.impl.factory.primitive.${name}Lists;
import org.eclipse.collections.impl.lazy.primitive.Reverse${name}Iterable;
import org.eclipse.collections.impl.list.mutable.FastList;
import org.eclipse.collections.impl.primitive.Abstract${name}Iterable;
import org.eclipse.collections.impl.set.mutable.primitive.${name}HashSet;
import org.eclipse.collections.impl.tuple.primitive.PrimitiveTuples;
import org.eclipse.collections.impl.utility.Iterate;

import org.eclipse.collections.impl.list.mutable.primitive.UnmodifiableLongList;
import org.eclipse.collections.impl.list.mutable.primitive.SynchronizedLongList;

/**
 * ${name}ArrayList is similar to {@link FastList}, and is memory-optimized for ${type} primitives.
 * This file was automatically generated from template file primitiveArrayList.stg.
 *
 * @since 3.0.
 */
public class ${name}ArrayList extends Abstract${name}Iterable
        implements Mutable${name}List, Externalizable
{
    private static final long serialVersionUID = 1L;
    private static final ${type}[] DEFAULT_SIZED_EMPTY_ARRAY = {};
    private static final ${type}[] ZERO_SIZED_ARRAY = {};
    private static final int MAXIMUM_ARRAY_SIZE = Integer.MAX_VALUE - 8;

    protected int size;
    protected transient ${type}[] items = DEFAULT_SIZED_EMPTY_ARRAY;

    public ${name}ArrayList()
    {
    }

    public ${name}ArrayList(int initialCapacity)
    {
        this.items = initialCapacity == 0 ? ZERO_SIZED_ARRAY : new ${type}[initialCapacity];
    }

    public ${name}ArrayList(${type}... array)
    {
        this.size = array.length;
        this.items = array;
    }

    /**
     * Creates a new list using the passed {@code elements} argument as the backing store.
     * <p>
     * !!! WARNING: This method uses the passed in array, so can be very unsafe if the original
     * array is held onto anywhere else. !!!
     */
    public static ${name}ArrayList newListWith(${type}... elements)
    {
        return new ${name}ArrayList(elements);
    }

    public static ${name}ArrayList newList(${name}Iterable source)
    {
        return ${name}ArrayList.newListWith(source.toArray());
    }

    public static ${name}ArrayList newWithNValues(int size, ${type} value)
    {
        ${name}ArrayList newList = new ${name}ArrayList(size);
        newList.size = size;
        Arrays.fill(newList.items, value);
        return newList;
    }

    @Override
    public int size()
    {
        return this.size;
    }

    @Override
    public void clear()
    {
        Arrays.fill(this.items, 0, size, ${zero(type)});
        this.size = 0;
    }

    @Override
    public boolean contains(${type} value)
    {
        for (int i = 0; i < this.size; i++)
        {
            if (${equals(type,"this.items[i]", "value")})
            {
                return true;
            }
        }
        return false;
    }

    @Override
    public ${type} get(int index)
    {
        if (index < this.size)
        {
            return this.items[index];
        }
        throw this.newIndexOutOfBoundsException(index);
    }

    private IndexOutOfBoundsException newIndexOutOfBoundsException(int index)
    {
        return new IndexOutOfBoundsException("Index: " + index + " Size: " + this.size);
    }

    @Override
    public ${type} getFirst()
    {
        this.checkEmpty();
        return this.items[0];
    }

    @Override
    public ${type} getLast()
    {
        this.checkEmpty();
        return this.items[this.size() - 1];
    }

    private void checkEmpty()
    {
        if (this.isEmpty())
        {
            throw this.newIndexOutOfBoundsException(0);
        }
    }

    @Override
    public int indexOf(${type} value)
    {
        for (int i = 0; i < this.size; i++)
        {
            if (${equals(type,"this.items[i]", "value")})
            {
                return i;
            }
        }
        return -1;
    }

    @Override
    public int lastIndexOf(${type} value)
    {
        for (int i = this.size - 1; i >= 0; i--)
        {
            if (${equals(type,"this.items[i]", "value")})
            {
                return i;
            }
        }
        return -1;
    }

    public void trimToSize()
    {
        if (this.size < this.items.length)
        {
            this.transferItemsToNewArrayWithCapacity(this.size);
        }
    }

    private void transferItemsToNewArrayWithCapacity(int newCapacity)
    {
        this.items = this.copyItemsWithNewCapacity(newCapacity);
    }

    private ${type}[] copyItemsWithNewCapacity(int newCapacity)
    {
        ${type}[] newItems = new ${type}[newCapacity];
        System.arraycopy(this.items, 0, newItems, 0, Math.min(this.size, newCapacity));
        return newItems;
    }

    private int sizePlusFiftyPercent(int oldSize)
    {
        int result = oldSize + (oldSize >> 1) + 1;
        return result < oldSize ? MAXIMUM_ARRAY_SIZE : result;
    }

    public void ensureCapacity(int minCapacity)
    {
        int oldCapacity = this.items.length;
        if (minCapacity > oldCapacity)
        {
            int newCapacity = Math.max(this.sizePlusFiftyPercent(oldCapacity), minCapacity);
            this.transferItemsToNewArrayWithCapacity(newCapacity);
        }
    }

    private void ensureCapacityForAdd()
    {
        if (this.items == DEFAULT_SIZED_EMPTY_ARRAY)
        {
            this.items = new ${type}[10];
        }
        else
        {
            this.transferItemsToNewArrayWithCapacity(this.sizePlusFiftyPercent(this.size));
        }
    }

    @Override
    public boolean add(${type} newItem)
    {
        if (this.items.length == this.size)
        {
            this.ensureCapacityForAdd();
        }
        this.items[this.size] = newItem;
        this.size++;
        return true;
    }

    @Override
    public boolean addAll(${type}... source)
    {
        if (source.length < 1)
        {
            return false;
        }
        this.copyItems(source.length, source);
        return true;
    }

    @Override
    public boolean addAll(${name}Iterable source)
    {
        if (source instanceof ${name}ArrayList)
        {
            if (source.isEmpty())
            {
                return false;
            }
            ${name}ArrayList other = (${name}ArrayList) source;
            this.copyItems(other.size(), other.items);
            return true;
        }
        return this.addAll(source.toArray());
    }

    private void copyItems(int sourceSize, ${type}[] source)
    {
        int newSize = this.size + sourceSize;
        this.ensureCapacity(newSize);
        System.arraycopy(source, 0, this.items, this.size, sourceSize);
        this.size = newSize;
    }

    private void throwOutOfBounds(int index)
    {
        throw this.newIndexOutOfBoundsException(index);
    }

    @Override
    public void addAtIndex(int index, ${type} element)
    {
        if (index > -1 && index < this.size)
        {
            this.addAtIndexLessThanSize(index, element);
        }
        else if (index == this.size)
        {
            this.add(element);
        }
        else
        {
            this.throwOutOfBounds(index);
        }
    }

    private void addAtIndexLessThanSize(int index, ${type} element)
    {
        int oldSize = this.size;
        this.size++;
        if (this.items.length == oldSize)
        {
            ${type}[] newItems = new ${type}[this.sizePlusFiftyPercent(oldSize)];
            if (index > 0)
            {
                System.arraycopy(this.items, 0, newItems, 0, index);
            }
            System.arraycopy(this.items, index, newItems, index + 1, oldSize - index);
            this.items = newItems;
        }
        else
        {
            System.arraycopy(this.items, index, this.items, index + 1, oldSize - index);
        }
        this.items[index] = element;
    }

    @Override
    public boolean addAllAtIndex(int index, ${type}... source)
    {
        if (index > this.size || index < 0)
        {
            this.throwOutOfBounds(index);
        }
        if (source.length == 0)
        {
            return false;
        }
        int sourceSize = source.length;
        int newSize = this.size + sourceSize;
        this.ensureCapacity(newSize);
        this.shiftElementsAtIndex(index, sourceSize);
        System.arraycopy(source, 0, this.items, index, sourceSize);
        this.size = newSize;
        return true;
    }

    @Override
    public boolean addAllAtIndex(int index, ${name}Iterable source)
    {
        return this.addAllAtIndex(index, source.toArray());
    }

    private void shiftElementsAtIndex(int index, int sourceSize)
    {
        int numberToMove = this.size - index;
        if (numberToMove > 0)
        {
            System.arraycopy(this.items, index, this.items, index + sourceSize, numberToMove);
        }
    }

    @Override
    public boolean remove(${type} value)
    {
        int index = this.indexOf(value);
        if (index >= 0)
        {
            this.removeAtIndex(index);
            return true;
        }
        return false;
    }

    @Override
    public boolean removeIf(${name}Predicate predicate)
    {
        int currentFilledIndex = 0;
        for (int i = 0; i < this.size; i++)
        {
            ${type} item = this.items[i];
            if (!predicate.accept(item))
            {
                // keep it
                if (currentFilledIndex != i)
                {
                    this.items[currentFilledIndex] = item;
                }
                currentFilledIndex++;
            }
        }
        boolean changed = currentFilledIndex < this.size;
        this.wipeAndResetTheEnd(currentFilledIndex);
        return changed;
    }

    private void wipeAndResetTheEnd(int newCurrentFilledIndex)
    {
        for (int i = newCurrentFilledIndex; i < this.size; i++)
        {
            this.items[i] = ${zero(type)};
        }
        this.size = newCurrentFilledIndex;
    }

    @Override
    public boolean removeAll(${name}Iterable source)
    {
        boolean modified = false;
        for (int index = 0; index < this.size; index++)
        {
            if (source.contains(this.get(index)))
            {
                this.removeAtIndex(index);
                index--;
                modified = true;
            }
        }
        return modified;
    }

    @Override
    public boolean removeAll(${type}... source)
    {
        ${name}HashSet set = ${name}HashSet.newSetWith(source);
        ${type}[] newItems = new ${type}[this.size];
        int count = 0;
        int oldSize = this.size;
        for (int index = 0; index < this.size; index++)
        {
            if (!set.contains(this.items[index]))
            {
                newItems[count] = this.items[index];
                count++;
            }
        }
        this.items = newItems;
        this.size = count;
        return oldSize != this.size;
    }

    @Override
    public boolean retainAll(${name}Iterable source)
    {
        int oldSize = this.size();
        final ${name}Set sourceSet = source instanceof ${name}Set ? (${name}Set) source : source.toSet();
        ${name}ArrayList retained = this.select(sourceSet::contains);
        this.size = retained.size;
        this.items = retained.items;
        return oldSize != this.size();
    }

    @Override
    public boolean retainAll(${type}... source)
    {
        return this.retainAll(${name}HashSet.newSetWith(source));
    }

    @Override
    public ${type} removeAtIndex(int index)
    {
        ${type} previous = this.get(index);
        int totalOffset = this.size - index - 1;
        if (totalOffset > 0)
        {
            System.arraycopy(this.items, index + 1, this.items, index, totalOffset);
        }
        --this.size;
        this.items[this.size] = ${zero(type)};
        return previous;
    }

    @Override
    public ${type} set(int index, ${type} element)
    {
        ${type} previous = this.get(index);
        this.items[index] = element;
        return previous;
    }

    @Override
    public ${name}ArrayList with(${type} element)
    {
        this.add(element);
        return this;
    }

    @Override
    public ${name}ArrayList without(${type} element)
    {
        this.remove(element);
        return this;
    }

    @Override
    public ${name}ArrayList withAll(${name}Iterable elements)
    {
        this.addAll(elements.toArray());
        return this;
    }

    @Override
    public ${name}ArrayList withoutAll(${name}Iterable elements)
    {
        this.removeAll(elements);
        return this;
    }

    public ${name}ArrayList with(${type} element1, ${type} element2)
    {
        this.add(element1);
        this.add(element2);
        return this;
    }

    public ${name}ArrayList with(${type} element1, ${type} element2, ${type} element3)
    {
        this.add(element1);
        this.add(element2);
        this.add(element3);
        return this;
    }

    public ${name}ArrayList with(${type} element1, ${type} element2, ${type} element3, ${type}... elements)
    {
        this.add(element1);
        this.add(element2);
        this.add(element3);
        return this.withArrayCopy(elements, 0, elements.length);
    }

    private ${name}ArrayList withArrayCopy(${type}[] elements, int begin, int length)
    {
        this.ensureCapacity(this.size + length);
        System.arraycopy(elements, begin, this.items, this.size, length);
        this.size += length;
        return this;
    }

    @Override
    public Mutable${name}Iterator ${type}Iterator()
    {
        return new Internal${name}Iterator();
    }

    @Override
    public void forEach(${name}Procedure procedure)
    {
        this.each(procedure);
    }

    /**
     * @since 7.0.
     */
    @Override
    public void each(${name}Procedure procedure)
    {
        for (int i = 0; i < this.size; i++)
        {
            procedure.value(this.items[i]);
        }
    }

    @Override
    public void forEachWithIndex(${name}IntProcedure procedure)
    {
        for (int i = 0; i < this.size; i++)
        {
            procedure.value(this.items[i], i);
        }
    }

    @Override
    public <T> T injectInto(T injectedValue, Object${name}ToObjectFunction<? super T, ? extends T> function)
    {
        T result = injectedValue;
        for (int i = 0; i < this.size; i++)
        {
            result = function.valueOf(result, this.items[i]);
        }
        return result;
    }

    @Override
    public <T> T injectIntoWithIndex(T injectedValue, Object${name}IntToObjectFunction<? super T, ? extends T> function)
    {
        T result = injectedValue;
        for (int i = 0; i < this.size; i++)
        {
            result = function.valueOf(result, this.items[i], i);
        }
        return result;
    }

    @Override
    public RichIterable<${name}Iterable> chunk(int size)
    {
        if (size <= 0)
        {
            throw new IllegalArgumentException("Size for groups must be positive but was: " + size);
        }
        MutableList<${name}Iterable> result = Lists.mutable.empty();
        if (this.notEmpty())
        {
            if (this.size() <= size)
            {
                result.add(${name}Lists.mutable.withAll(this));
            }
            else
            {
                ${name}Iterator iterator = this.${type}Iterator();
                while (iterator.hasNext())
                {
                    Mutable${name}List batch = ${name}Lists.mutable.empty();
                    for (int i = 0; i < size && iterator.hasNext(); i++)
                    {
                        batch.add(iterator.next());
                    }
                    result.add(batch);
                }
            }
        }
        return result;
    }

    @Override
    public int count(${name}Predicate predicate)
    {
        int count = 0;
        for (int i = 0; i < this.size; i++)
        {
            if (predicate.accept(this.items[i]))
            {
                count++;
            }
        }
        return count;
    }

    @Override
    public boolean anySatisfy(${name}Predicate predicate)
    {
        for (int i = 0; i < this.size; i++)
        {
            if (predicate.accept(this.items[i]))
            {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean allSatisfy(${name}Predicate predicate)
    {
        for (int i = 0; i < this.size; i++)
        {
            if (!predicate.accept(this.items[i]))
            {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean noneSatisfy(${name}Predicate predicate)
    {
        return !this.anySatisfy(predicate);
    }

    @Override
    public ${name}ArrayList select(${name}Predicate predicate)
    {
        return this.select(predicate, new ${name}ArrayList());
    }

    /**
     * @since 8.1.
     */
    @Override
    public <R extends Mutable${name}Collection> R select(${name}Predicate predicate, R target)
    {
        for (int i = 0; i < this.size; i++)
        {
            ${type} item = this.items[i];
            if (predicate.accept(item))
            {
                target.add(item);
            }
        }
        return target;
    }

    @Override
    public ${name}ArrayList reject(${name}Predicate predicate)
    {
        return this.reject(predicate, new ${name}ArrayList());
    }

    /**
     * @since 8.1.
     */
    @Override
    public <R extends Mutable${name}Collection> R reject(${name}Predicate predicate, R target)
    {
        for (int i = 0; i < this.size; i++)
        {
            ${type} item = this.items[i];
            if (!predicate.accept(item))
            {
                target.add(item);
            }
        }
        return target;
    }

    @Override
    public ${type} detectIfNone(${name}Predicate predicate, ${type} ifNone)
    {
        for (int i = 0; i < this.size; i++)
        {
            ${type} item = this.items[i];
            if (predicate.accept(item))
            {
                return item;
            }
        }
        return ifNone;
    }

    @Override
    public <V> MutableList<V> collect(${name}ToObjectFunction<? extends V> function)
    {
        return this.collect(function, FastList.newList(this.size));
    }

    /**
     * @since 8.1.
     */
    @Override
    public <V, R extends Collection<V>> R collect(${name}ToObjectFunction<? extends V> function, R target)
    {
        for (int i = 0; i < this.size; i++)
        {
            target.add(function.valueOf(this.items[i]));
        }
        return target;
    }

    @Override
    public ${type} max()
    {
        if (this.isEmpty())
        {
            throw new NoSuchElementException();
        }
        ${type} max = this.items[0];
        for (int i = 1; i < this.size; i++)
        {
            ${type} value = this.items[i];
            if (${lessThan(type,"max", "value")})
            {
                max = value;
            }
        }
        return max;
    }

    @Override
    public ${type} min()
    {
        if (this.isEmpty())
        {
            throw new NoSuchElementException();
        }
        ${type} min = this.items[0];
        for (int i = 1; i < this.size; i++)
        {
            ${type} value = this.items[i];
            if (${lessThan(type,"value", "min")})
            {
                min = value;
            }
        }
        return min;
    }

    @Override
    """+(if(prim.isFloatingPoint)"""public ${wideType(type)} sum()
{
    ${wideType(type)} result = ${wideZero(type)};
    ${wideType(type)} compensation = ${wideZero(type)};
    for (int i = 0; i < this.size; i++)
    {
        ${wideType(type)} adjustedValue = this.items[i] - compensation;
        ${wideType(type)} nextSum = result + adjustedValue;
        compensation = nextSum - result - adjustedValue;
        result = nextSum;
    }
    return result;
}

    """ else """public ${wideType(type)} sum()
{
    ${wideType(type)} result = ${wideZero(type)};
    for (int i = 0; i < this.size; i++)
    {
        result += this.items[i];
    }
    return result;
}

    """)+"""

    @Override
    public ${wideType(type)} dotProduct(${name}List list)
    {
        if (this.size != list.size())
        {
            throw new IllegalArgumentException("Lists used in dotProduct must be the same size");
        }
        ${wideType(type)} sum = ${wideZero(type)};
        for (int i = 0; i < this.size; i++)
        {
            sum += ${castWideType(type)}this.items[i] * list.get(i);
        }
        return sum;
    }

    @Override
    public ${type}[] toArray()
    {
        ${type}[] newItems = new ${type}[this.size];
        System.arraycopy(this.items, 0, newItems, 0, this.size);
        return newItems;
    }

    @Override
    public boolean equals(Object otherList)
    {
        if (otherList == this)
        {
            return true;
        }
        if (!(otherList instanceof ${name}List))
        {
            return false;
        }
        ${name}List list = (${name}List) otherList;
        if (this.size != list.size())
        {
            return false;
        }
        for (int i = 0; i < this.size; i++)
        {
            if (${notEquals(type,"this.items[i]", "list.get(i)")})
            {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode()
    {
        int hashCode = 1;
        for (int i = 0; i < this.size; i++)
        {
            ${type} item = this.items[i];
            hashCode = 31 * hashCode + ${hashCode(type,"item")};
        }
        return hashCode;
    }

    @Override
    public void appendString(
            Appendable appendable,
            String start,
            String separator,
            String end)
    {
        try
        {
            appendable.append(start);
            for (int i = 0; i < this.size; i++)
            {
                if (i > 0)
                {
                    appendable.append(separator);
                }
                ${type} value = this.items[i];
                appendable.append(String.valueOf(value));
            }
            appendable.append(end);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Mutable${name}List asUnmodifiable()
    {
        return new Unmodifiable${name}List(this);
    }

    @Override
    public Mutable${name}List asSynchronized()
    {
        return new Synchronized${name}List(this);
    }

    @Override
    public Immutable${name}List toImmutable()
    {
        if (this.size == 0)
        {
            return ${name}Lists.immutable.empty();
        }
        if (this.size == 1)
        {
            return ${name}Lists.immutable.with(this.items[0]);
        }
        return ${name}Lists.immutable.with(this.toArray());
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException
    {
        out.writeInt(this.size);
        for (int i = 0; i < this.size; i++)
        {
            out.write${name}(this.items[i]);
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException
    {
        this.size = in.readInt();
        this.items = new ${type}[this.size];
        for (int i = 0; i < this.size; i++)
        {
            this.items[i] = in.read${name}();
        }
    }

    @Override
    public Lazy${name}Iterable asReversed()
    {
        return Reverse${name}Iterable.adapt(this);
    }

    @Override
    public ${name}ArrayList reverseThis()
    {
        int endIndex = this.size - 1;
        for (int i = 0; i < this.size / 2; i++)
        {
            ${type} tempSwapValue = this.items[i];
            this.items[i] = this.items[endIndex - i];
            this.items[endIndex - i] = tempSwapValue;
        }
        return this;
    }

    @Override
    public ${name}ArrayList sortThis()
    {
        Arrays.sort(this.items, 0, this.size);
        return this;
    }

    @Override
    public ${name}ArrayList toReversed()
    {
        return ${name}ArrayList.newList(this.asReversed());
    }

    @Override
    public int binarySearch(${type} value)
    {
        return Arrays.binarySearch(this.items, 0, this.size, value);
    }

    @Override
    public Mutable${name}List distinct()
    {
        ${name}ArrayList target = new ${name}ArrayList();
        Mutable${name}Set seenSoFar = new ${name}HashSet(this.size());

        for (int i = 0; i < this.size; i++)
        {
            ${type} each = this.items[i];
            if (seenSoFar.add(each))
            {
                target.add(each);
            }
        }
        return target;
    }

    @Override
    public Mutable${name}List subList(int fromIndex, int toIndex)
    {
        throw new UnsupportedOperationException("subList not yet implemented!");
    }

    /**
     * @since 9.1.
     */
    @Override
    public MutableList<${name}${name}Pair> zip${name}(${name}Iterable iterable)
    {
        int size = this.size();
        int otherSize = iterable.size();
        MutableList<${name}${name}Pair> target = Lists.mutable.withInitialCapacity(Math.min(size, otherSize));
        ${name}Iterator iterator = iterable.${type}Iterator();
        for (int i = 0; i < size && i < otherSize; i++)
        {
            target.add(PrimitiveTuples.pair(this.items[i], iterator.next()));
        }
        return target;
    }

    /**
     * Creates a new empty ${name}ArrayList.
     *
     * @since 9.2.
     */
    public ${name}ArrayList newEmpty()
    {
        return new ${name}ArrayList();
    }

    /**
     * @since 9.1.
     */
    @Override
    public <T> MutableList<${name}ObjectPair<T>> zip(Iterable<T> iterable)
    {
        int size = this.size();
        int otherSize = Iterate.sizeOf(iterable);
        MutableList<${name}ObjectPair<T>> target = Lists.mutable.withInitialCapacity(Math.min(size, otherSize));
        Iterator<T> iterator = iterable.iterator();
        for (int i = 0; i < size && iterator.hasNext(); i++)
        {
            target.add(PrimitiveTuples.pair(this.items[i], iterator.next()));
        }
        return target;
    }

    private class Internal${name}Iterator implements Mutable${name}Iterator
    {
        /**
         * Index of element to be returned by subsequent call to next.
         */
        private int currentIndex;
        private int lastIndex = -1;

        @Override
        public boolean hasNext()
        {
            return this.currentIndex != ${name}ArrayList.this.size();
        }

        @Override
        public ${type} next()
        {
            if (!this.hasNext())
            {
                throw new NoSuchElementException();
            }
            ${type} next = ${name}ArrayList.this.items[this.currentIndex];
            this.lastIndex = this.currentIndex++;
            return next;
        }

        @Override
        public void remove()
        {
            if (this.lastIndex == -1)
            {
                throw new IllegalStateException();
            }
            ${name}ArrayList.this.removeAtIndex(this.lastIndex);
            this.currentIndex--;
            this.lastIndex = -1;
        }
    }
}


"""
