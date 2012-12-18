package org.mapdb;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.junit.rules.ErrorCollector;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * Parametrized tests generator
 * idea comes from
 * https://github.com/schauder/parameterizedTestsWithRules
 *
 */
public class Gen<T> implements TestRule {

    private Object[] currentValue = new Object[1];

    private final AccessibleErrorCollector errorCollector = new AccessibleErrorCollector();

    private final List<T> values;

    public Gen(T v1, T... vv) {
        values = new ArrayList();
        values.add(v1);
        for(T t:vv){
            values.add(t);
        }
    }

    public T get() {
        return (T) currentValue[0];
    }

    @Override
    public Statement apply(Statement test, Description description) {
        return new RepeatedStatement<T>(test, new SyncingIterable<T>(values,
                currentValue), errorCollector);
    }

    static class RepeatedStatement<T> extends Statement {

        private final Statement test;
        private final Iterable<T> values;
        private final AccessibleErrorCollector errorCollector;

        public RepeatedStatement(Statement test, Iterable<T> values,
                                 AccessibleErrorCollector errorCollector) {
            this.test = test;
            this.values = values;
            this.errorCollector = errorCollector;
        }

        @Override
        public void evaluate() throws Throwable {
            for (T v : values) {
                try {
                    test.evaluate();
                } catch (Throwable t) {
                    errorCollector.addError(new Error("For value: "
                            + v, t));
                }
            }
            errorCollector.verify();
        }
    }
    static class AccessibleErrorCollector extends ErrorCollector {

        @Override
        public void verify() throws Throwable {
            super.verify();
        }

    }

    static class SyncingIterable<T> implements Iterable<T> {

        private final Iterable<T> values;
        private Object[] valueContainer;

        public SyncingIterable(Iterable<T> values,
                               Object[] valueContainer) {
            this.values = values;
            this.valueContainer = valueContainer;
        }

        @Override
        public Iterator<T> iterator() {
            return new Iterator<T>() {
                private final Iterator<T> delegate = values.iterator();

                @Override
                public boolean hasNext() {
                    return delegate.hasNext();
                }

                @Override
                public T next() {
                    T next = delegate.next();
                    valueContainer[0] = next;
                    return next;
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException(
                            "Can't remove from this iterator");
                }
            };
        }
    }
}

