/*
 * Written by Doug Lea with assistance from members of JCP JSR-166
 * Expert Group and released to the public domain, as explained at
 * http://creativecommons.org/publicdomain/zero/1.0/
 * Other contributors include Andrew Wright, Jeffrey Hayes,
 * Pat Fisher, Mike Judd.
 */
package org.mapdb.util;

import org.mapdb.tree.jsr166Tests.JSR166TestCase;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

//TODO test skipped
abstract
public class ScheduledExecutorServiceWrapperTest extends JSR166TestCase {

    
    
    /**
     * execute successfully executes a runnable
     */
    public void testExecute() throws InterruptedException {
        final ScheduledExecutorService p = new ScheduledExecutorServiceWrapper(Executors.newScheduledThreadPool(1));
        try (PoolCleaner cleaner = cleaner(p)) {
            final CountDownLatch done = new CountDownLatch(1);
            final Runnable task = new CheckedRunnable() {
                public void realRun() { done.countDown(); }};
            p.execute(task);
            assertTrue(done.await(LONG_DELAY_MS, MILLISECONDS));
        }
    }

    /**
     * delayed schedule of callable successfully executes after delay
     */
    public void testSchedule1() throws Exception {
        final ScheduledExecutorService p = new ScheduledExecutorServiceWrapper(Executors.newScheduledThreadPool(1));
        try (PoolCleaner cleaner = cleaner(p)) {
            final long startTime = System.nanoTime();
            final CountDownLatch done = new CountDownLatch(1);
            Callable task = new CheckedCallable<Boolean>() {
                public Boolean realCall() {
                    done.countDown();
                    assertTrue(millisElapsedSince(startTime) >= timeoutMillis());
                    return Boolean.TRUE;
                }};
            Future f = p.schedule(task, timeoutMillis(), MILLISECONDS);
            assertSame(Boolean.TRUE, f.get());
            assertTrue(millisElapsedSince(startTime) >= timeoutMillis());
            assertTrue(done.await(0L, MILLISECONDS));
        }
    }

    /**
     * delayed schedule of runnable successfully executes after delay
     */
    public void testSchedule3() throws Exception {
        final ScheduledExecutorService p = new ScheduledExecutorServiceWrapper(Executors.newScheduledThreadPool(1));
        try (PoolCleaner cleaner = cleaner(p)) {
            final long startTime = System.nanoTime();
            final CountDownLatch done = new CountDownLatch(1);
            Runnable task = new CheckedRunnable() {
                public void realRun() {
                    done.countDown();
                    assertTrue(millisElapsedSince(startTime) >= timeoutMillis());
                }};
            Future f = p.schedule(task, timeoutMillis(), MILLISECONDS);
            await(done);
            assertNull(f.get(LONG_DELAY_MS, MILLISECONDS));
            assertTrue(millisElapsedSince(startTime) >= timeoutMillis());
        }
    }

    /**
     * scheduleAtFixedRate executes runnable after given initial delay
     */
    public void testSchedule4() throws Exception {
        final ScheduledExecutorService p = new ScheduledExecutorServiceWrapper(Executors.newScheduledThreadPool(1));
        try (PoolCleaner cleaner = cleaner(p)) {
            final long startTime = System.nanoTime();
            final CountDownLatch done = new CountDownLatch(1);
            Runnable task = new CheckedRunnable() {
                public void realRun() {
                    done.countDown();
                    assertTrue(millisElapsedSince(startTime) >= timeoutMillis());
                }};
            ScheduledFuture f =
                p.scheduleAtFixedRate(task, timeoutMillis(),
                                      LONG_DELAY_MS, MILLISECONDS);
            await(done);
            assertTrue(millisElapsedSince(startTime) >= timeoutMillis());
            f.cancel(true);
        }
    }

    /**
     * scheduleWithFixedDelay executes runnable after given initial delay
     */
    public void testSchedule5() throws Exception {
        final ScheduledExecutorService p = new ScheduledExecutorServiceWrapper(Executors.newScheduledThreadPool(1));
        try (PoolCleaner cleaner = cleaner(p)) {
            final long startTime = System.nanoTime();
            final CountDownLatch done = new CountDownLatch(1);
            Runnable task = new CheckedRunnable() {
                public void realRun() {
                    done.countDown();
                    assertTrue(millisElapsedSince(startTime) >= timeoutMillis());
                }};
            ScheduledFuture f =
                p.scheduleWithFixedDelay(task, timeoutMillis(),
                                         LONG_DELAY_MS, MILLISECONDS);
            await(done);
            assertTrue(millisElapsedSince(startTime) >= timeoutMillis());
            f.cancel(true);
        }
    }

    static class RunnableCounter implements Runnable {
        AtomicInteger count = new AtomicInteger(0);
        public void run() { count.getAndIncrement(); }
    }

    /**
     * scheduleAtFixedRate executes series of tasks at given rate
     */
    public void testFixedRateSequence() throws InterruptedException {
        final ScheduledExecutorService p = new ScheduledExecutorServiceWrapper(Executors.newScheduledThreadPool(1));
        try (PoolCleaner cleaner = cleaner(p)) {
            for (int delay = 1; delay <= LONG_DELAY_MS; delay *= 3) {
                long startTime = System.nanoTime();
                int cycles = 10;
                final CountDownLatch done = new CountDownLatch(cycles);
                Runnable task = new CheckedRunnable() {
                    public void realRun() { done.countDown(); }};
                ScheduledFuture h =
                    p.scheduleAtFixedRate(task, 0, delay, MILLISECONDS);
                await(done);
                h.cancel(true);
                double normalizedTime =
                    (double) millisElapsedSince(startTime) / delay;
                if (normalizedTime >= cycles - 1 &&
                    normalizedTime <= cycles)
                    return;
            }
            fail("unexpected execution rate");
        }
    }

    /**
     * scheduleWithFixedDelay executes series of tasks with given period
     */
    public void testFixedDelaySequence() throws InterruptedException {
        final ScheduledExecutorService p = new ScheduledExecutorServiceWrapper(Executors.newScheduledThreadPool(1));
        try (PoolCleaner cleaner = cleaner(p)) {
            for (int delay = 1; delay <= LONG_DELAY_MS; delay *= 3) {
                long startTime = System.nanoTime();
                int cycles = 10;
                final CountDownLatch done = new CountDownLatch(cycles);
                Runnable task = new CheckedRunnable() {
                    public void realRun() { done.countDown(); }};
                ScheduledFuture h =
                    p.scheduleWithFixedDelay(task, 0, delay, MILLISECONDS);
                await(done);
                h.cancel(true);
                double normalizedTime =
                    (double) millisElapsedSince(startTime) / delay;
                if (normalizedTime >= cycles - 1 &&
                    normalizedTime <= cycles)
                    return;
            }
            fail("unexpected execution rate");
        }
    }

    /**
     * execute(null) throws NPE
     */
    public void testExecuteNull() throws InterruptedException {
        final ScheduledExecutorService p = new ScheduledExecutorServiceWrapper(Executors.newScheduledThreadPool(1));
        try (PoolCleaner cleaner = cleaner(p)) {
            try {
                p.execute(null);
                shouldThrow();
            } catch (IllegalArgumentException success) {}
        }
    }

    /**
     * schedule(null) throws NPE
     */
    public void testScheduleNull() throws InterruptedException {
        final ScheduledExecutorService p = new ScheduledExecutorServiceWrapper(Executors.newScheduledThreadPool(1));
        try (PoolCleaner cleaner = cleaner(p)) {
            try {
                TrackedCallable callable = null;
                Future f = p.schedule(callable, SHORT_DELAY_MS, MILLISECONDS);
                shouldThrow();
            } catch (IllegalArgumentException success) {}
        }
    }

    /**
     * execute throws RejectedExecutionException if shutdown
     */
    public void testSchedule1_RejectedExecutionException() throws InterruptedException {
        final ScheduledExecutorService p = new ScheduledExecutorServiceWrapper(Executors.newScheduledThreadPool(1));
        try (PoolCleaner cleaner = cleaner(p)) {
            try {
                p.shutdown();
                p.schedule(new NoOpRunnable(),
                           MEDIUM_DELAY_MS, MILLISECONDS);
                shouldThrow();
            } catch (RejectedExecutionException success) {
            } catch (SecurityException ok) {}
        }
    }

    /**
     * schedule throws RejectedExecutionException if shutdown
     */
    public void testSchedule2_RejectedExecutionException() throws InterruptedException {
        final ScheduledExecutorService p = new ScheduledExecutorServiceWrapper(Executors.newScheduledThreadPool(1));
        try (PoolCleaner cleaner = cleaner(p)) {
            try {
                p.shutdown();
                p.schedule(new NoOpCallable(),
                           MEDIUM_DELAY_MS, MILLISECONDS);
                shouldThrow();
            } catch (RejectedExecutionException success) {
            } catch (SecurityException ok) {}
        }
    }

    /**
     * schedule callable throws RejectedExecutionException if shutdown
     */
    public void testSchedule3_RejectedExecutionException() throws InterruptedException {
        final ScheduledExecutorService p = new ScheduledExecutorServiceWrapper(Executors.newScheduledThreadPool(1));
        try (PoolCleaner cleaner = cleaner(p)) {
            try {
                p.shutdown();
                p.schedule(new NoOpCallable(),
                           MEDIUM_DELAY_MS, MILLISECONDS);
                shouldThrow();
            } catch (RejectedExecutionException success) {
            } catch (SecurityException ok) {}
        }
    }

    /**
     * scheduleAtFixedRate throws RejectedExecutionException if shutdown
     */
    public void testScheduleAtFixedRate1_RejectedExecutionException() throws InterruptedException {
        final ScheduledExecutorService p = new ScheduledExecutorServiceWrapper(Executors.newScheduledThreadPool(1));
        try (PoolCleaner cleaner = cleaner(p)) {
            try {
                p.shutdown();
                p.scheduleAtFixedRate(new NoOpRunnable(),
                                      MEDIUM_DELAY_MS, MEDIUM_DELAY_MS, MILLISECONDS);
                shouldThrow();
            } catch (RejectedExecutionException success) {
            } catch (SecurityException ok) {}
        }
    }

    /**
     * scheduleWithFixedDelay throws RejectedExecutionException if shutdown
     */
    public void testScheduleWithFixedDelay1_RejectedExecutionException() throws InterruptedException {
        final ScheduledExecutorService p = new ScheduledExecutorServiceWrapper(Executors.newScheduledThreadPool(1));
        try (PoolCleaner cleaner = cleaner(p)) {
            try {
                p.shutdown();
                p.scheduleWithFixedDelay(new NoOpRunnable(),
                                         MEDIUM_DELAY_MS, MEDIUM_DELAY_MS, MILLISECONDS);
                shouldThrow();
            } catch (RejectedExecutionException success) {
            } catch (SecurityException ok) {}
        }
    }


    /**
     * isShutdown is false before shutdown, true after
     */
    public void testIsShutdown() {

        final ScheduledExecutorService p = new ScheduledExecutorServiceWrapper(Executors.newScheduledThreadPool(1));
        try {
            assertFalse(p.isShutdown());
        }
        finally {
            try { p.shutdown(); } catch (SecurityException ok) { return; }
        }
        assertTrue(p.isShutdown());
    }

    /**
     * isTerminated is false before termination, true after
     */
    public void testIsTerminated() throws InterruptedException {
        final ScheduledExecutorService p = new ScheduledExecutorServiceWrapper(Executors.newScheduledThreadPool(1));
        try (PoolCleaner cleaner = cleaner(p)) {
            final CountDownLatch threadStarted = new CountDownLatch(1);
            final CountDownLatch done = new CountDownLatch(1);
            assertFalse(p.isTerminated());
            p.execute(new CheckedRunnable() {
                public void realRun() throws InterruptedException {
                    assertFalse(p.isTerminated());
                    threadStarted.countDown();
                    await(done);
                }});
            await(threadStarted);
//            assertFalse(p.isTerminating());
            done.countDown();
            try { p.shutdown(); } catch (SecurityException ok) { return; }
            assertTrue(p.awaitTermination(LONG_DELAY_MS, MILLISECONDS));
            assertTrue(p.isTerminated());
        }
    }


    /**
     * shutdownNow returns a list containing tasks that were not run,
     * and those tasks are drained from the queue
     */
    public void testShutdownNow() throws InterruptedException {
        final int poolSize = 2;
        final int count = 5;
        final AtomicInteger ran = new AtomicInteger(0);
        final ScheduledExecutorService p = new ScheduledExecutorServiceWrapper(Executors.newScheduledThreadPool(poolSize));
        final CountDownLatch threadsStarted = new CountDownLatch(poolSize);
        Runnable waiter = new CheckedRunnable() { public void realRun() {
            threadsStarted.countDown();
            try {
                MILLISECONDS.sleep(2 * LONG_DELAY_MS);
            } catch (InterruptedException success) {}
            ran.getAndIncrement();
        }};
        for (int i = 0; i < count; i++)
            p.execute(waiter);
        await(threadsStarted);
//        assertEquals(poolSize, p.getActiveCount());
//        assertEquals(0, p.getCompletedTaskCount());
        final List<Runnable> queuedTasks;
        try {
            queuedTasks = p.shutdownNow();
        } catch (SecurityException ok) {
            return; // Allowed in case test doesn't have privs
        }
        assertTrue(p.isShutdown());
//        assertTrue(p.getQueue().isEmpty());
//        assertEquals(count - poolSize, queuedTasks.size());
        assertTrue(p.awaitTermination(LONG_DELAY_MS, MILLISECONDS));
        assertTrue(p.isTerminated());
//        assertEquals(poolSize, ran.get());
//        assertEquals(poolSize, p.getCompletedTaskCount());
    }

    /**
     * shutdownNow returns a list containing tasks that were not run,
     * and those tasks are drained from the queue
     */
    public void testShutdownNow_delayedTasks() throws InterruptedException {
        final ScheduledExecutorService p = new ScheduledExecutorServiceWrapper(Executors.newScheduledThreadPool(1));
        List<ScheduledFuture> tasks = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            Runnable r = new NoOpRunnable();
            tasks.add(p.schedule(r, 9, SECONDS));
            tasks.add(p.scheduleAtFixedRate(r, 9, 9, SECONDS));
            tasks.add(p.scheduleWithFixedDelay(r, 9, 9, SECONDS));
        }
//        if (testImplementationDetails)
//            assertEquals(new HashSet(tasks), new HashSet(p.getQueue()));
        final List<Runnable> queuedTasks;
        try {
            queuedTasks = p.shutdownNow();
        } catch (SecurityException ok) {
            return; // Allowed in case test doesn't have privs
        }
        assertTrue(p.isShutdown());
//        assertTrue(p.getQueue().isEmpty());
        if (testImplementationDetails)
            assertEquals(new HashSet(tasks), new HashSet(queuedTasks));
//        assertEquals(tasks.size(), queuedTasks.size());
//        for (ScheduledFuture task : tasks) {
//            assertFalse(task.isDone());
//            assertFalse(task.isCancelled());
//        }
        assertTrue(p.awaitTermination(LONG_DELAY_MS, MILLISECONDS));
        assertTrue(p.isTerminated());
    }

    /**
     * completed submit of callable returns result
     */
    public void testSubmitCallable() throws Exception {
        final ExecutorService e = new ScheduledExecutorServiceWrapper(Executors.newScheduledThreadPool(2));
        try (PoolCleaner cleaner = cleaner(e)) {
            Future<String> future = e.submit(new StringTask());
            String result = future.get();
            assertSame(TEST_STRING, result);
        }
    }

    /**
     * completed submit of runnable returns successfully
     */
    public void testSubmitRunnable() throws Exception {
        final ExecutorService e = new ScheduledExecutorServiceWrapper(Executors.newScheduledThreadPool(2));
        try (PoolCleaner cleaner = cleaner(e)) {
            Future<?> future = e.submit(new NoOpRunnable());
            future.get();
            assertTrue(future.isDone());
        }
    }

    /**
     * completed submit of (runnable, result) returns result
     */
    public void testSubmitRunnable2() throws Exception {
        final ExecutorService e = new ScheduledExecutorServiceWrapper(Executors.newScheduledThreadPool(2));
        try (PoolCleaner cleaner = cleaner(e)) {
            Future<String> future = e.submit(new NoOpRunnable(), TEST_STRING);
            String result = future.get();
            assertSame(TEST_STRING, result);
        }
    }

    /**
     * invokeAny(null) throws NPE
     */
    public void testInvokeAny1() throws Exception {
        final ExecutorService e = new ScheduledExecutorServiceWrapper(Executors.newScheduledThreadPool(2));
        try (PoolCleaner cleaner = cleaner(e)) {
            try {
                e.invokeAny(null);
                shouldThrow();
            } catch (IllegalArgumentException success) {}
        }
    }

    /**
     * invokeAny(empty collection) throws IAE
     */
    public void testInvokeAny2() throws Exception {
        final ExecutorService e = new ScheduledExecutorServiceWrapper(Executors.newScheduledThreadPool(2));
        try (PoolCleaner cleaner = cleaner(e)) {
            try {
                e.invokeAny(new ArrayList<Callable<String>>());
                shouldThrow();
            } catch (IllegalArgumentException success) {}
        }
    }

    /**
     * invokeAny(c) throws NPE if c has null elements
     */
    public void testInvokeAny3() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        final ExecutorService e = new ScheduledExecutorServiceWrapper(Executors.newScheduledThreadPool(2));
        try (PoolCleaner cleaner = cleaner(e)) {
            List<Callable<String>> l = new ArrayList<Callable<String>>();
            l.add(latchAwaitingStringTask(latch));
            l.add(null);
            try {
                e.invokeAny(l);
                shouldThrow();
            } catch (IllegalArgumentException success) {}
            latch.countDown();
        }
    }

    /**
     * invokeAny(c) throws ExecutionException if no task completes
     */
    public void testInvokeAny4() throws Exception {
        final ExecutorService e = new ScheduledExecutorServiceWrapper(Executors.newScheduledThreadPool(2));
        try (PoolCleaner cleaner = cleaner(e)) {
            List<Callable<String>> l = new ArrayList<Callable<String>>();
            l.add(new NPETask());
            try {
                e.invokeAny(l);
                shouldThrow();
            } catch (ExecutionException success) {
                assertTrue(success.getCause() instanceof NullPointerException);
            }
        }
    }

    /**
     * invokeAny(c) returns result of some task
     */
    public void testInvokeAny5() throws Exception {
        final ExecutorService e = new ScheduledExecutorServiceWrapper(Executors.newScheduledThreadPool(2));
        try (PoolCleaner cleaner = cleaner(e)) {
            List<Callable<String>> l = new ArrayList<Callable<String>>();
            l.add(new StringTask());
            l.add(new StringTask());
            String result = e.invokeAny(l);
            assertSame(TEST_STRING, result);
        }
    }

    /**
     * invokeAll(null) throws NPE
     */
    public void testInvokeAll1() throws Exception {
        final ExecutorService e = new ScheduledExecutorServiceWrapper(Executors.newScheduledThreadPool(2));
        try (PoolCleaner cleaner = cleaner(e)) {
            try {
                e.invokeAll(null);
                shouldThrow();
            } catch (IllegalArgumentException success) {}
        }
    }

    /**
     * invokeAll(empty collection) returns empty collection
     */
    public void testInvokeAll2() throws Exception {
        final ExecutorService e = new ScheduledExecutorServiceWrapper(Executors.newScheduledThreadPool(2));
        try (PoolCleaner cleaner = cleaner(e)) {
            List<Future<String>> r = e.invokeAll(new ArrayList<Callable<String>>());
            assertTrue(r.isEmpty());
        }
    }

    /**
     * invokeAll(c) throws NPE if c has null elements
     */
    public void testInvokeAll3() throws Exception {
        final ExecutorService e = new ScheduledExecutorServiceWrapper(Executors.newScheduledThreadPool(2));
        try (PoolCleaner cleaner = cleaner(e)) {
            List<Callable<String>> l = new ArrayList<Callable<String>>();
            l.add(new StringTask());
            l.add(null);
            try {
                e.invokeAll(l);
                shouldThrow();
            } catch (IllegalArgumentException success) {}
        }
    }

    /**
     * get of invokeAll(c) throws exception on failed task
     */
    public void testInvokeAll4() throws Exception {
        final ExecutorService e = new ScheduledExecutorServiceWrapper(Executors.newScheduledThreadPool(2));
        try (PoolCleaner cleaner = cleaner(e)) {
            List<Callable<String>> l = new ArrayList<Callable<String>>();
            l.add(new NPETask());
            List<Future<String>> futures = e.invokeAll(l);
            assertEquals(1, futures.size());
            try {
                futures.get(0).get();
                shouldThrow();
            } catch (ExecutionException success) {
                assertTrue(success.getCause() instanceof NullPointerException);
            }
        }
    }

    /**
     * invokeAll(c) returns results of all completed tasks
     */
    public void testInvokeAll5() throws Exception {
        final ExecutorService e = new ScheduledExecutorServiceWrapper(Executors.newScheduledThreadPool(2));
        try (PoolCleaner cleaner = cleaner(e)) {
            List<Callable<String>> l = new ArrayList<Callable<String>>();
            l.add(new StringTask());
            l.add(new StringTask());
            List<Future<String>> futures = e.invokeAll(l);
            assertEquals(2, futures.size());
            for (Future<String> future : futures)
                assertSame(TEST_STRING, future.get());
        }
    }

    /**
     * timed invokeAny(null) throws NPE
     */
    public void testTimedInvokeAny1() throws Exception {
        final ExecutorService e = new ScheduledExecutorServiceWrapper(Executors.newScheduledThreadPool(2));
        try (PoolCleaner cleaner = cleaner(e)) {
            try {
                e.invokeAny(null, MEDIUM_DELAY_MS, MILLISECONDS);
                shouldThrow();
            } catch (IllegalArgumentException success) {}
        }
    }

    /**
     * timed invokeAny(,,null) throws NPE
     */
    public void testTimedInvokeAnyNullTimeUnit() throws Exception {
        final ExecutorService e = new ScheduledExecutorServiceWrapper(Executors.newScheduledThreadPool(2));
        try (PoolCleaner cleaner = cleaner(e)) {
            List<Callable<String>> l = new ArrayList<Callable<String>>();
            l.add(new StringTask());
            try {
                e.invokeAny(l, MEDIUM_DELAY_MS, null);
                shouldThrow();
            } catch (IllegalArgumentException success) {}
        }
    }

    /**
     * timed invokeAny(empty collection) throws IAE
     */
    public void testTimedInvokeAny2() throws Exception {
        final ExecutorService e = new ScheduledExecutorServiceWrapper(Executors.newScheduledThreadPool(2));
        try (PoolCleaner cleaner = cleaner(e)) {
            try {
                e.invokeAny(new ArrayList<Callable<String>>(), MEDIUM_DELAY_MS, MILLISECONDS);
                shouldThrow();
            } catch (IllegalArgumentException success) {}
        }
    }

    /**
     * timed invokeAny(c) throws NPE if c has null elements
     */
    public void testTimedInvokeAny3() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        final ExecutorService e = new ScheduledExecutorServiceWrapper(Executors.newScheduledThreadPool(2));
        try (PoolCleaner cleaner = cleaner(e)) {
            List<Callable<String>> l = new ArrayList<Callable<String>>();
            l.add(latchAwaitingStringTask(latch));
            l.add(null);
            try {
                e.invokeAny(l, MEDIUM_DELAY_MS, MILLISECONDS);
                shouldThrow();
            } catch (IllegalArgumentException success) {}
            latch.countDown();
        }
    }

    /**
     * timed invokeAny(c) throws ExecutionException if no task completes
     */
    public void testTimedInvokeAny4() throws Exception {
        final ExecutorService e = new ScheduledExecutorServiceWrapper(Executors.newScheduledThreadPool(2));
        try (PoolCleaner cleaner = cleaner(e)) {
            long startTime = System.nanoTime();
            List<Callable<String>> l = new ArrayList<Callable<String>>();
            l.add(new NPETask());
            try {
                e.invokeAny(l, LONG_DELAY_MS, MILLISECONDS);
                shouldThrow();
            } catch (ExecutionException success) {
                assertTrue(success.getCause() instanceof NullPointerException);
            }
            assertTrue(millisElapsedSince(startTime) < LONG_DELAY_MS);
        }
    }

    /**
     * timed invokeAny(c) returns result of some task
     */
    public void testTimedInvokeAny5() throws Exception {
        final ExecutorService e = new ScheduledExecutorServiceWrapper(Executors.newScheduledThreadPool(2));
        try (PoolCleaner cleaner = cleaner(e)) {
            long startTime = System.nanoTime();
            List<Callable<String>> l = new ArrayList<Callable<String>>();
            l.add(new StringTask());
            l.add(new StringTask());
            String result = e.invokeAny(l, LONG_DELAY_MS, MILLISECONDS);
            assertSame(TEST_STRING, result);
            assertTrue(millisElapsedSince(startTime) < LONG_DELAY_MS);
        }
    }

    /**
     * timed invokeAll(null) throws NPE
     */
    public void testTimedInvokeAll1() throws Exception {
        final ExecutorService e = new ScheduledExecutorServiceWrapper(Executors.newScheduledThreadPool(2));
        try (PoolCleaner cleaner = cleaner(e)) {
            try {
                e.invokeAll(null, MEDIUM_DELAY_MS, MILLISECONDS);
                shouldThrow();
            } catch (IllegalArgumentException success) {}
        }
    }

    /**
     * timed invokeAll(,,null) throws NPE
     */
    public void testTimedInvokeAllNullTimeUnit() throws Exception {
        final ExecutorService e = new ScheduledExecutorServiceWrapper(Executors.newScheduledThreadPool(2));
        try (PoolCleaner cleaner = cleaner(e)) {
            List<Callable<String>> l = new ArrayList<Callable<String>>();
            l.add(new StringTask());
            try {
                e.invokeAll(l, MEDIUM_DELAY_MS, null);
                shouldThrow();
            } catch (IllegalArgumentException success) {}
        }
    }

    /**
     * timed invokeAll(empty collection) returns empty collection
     */
    public void testTimedInvokeAll2() throws Exception {
        final ExecutorService e = new ScheduledExecutorServiceWrapper(Executors.newScheduledThreadPool(2));
        try (PoolCleaner cleaner = cleaner(e)) {
            List<Future<String>> r = e.invokeAll(new ArrayList<Callable<String>>(),
                                                 MEDIUM_DELAY_MS, MILLISECONDS);
            assertTrue(r.isEmpty());
        }
    }

    /**
     * timed invokeAll(c) throws NPE if c has null elements
     */
    public void testTimedInvokeAll3() throws Exception {
        final ExecutorService e = new ScheduledExecutorServiceWrapper(Executors.newScheduledThreadPool(2));
        try (PoolCleaner cleaner = cleaner(e)) {
            List<Callable<String>> l = new ArrayList<Callable<String>>();
            l.add(new StringTask());
            l.add(null);
            try {
                e.invokeAll(l, MEDIUM_DELAY_MS, MILLISECONDS);
                shouldThrow();
            } catch (IllegalArgumentException success) {}
        }
    }

    /**
     * get of element of invokeAll(c) throws exception on failed task
     */
    public void testTimedInvokeAll4() throws Exception {
        final ExecutorService e = new ScheduledExecutorServiceWrapper(Executors.newScheduledThreadPool(2));
        try (PoolCleaner cleaner = cleaner(e)) {
            List<Callable<String>> l = new ArrayList<Callable<String>>();
            l.add(new NPETask());
            List<Future<String>> futures =
                e.invokeAll(l, LONG_DELAY_MS, MILLISECONDS);
            assertEquals(1, futures.size());
            try {
                futures.get(0).get();
                shouldThrow();
            } catch (ExecutionException success) {
                assertTrue(success.getCause() instanceof NullPointerException);
            }
        }
    }

    /**
     * timed invokeAll(c) returns results of all completed tasks
     */
    public void testTimedInvokeAll5() throws Exception {
        final ExecutorService e = new ScheduledExecutorServiceWrapper(Executors.newScheduledThreadPool(2));
        try (PoolCleaner cleaner = cleaner(e)) {
            List<Callable<String>> l = new ArrayList<Callable<String>>();
            l.add(new StringTask());
            l.add(new StringTask());
            List<Future<String>> futures =
                e.invokeAll(l, LONG_DELAY_MS, MILLISECONDS);
            assertEquals(2, futures.size());
            for (Future<String> future : futures)
                assertSame(TEST_STRING, future.get());
        }
    }

    /**
     * timed invokeAll(c) cancels tasks not completed by timeout
     */
    public void testTimedInvokeAll6() throws Exception {
        for (long timeout = timeoutMillis();;) {
            final CountDownLatch done = new CountDownLatch(1);
            final Callable<String> waiter = new CheckedCallable<String>() {
                public String realCall() {
                    try { done.await(LONG_DELAY_MS, MILLISECONDS); }
                    catch (InterruptedException ok) {}
                    return "1"; }};
            final ExecutorService p = new ScheduledExecutorServiceWrapper(Executors.newScheduledThreadPool(2));
            try (PoolCleaner cleaner = cleaner(p, done)) {
                List<Callable<String>> tasks = new ArrayList<>();
                tasks.add(new StringTask("0"));
                tasks.add(waiter);
                tasks.add(new StringTask("2"));
                long startTime = System.nanoTime();
                List<Future<String>> futures =
                    p.invokeAll(tasks, timeout, MILLISECONDS);
                assertEquals(tasks.size(), futures.size());
                assertTrue(millisElapsedSince(startTime) >= timeout);
                for (Future future : futures)
                    assertTrue(future.isDone());
                assertTrue(futures.get(1).isCancelled());
                try {
                    assertEquals("0", futures.get(0).get());
                    assertEquals("2", futures.get(2).get());
                    break;
                } catch (CancellationException retryWithLongerTimeout) {
                    timeout *= 2;
                    if (timeout >= LONG_DELAY_MS / 2)
                        fail("expected exactly one task to be cancelled");
                }
            }
        }
    }


}
