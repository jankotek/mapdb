package org.mapdb.util

import com.google.common.collect.MapMaker
import java.util.*
import java.util.concurrent.*
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Wraps `ScheduledExecutorService` and provides independent shutdown.
 */
class ScheduledExecutorServiceWrapper
    (protected val s:ScheduledExecutorService)
    :ScheduledExecutorService{

    protected val isShutdown = AtomicBoolean(false)

    protected val tasks:MutableMap<Future<*>, Any> = MapMaker().weakValues().makeMap()

    @Synchronized override fun schedule(command: Runnable, delay: Long, unit: TimeUnit): ScheduledFuture<*> {
        check()
        val ret = s.schedule(command, delay, unit)
        tasks.put(ret, command);
        return ret;
    }

    @Synchronized override fun <V : Any?> schedule(callable: Callable<V>, delay: Long, unit: TimeUnit): ScheduledFuture<V> {
        check()
        val ret = s.schedule(callable, delay, unit)
        tasks.put(ret,callable)
        return ret;    }

    @Synchronized override fun <T : Any?> submit(task: Callable<T>): Future<T> {
        check()
        val ret = s.submit(task)
        tasks.put(ret, task)
        return ret;
    }

    @Synchronized override fun <T : Any?> submit(task: Runnable, result: T): Future<T> {
        check()
        val ret = s.submit(task, result)
        tasks.put(ret, task)
        return ret;
    }

    @Synchronized override fun submit(task: Runnable): Future<*> {
        check()
        val ret = s.submit(task)
        tasks.put(ret,task)
        return ret;
    }


    @Synchronized override fun execute(command: Runnable) {
        check()
        val ret = submit(command)
        tasks.put(ret,command);
    }


    @Synchronized override fun scheduleAtFixedRate(command: Runnable, initialDelay: Long, period: Long, unit: TimeUnit): ScheduledFuture<*> {
        check()
        val ret = s.scheduleAtFixedRate(command, initialDelay, period, unit)
        tasks.put(ret, command)
        return ret;
    }

    @Synchronized override fun scheduleWithFixedDelay(command: Runnable, initialDelay: Long, delay: Long, unit: TimeUnit): ScheduledFuture<*> {
        check()
        val ret = s.scheduleWithFixedDelay(command, initialDelay, delay, unit)
        tasks.put(ret,command)
        return ret;
    }

    override fun <T : Any?> invokeAll(tasks: MutableCollection<out Callable<T>>): MutableList<Future<T>> {
        return invokeAll(tasks, -1, TimeUnit.MILLISECONDS)
    }

    override fun <T : Any?> invokeAll(tasks: MutableCollection<out Callable<T>>, timeout: Long, unit: TimeUnit): MutableList<Future<T>> {
        if(tasks.isEmpty())
            return ArrayList()

        val endTime = if(timeout<0) Long.MAX_VALUE else System.currentTimeMillis()+unit.toMillis(timeout);

        val ts =  tasks.map{submit(it)}.toMutableList()
        //wait until at least one task is running or timeout
        while(ts.find { t-> !t.isCancelled && !t.isDone}!=null){
            Thread.sleep(1)
            if(System.currentTimeMillis()>endTime){
                //cancel not completed task
                ts.filter{t-> !t.isCancelled && !t.isDone }.forEach { it.cancel(false) }
            }
        }
        return ts
    }

    override fun <T : Any?> invokeAny(tasks: MutableCollection<out Callable<T>>): T {
        if(tasks.isEmpty())
            throw IllegalArgumentException()

        val ts =  tasks.map{submit(it)}.toMutableList()

        //wait until at least one task finishes
        while(true){
            ts.filter { it.isDone }.forEach { return it.get() }
            Thread.sleep(1)
        }
    }

    override fun <T : Any?> invokeAny(tasks: MutableCollection<out Callable<T>>, timeout: Long, unit: TimeUnit): T {
        if(tasks.isEmpty())
            throw IllegalArgumentException()

        val ts =  tasks.map{submit(it)}.toMutableList()
        val endTime = System.currentTimeMillis()+unit.toMillis(timeout)

        while(true){
            ts.filter { it.isDone }.forEach { return it.get() }
            if(System.currentTimeMillis()>endTime)
                throw TimeoutException()
            Thread.sleep(1)
        }
    }

    private fun check() {
        if (isShutdown())
            throw RejectedExecutionException()
        clearFinishedTasks()
    }

    override fun shutdown() {
        isShutdown.set(true)
        cancelScheduledTasks()
    }

    @Synchronized override fun shutdownNow(): MutableList<Runnable> {
        isShutdown.set(true)
        cancelScheduledTasks()
        return ArrayList<Runnable>()
    }


    override fun isShutdown(): Boolean = isShutdown.get()


    @Synchronized override fun awaitTermination(timeout: Long, unit: TimeUnit): Boolean {
        if(!isShutdown())
            throw IllegalAccessException("call shutdown() first")

        val time = System.currentTimeMillis() + unit.toMillis(timeout)
        while(System.currentTimeMillis()<time){
            if(isTerminated)
                return true
            Thread.sleep(1)
        }
        return false
    }



    @Synchronized protected fun clearFinishedTasks(){
        val iter = tasks.keys.iterator()
        while(iter.hasNext()){
            val v = iter.next()
            if(v == null || v.isDone || v.isCancelled)
                iter.remove()
        }
    }

    @Synchronized protected fun cancelScheduledTasks() {
        val iter = tasks.keys.iterator()
        while(iter.hasNext()){
            val v = iter.next()
            if(v==null) {
                iter.remove()
                continue
            }
            if(v is ScheduledFuture && !v.isDone && !v.isCancelled)
                v.cancel(false)

            if(v.isDone || v.isCancelled)
                iter.remove()
        }

    }

    @Synchronized override fun isTerminated(): Boolean {
        if(!isShutdown())
            return false
        //check if there are any tasks running
        val iter = tasks.keys.iterator()
        while(iter.hasNext()){
            val v = iter.next()
            if(v!=null && !v.isDone && !v.isCancelled){
                return false
            }
            //clean finished tasks
            iter.remove()
        }

        return true
    }

}