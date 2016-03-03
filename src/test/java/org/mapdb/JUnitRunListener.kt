package org.mapdb

import org.junit.runner.Description
import org.junit.runner.notification.RunListener
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit

/**
 * Prints currently running tests into console every 5 minutes.
 * Is used to monitor integration tests which may run for several hours.
 */
class JUnitRunListener: RunListener() {

    val runningTests = ConcurrentHashMap<String, Long>()
    val period = 5*60*1000L

    val exec = TT.executor()
    init{
        exec.scheduleAtFixedRate({
            println("Running tests: ")
            runningTests.forEach {name, time ->
                println("   $name - " + (System.currentTimeMillis()-time)/(60*1000))
            }
        }, period, period, TimeUnit.MILLISECONDS)
    }



    override fun testStarted(description: Description?) {
        runningTests.put(description!!.displayName!!, System.currentTimeMillis())
    }

    override fun testFinished(description: Description?) {
        runningTests.remove(description!!.displayName)
    }
}
