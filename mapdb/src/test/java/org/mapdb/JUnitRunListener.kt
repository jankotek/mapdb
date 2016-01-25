package org.mapdb

import org.junit.runner.Description
import org.junit.runner.notification.RunListener
import java.util.*
import java.util.concurrent.TimeUnit

/**
 * Prints currently running tests into console every 5 minutes.
 * Is used to monitor integration tests which may run for several hours.
 */
class JUnitRunListener: RunListener() {

    val runningTests = Collections.synchronizedSet(HashSet<String>())
    val period = 5*60*1000L

    val exec = TT.executor()
    init{
        exec.scheduleAtFixedRate({
            println("Running tests: ")
            runningTests.forEach {
                println("   " +it)
            }
        }, period, period, TimeUnit.MILLISECONDS)
    }



    override fun testStarted(description: Description?) {
        runningTests.add(description?.displayName)
    }

    override fun testFinished(description: Description?) {
        runningTests.remove(description?.displayName)
    }
}
