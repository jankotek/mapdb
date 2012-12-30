package benchmark

import com.carrotsearch.junitbenchmarks.AbstractBenchmark
import com.carrotsearch.junitbenchmarks.BenchmarkRule
import org.junit.Test
import org.mapdb.CC
import org.junit.Assume

class MicroBenchDemo(): AbstractBenchmark() {

    Test fun concat(){
        Assume.assumeTrue(CC.FULL_TEST);
        var s = ""
        for (i in 0..1000 - 1) s += i
        if (s.length() == 111) throw Error()
    }

    Test fun concat_string_builder(): Unit {
        Assume.assumeTrue(CC.FULL_TEST);
        var s= StringBuilder()
        for (i in 0..1000 - 1) s.append(i)
        if (s.toString().length() == 111) throw Error()
    }


}
