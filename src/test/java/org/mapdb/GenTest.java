package org.mapdb;

import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class GenTest {

    @Rule
    public Gen<String> params =
            new Gen("alpha", "beta", "gamma", "alfa");

    @Test
    public void testSomething() throws Exception {
        assertTrue(params.get().length() >= 4);
    }
}
