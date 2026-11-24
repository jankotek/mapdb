package org.mapdb.serializer;

import org.junit.Test;
import org.mapdb.Serializer;

import java.nio.charset.StandardCharsets;
import java.io.ByteArrayOutputStream;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Comparator;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SerializerStringSearchAllocationTest {

    @Test
    public void largeSearchFitsTheExistingValuesAndQuery() throws Exception {
        for (String serializer : new String[]{"plain", "delta", "originalHash"}) {
            for (String mode : new String[]{"natural", "comparator"}) {
                Process child = new ProcessBuilder(
                        Paths.get(System.getProperty("java.home"), "bin", "java").toString(),
                        "-Xms24m", "-Xmx24m", "-XX:+UseSerialGC", "-XX:NewRatio=15",
                        "-cp", System.getProperty("java.class.path"),
                        SerializerStringSearchAllocationTest.class.getName(), serializer, mode).redirectErrorStream(true).start();
                try {
                    assertTrue("String search child did not finish: " + serializer + "/" + mode,
                            child.waitFor(15, TimeUnit.SECONDS));
                    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                    byte[] buffer = new byte[4096];
                    int count;
                    while ((count = child.getInputStream().read(buffer)) != -1) bytes.write(buffer, 0, count);
                    String output = new String(bytes.toByteArray(), StandardCharsets.UTF_8);
                    assertEquals(serializer + "/" + mode + ":\n" + output, 0, child.exitValue());
                    assertEquals("SEARCH_COMPLETE\n", output);
                } finally {
                    if (child.isAlive()) {
                        child.destroyForcibly();
                        assertTrue("String search child must stop", child.waitFor(5, TimeUnit.SECONDS));
                    }
                }
            }
        }
    }

    @Test
    public void matchesAndInsertionPointsKeepStringOrdering() {
        String[] values = {"", "\u0000", "a", "a\u0000", "aa", "ab", "é", "\uD83C\uDF31", "\uFFFF"};
        String[] queries = {"", "\u0000", "\u0001", "a", "a\u0000", "a\u0001", "aa", "aaa", "ab", "z", "é", "\uD83C\uDF31", "\uFFFF", "\uFFFFx"};
        for (GroupSerializer<String> serializer : serializers()) {
            Object stored = serializer.valueArrayFromArray(values);
            for (String query : queries) {
                int expected = Arrays.binarySearch(values, query);
                assertEquals(query, expected, serializer.valueArraySearch(stored, query));
                assertEquals(query, expected, serializer.valueArraySearch(stored, query, serializer));
                assertEquals(query, expected, serializer.valueArraySearch(stored, query, Comparator.naturalOrder()));
            }
            assertEquals(-1, serializer.valueArraySearch(serializer.valueArrayEmpty(), "a"));
            assertEquals(-1, serializer.valueArraySearch(serializer.valueArrayEmpty(), "a", serializer));
            Object duplicates = serializer.valueArrayFromArray(new String[]{"a", "a", "a"});
            assertEquals(1, serializer.valueArraySearch(duplicates, "a"));
            String[] reversed = values.clone();
            Comparator<String> reverse = Comparator.reverseOrder();
            Arrays.sort(reversed, reverse);
            Object reverseStored = serializer.valueArrayFromArray(reversed);
            for (String query : queries) {
                assertEquals(query, Arrays.binarySearch(reversed, query, reverse),
                        serializer.valueArraySearch(reverseStored, query, reverse));
            }
        }
    }

    @Test
    public void aSubclassComparatorKeepsItsOrdering() {
        SerializerString reverse = new SerializerString() {
            @Override public int compare(String first, String second) {
                return second.compareTo(first);
            }
        };
        String[] values = {"z", "m", "a"};
        Object stored = reverse.valueArrayFromArray(values);
        for (String query : new String[]{"zz", "z", "n", "m", "b", "a", ""}) {
            assertEquals(Arrays.binarySearch(values, query, reverse),
                    reverse.valueArraySearch(stored, query, reverse));
        }
    }

    @SuppressWarnings("unchecked")
    private static GroupSerializer<String>[] serializers() {
        return new GroupSerializer[]{Serializer.STRING, Serializer.STRING_DELTA, Serializer.STRING_ORIGHASH};
    }

    public static void main(String[] args) {
        GroupSerializer<String> serializer = args[0].equals("plain") ? Serializer.STRING :
                args[0].equals("delta") ? Serializer.STRING_DELTA : Serializer.STRING_ORIGHASH;
        // Query and stored characters occupy 16 MiB. Non-Latin characters keep that
        // size the same before and after compact Strings. A further 8-MiB copy cannot
        // fit in the 24-MiB child; its fixed old-generation space holds the two inputs.
        String query = largeQuery();
        Object values = serializer.valueArrayFromArray(new String[]{query});
        for (int iteration = 0; iteration < 20; iteration++) {
            int result = args[1].equals("natural") ? serializer.valueArraySearch(values, query) :
                    serializer.valueArraySearch(values, query, serializer);
            if (result != 0) throw new AssertionError("Expected index 0 but received " + result);
        }
        System.out.print("SEARCH_COMPLETE\n");
    }

    private static String largeQuery() {
        String text = "\u0100";
        // Concatenation constructs the input without a temporary copy of the final
        // character array, keeping preparation below search's working-memory bound.
        for (int power = 0; power < 22; power++) text = text.concat(text);
        return text;
    }
}
