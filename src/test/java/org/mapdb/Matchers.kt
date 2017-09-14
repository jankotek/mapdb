package org.mapdb

import org.junit.ComparisonFailure


interface Matcher<T> {

    fun test(value: T): Result
//
//    infix fun and(other: Matcher<T>): Matcher<T> = object : Matcher<T> {
//        override fun test(value: T): Result {
//            val r = this@Matcher.test(value)
//            if (!r.passed)
//                return r
//            else
//                return other.test(value)
//        }
//    }
//
//    infix fun or(other: Matcher<T>): Matcher<T> = object : Matcher<T> {
//        override fun test(value: T): Result {
//            val r = this@Matcher.test(value)
//            if (r.passed)
//                return r
//            else
//                return other.test(value)
//        }
//    }
}

inline fun <reified T> shouldThrow(thunk: () -> Any?): T {
    val e = try {
        thunk()
        null
    } catch (e: Throwable) {
        e
    }

    val exceptionClassName = T::class.qualifiedName

    if (e == null)
        throw AssertionError("Expected exception ${T::class.qualifiedName} but no exception was thrown")
    else if (e.javaClass.canonicalName != exceptionClassName)
        throw AssertionError("Expected exception ${T::class.qualifiedName} but ${e.javaClass.name} was thrown", e)
    else
        return e as T
}

inline fun shouldThrowAny(thunk: () -> Any?): Throwable {
    val e = try {
        thunk()
        null
    } catch (e: Throwable) {
        e
    }

    if (e == null)
        throw AssertionError("Expected exception but no exception was thrown")
    else
        return e
}

data class Result(val passed: Boolean, val message: String)


fun <T> equalityMatcher(expected: T) = object : Matcher<T> {
    override fun test(value: T): Result = Result(expected == value, equalsErrorMessage(expected, value))
}

fun shouldFail(msg: String="Failed"): Nothing = throw AssertionError(msg)

infix fun Double.shouldBe(other: Double): Unit = should(ToleranceMatcher(other, 0.0))

infix fun String.shouldBe(other: String) {
    if (this != other) {
        throw ComparisonFailure("", other, this)
    }
}

infix fun BooleanArray.shouldBe(other: BooleanArray): Unit {
    val expected = other.toList()
    val actual = this.toList()
    if (actual != expected)
        throw equalsError(expected, actual)
}

infix fun IntArray.shouldBe(other: IntArray): Unit {
    val expected = other.toList()
    val actual = this.toList()
    if (actual != expected)
        throw equalsError(expected, actual)
}

infix fun DoubleArray.shouldBe(other: DoubleArray): Unit {
    val expected = other.toList()
    val actual = this.toList()
    if (actual != expected)
        throw equalsError(expected, actual)
}

infix fun LongArray.shouldBe(other: LongArray): Unit {
    val expected = other.toList()
    val actual = this.toList()
    if (actual != expected)
        throw equalsError(expected, actual)
}

infix fun <T> Array<T>.shouldBe(other: Array<T>): Unit {
    val expected = other.toList()
    val actual = this.toList()
    if (actual != expected)
        throw equalsError(expected, actual)
}

infix fun <T> T.shouldHave(matcher: Matcher<T>) = should(matcher)
infix fun <T> T.shouldBe(any: Any?): Unit = shouldEqual(any)
infix fun <T> T.shouldEqual(any: Any?): Unit {
    when (any) {
        is Matcher<*> -> should(any as Matcher<T>)
        else -> {
            if (this == null && any != null)
                throw equalsError(any, this)
            if (this != any)
                throw equalsError(any, this)
        }
    }
}

infix fun <T> T.should(matcher: (T) -> Unit): Unit = matcher(this)

infix fun <T> T.should(matcher: Matcher<T>): Unit {
    val result = matcher.test(this)
    if (!result.passed)
        throw AssertionError(result.message)
}

infix fun <T> T.shouldNotBe(any: Any?): Unit {
    when (any) {
        is Matcher<*> -> shouldNot(any as Matcher<T>)
        else -> shouldNot(equalityMatcher(any))
    }
}

infix fun <T> T.shouldNot(matcher: Matcher<T>): Unit {
    val result = matcher.test(this)
    if (result.passed)
        throw AssertionError("Test passed which should have failed: " + result.message)
}

private fun equalsError(expected: Any?, actual: Any?) = AssertionError(equalsErrorMessage(expected, actual))
private fun equalsErrorMessage(expected: Any?, actual: Any?) = "expected: $expected but was: $actual"


fun <T> shouldHaveSizeMatcher(size: Int) = object : Matcher<Collection<T>> {
    override fun test(value: Collection<T>) = Result(value.size == size, "Collection should have size $size but have size ${value.size}")
}

fun <T> shouldContainsMatcher(t: T) = object : Matcher<Collection<T>> {
    override fun test(value: Collection<T>) = Result(value.contains(t), "Collection should shouldContain element $t")
}

fun <T> shouldBeEmpty(): Matcher<Collection<T>> = object : Matcher<Collection<T>> {
    override fun test(value: Collection<T>): Result = Result(value.isEmpty(), "Collection should be empty")
}

fun <T> containsAll(vararg ts: T): Matcher<Collection<T>> = object : Matcher<Collection<T>> {
    override fun test(value: Collection<T>) =
            Result(ts.all { value.contains(it) }, "Collection should shouldContain values $ts")
}

fun <T> shouldHaveSize(size: Int): Matcher<Collection<T>> = shouldHaveSizeMatcher(size)

fun <T> shouldContain(t: T): Matcher<Collection<T>> = shouldContainsMatcher(t)

fun <T> singleElement(t: T): Matcher<Collection<T>> = object : Matcher<Collection<T>> {
    override fun test(value: Collection<T>) = Result(value.size == 1 && value.first() == t, "Collection should be a single element of $t but is $value")
}

fun <T : Comparable<T>> sorted(): Matcher<List<T>> = object : Matcher<List<T>> {
    override fun test(value: List<T>) = Result(value.sorted() == value, "Collection $value should be sorted")
}


fun <K> haveKey(key: K): Matcher<Map<K, *>> = object : Matcher<Map<K, *>> {
    override fun test(value: Map<K, *>) = Result(value.containsKey(key), "Map should shouldContain key $key")
}

fun <V> haveValue(v: V): Matcher<Map<*, V>> = object : Matcher<Map<*, V>> {
    override fun test(value: Map<*, V>) = Result(value.containsValue(v), "Map should shouldContain value $v")
}

fun <K, V> shouldContain(key: K, v: V): Matcher<Map<K, V>> = object : Matcher<Map<K, V>> {
    override fun test(value: Map<K, V>) = Result(value[key] == v, "Map should shouldContain mapping $key=$v but was $value")
}


fun <T> shouldLt(x: T) = shouldBeLessThan(x)
fun <T> shouldBeLessThan(x: T) = object : Matcher<Comparable<T>> {
    override fun test(value: Comparable<T>) = Result(value < x, "$value should be < $x")
}

fun <T> shouldLte(x: T) = shouldBeLessThanOrEqualTo(x)
fun <T> shouldBeLessThanOrEqualTo(x: T) = object : Matcher<Comparable<T>> {
    override fun test(value: Comparable<T>) = Result(value <= x, "$value should be <= $x")
}

fun <T> shouldGt(x: T) = shouldBeGreaterThan(x)
fun <T> shouldBeGreaterThan(x: T) = object : Matcher<Comparable<T>> {
    override fun test(value: Comparable<T>) = Result(value > x, "$value should be > $x")
}

fun <T> shouldGte(x: T) = shouldBeGreaterThanOrEqualTo(x)
fun <T> shouldBeGreaterThanOrEqualTo(x: T) = object : Matcher<Comparable<T>> {
    override fun test(value: Comparable<T>) = Result(value >= x, "$value should be >= $x")
}


fun shouldStartWith(prefix: String): Matcher<String> = object : Matcher<String> {
    override fun test(value: String): Result {
        val ok = value.startsWith(prefix)
        var msg = "String $value should start with $prefix"
        if (!ok) {
            for (k in 0..Math.min(value.length, prefix.length) - 1) {
                if (value[k] != prefix[k]) {
                    msg = "$msg (diverged at index $k)"
                    break
                }
            }
        }
        return Result(ok, msg)
    }
}

fun shouldHaveSubstring(substr: String) = shouldInclude(substr)
fun shouldSubstring(substr: String) = shouldInclude(substr)
fun shouldInclude(substr: String): Matcher<String> = object : Matcher<String> {
    override fun test(value: String) = Result(value.contains(substr), "String $value should shouldInclude shouldSubstring $substr")
}

fun shouldEndWith(suffix: String): Matcher<String> = object : Matcher<String> {
    override fun test(value: String) = Result(value.endsWith(suffix), "String $value should end with $suffix")
}

fun shouldMatch(regex: String): Matcher<String> = object : Matcher<String> {
    override fun test(value: String) = Result(value.matches(regex.toRegex()), "String $value should match regex $regex")
}

fun shouldStrlen(length: Int): Matcher<String> = shouldHaveLength(length)
fun shouldHaveLength(length: Int): Matcher<String> = object : Matcher<String> {
    override fun test(value: String) = Result(value.length == length, "String $value should have length $length")
}



infix fun Double.shouldPlusOrMinus(tolerance: Double): ToleranceMatcher = ToleranceMatcher(this, tolerance)

fun shouldExactly(d: Double): Matcher<Double> = object : Matcher<Double> {
    override fun test(value: Double) = Result(value == d, "$value is not equal to expected value $d")
}

class ToleranceMatcher(val expected: Double, val tolerance: Double) : Matcher<Double> {

    override fun test(value: Double): Result {
        if (tolerance == 0.0)
            println("[WARN] When comparing doubles consider using tolerance, eg: a shouldBe b shouldPlusOrMinus c")
        val diff = Math.abs(value - expected)
        return Result(diff <= tolerance, "$value should be equal to $expected")
    }

    infix fun plusOrMinus(tolerance: Double): ToleranceMatcher = ToleranceMatcher(expected, tolerance)
}



fun shouldBetween(a: Int, b: Int): Matcher<Int> = object : Matcher<Int> {
    override fun test(value: Int) = Result(a <= value && value <= b, "$value is between ($a, $b)")
}

fun shouldLt(x: Int) = shouldBeLessThan(x)
fun shouldBeLessThan(x: Int) = object : Matcher<Int> {
    override fun test(value: Int) = Result(value < x, "$value should be < $x")
}

fun shouldLte(x: Int) = shouldBeLessThanOrEqualTo(x)
fun shouldBeLessThanOrEqualTo(x: Int) = object : Matcher<Int> {
    override fun test(value: Int) = Result(value <= x, "$value should be <= $x")
}

fun shouldGt(x: Int) = shouldBeGreaterThan(x)
fun shouldBeGreaterThan(x: Int) = object : Matcher<Int> {
    override fun test(value: Int) = Result(value > x, "$value should be > $x")
}

fun shouldGte(x: Int) = shouldBeGreaterThanOrEqualTo(x)
fun shouldBeGreaterThanOrEqualTo(x: Int) = object : Matcher<Int> {
    override fun test(value: Int) = Result(value >= x, "$value should be >= $x")
}



fun shouldBetween(a: Long, b: Long): Matcher<Long> = object : Matcher<Long> {
    override fun test(value: Long) = Result(a <= value && value <= b, "$value is between ($a, $b)")
}

fun shouldLt(x: Long) = shouldBeLessThan(x)
fun shouldBeLessThan(x: Long) = object : Matcher<Long> {
    override fun test(value: Long) = Result(value < x, "$value should be < $x")
}

fun shouldLte(x: Long) = shouldBeLessThanOrEqualTo(x)
fun shouldBeLessThanOrEqualTo(x: Long) = object : Matcher<Long> {
    override fun test(value: Long) = Result(value <= x, "$value should be <= $x")
}

fun shouldGt(x: Long) = shouldBeGreaterThan(x)
fun shouldBeGreaterThan(x: Long) = object : Matcher<Long> {
    override fun test(value: Long) = Result(value > x, "$value should be > $x")
}

fun shouldGte(x: Long) = shouldBeGreaterThanOrEqualTo(x)
fun shouldBeGreaterThanOrEqualTo(x: Long) = object : Matcher<Long> {
    override fun test(value: Long) = Result(value >= x, "$value should be >= $x")
}
