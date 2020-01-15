package com.linbit.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Test;

public class PairTest
{
    private final Pair<String, String> null_null = new Pair<>(null, null);
    private final Pair<String, String> null_a = new Pair<>(null, "a");
    private final Pair<String, String> a_null = new Pair<>("a", null);
    private final Pair<String, String> a_a = new Pair<>("a", "a");
    private final Pair<String, String> a_b = new Pair<>("a", "b");
    private final Pair<String, String> b_a = new Pair<>("b", "a");
    private final Pair<String, String> b_b = new Pair<>("b", "b");

    @Test
    public void compare() throws Exception
    {
        assertEquals(-1, null_a.compareTo(a_a));
        assertEquals(-1, null_a.compareTo(a_b));
        assertEquals(-1, null_a.compareTo(b_a));
        assertEquals(-1, null_a.compareTo(b_b));
        assertEquals(-1, null_a.compareTo(a_null));
        assertEquals(0, null_a.compareTo(null_a));
        assertEquals(1, null_a.compareTo(null_null));

        assertEquals(-1, a_null.compareTo(a_a));
        assertEquals(-1, a_null.compareTo(a_b));
        assertEquals(-1, a_null.compareTo(b_a));
        assertEquals(-1, a_null.compareTo(b_b));
        assertEquals(0, a_null.compareTo(a_null));
        assertEquals(1, a_null.compareTo(null_null));
        assertEquals(1, a_null.compareTo(null_a));

        assertEquals(-1, a_a.compareTo(a_b));
        assertEquals(-1, a_a.compareTo(b_a));
        assertEquals(-1, a_a.compareTo(b_b));
        assertEquals(0, a_a.compareTo(a_a));
        assertEquals(1, a_a.compareTo(null_null));
        assertEquals(1, a_a.compareTo(null_a));
        assertEquals(1, a_a.compareTo(a_null));

        assertEquals(-1, a_b.compareTo(b_a));
        assertEquals(-1, a_b.compareTo(b_b));
        assertEquals(0, a_b.compareTo(a_b));
        assertEquals(1, a_b.compareTo(a_a));
        assertEquals(1, a_b.compareTo(null_null));
        assertEquals(1, a_b.compareTo(null_a));
        assertEquals(1, a_b.compareTo(a_null));

        assertEquals(-1, b_a.compareTo(b_b));
        assertEquals(0, b_a.compareTo(b_a));
        assertEquals(1, b_a.compareTo(a_a));
        assertEquals(1, b_a.compareTo(a_b));
        assertEquals(1, b_a.compareTo(null_null));
        assertEquals(1, b_a.compareTo(null_a));
        assertEquals(1, b_a.compareTo(a_null));

        assertEquals(0, b_b.compareTo(b_b));
        assertEquals(1, b_b.compareTo(a_a));
        assertEquals(1, b_b.compareTo(a_b));
        assertEquals(1, b_b.compareTo(b_a));
        assertEquals(1, b_b.compareTo(null_null));
        assertEquals(1, b_b.compareTo(null_a));
        assertEquals(1, b_b.compareTo(a_null));

        List<Pair<String, String>> sortTest = new ArrayList<>();
        sortTest.add(b_b);
        sortTest.add(a_b);
        sortTest.add(null_null);
        sortTest.add(a_a);
        sortTest.add(null_a);
        sortTest.add(b_a);
        sortTest.add(a_null);

        Collections.sort(sortTest);
        List<Pair<String, String>> expectedOrder = Arrays.asList(
            null_null,
            null_a,
            a_null,
            a_a,
            a_b,
            b_a,
            b_b
        );
        for (int i = 0; i < expectedOrder.size(); i++)
        {
            assertEquals(expectedOrder.get(i), sortTest.get(i));
        }
    }

    @Test
    public void equals() throws Exception
    {
        assertTrue(a_a.equals(a_a));
        assertTrue(a_a.equals(new Pair<>("a", "a")));
        assertFalse(a_a.equals(a_b));

        assertTrue(null_a.equals(null_a));
        assertFalse(null_a.equals(a_null));

        assertTrue(a_null.equals(a_null));
        assertFalse(a_null.equals(null_a));
    }
}
