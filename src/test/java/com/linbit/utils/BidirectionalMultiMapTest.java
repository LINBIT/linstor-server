package com.linbit.utils;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("checkstyle:magicnumber")
public class BidirectionalMultiMapTest
{
    private Map<String, TreeSet<Integer>> backingMain;
    private Map<Integer, TreeSet<String>> backingInverted;
    private BidirectionalMultiMap<String, Integer> map;

    @Before
    public void setup()
    {
        backingMain = new HashMap<>();
        backingInverted = new HashMap<>();
        map = new BidirectionalMultiMap<>(
            backingMain,
            backingInverted,
            () -> new TreeSet<>(),
            () -> new TreeSet<>()
        );
        assertTrue(backingMain.isEmpty());
        assertTrue(backingInverted.isEmpty());
        assertTrue(map.isEmpty());
    }

    @Test
    public void testPut()
    {
        map.add("A", 1);
        assertTrue(backingMain.containsKey("A"));
        assertTrue(backingMain.get("A").contains(1));
        assertTrue(backingInverted.containsKey(1));
        assertTrue(backingInverted.get(1).contains("A"));
    }

    @Test
    public void testContains()
    {
        map.add("A", 1);
        assertTrue(backingMain.containsKey("A"));
        assertTrue(map.containsKey("A"));
        assertTrue(backingInverted.containsKey(1));
        assertTrue(map.containsValue(1));
    }

    @Test
    public void testCount()
    {
        map.add("A", 1);
        map.add("A", 2);
        map.add("B", 2);
        map.add("B", 1);
        map.add("C", 3);
        map.add("D", 4);
        map.add("D", 5);
        assertEquals(4, backingMain.size());
        assertEquals(backingMain.size(), map.keyCount());
        assertEquals(5, backingInverted.size());
        assertEquals(backingInverted.size(), map.valueCount());
    }

    @Test
    public void testIsEmpty()
    {
        map.add("A", 1);
        assertFalse(backingMain.isEmpty());
        assertFalse(backingInverted.isEmpty());
        assertFalse(map.isEmpty());
        map.removeValue(1);
        assertTrue(backingMain.isEmpty());
        assertTrue(backingInverted.isEmpty());
        assertTrue(map.isEmpty());
    }

    @Test
    public void testRemoveKey()
    {
        map.add("A", 1);
        map.add("B", 2);
        map.removeKey("B");
        assertTrue(map.containsKey("A"));
        assertTrue(map.containsValue(1));
        assertFalse(map.containsKey("B"));
        assertFalse(map.containsValue(2));
    }

    @Test
    public void testRemoveValue()
    {
        map.add("A", 1);
        map.add("B", 2);
        map.removeValue(2);
        assertTrue(map.containsKey("A"));
        assertTrue(map.containsValue(1));
        assertFalse(map.containsKey("B"));
        assertFalse(map.containsValue(2));
    }

    @Test
    public void testRemove()
    {
        map.add("A", 1);
        map.add("A", 2);
        map.add("A", 3);
        map.add("A", 4);
        map.add("A", 5);
        map.add("B", 1);
        assertTrue(map.containsKey("A"));
        assertTrue(map.containsValue(1));
        assertTrue(map.getByKey("A").contains(1));
        assertTrue(map.getByValue(1).contains("A"));
        map.remove("A", 1);
        assertTrue(map.containsKey("A"));
        assertTrue(map.containsValue(1));
        assertFalse(map.getByKey("A").contains(1));
        assertFalse(map.getByValue(1).contains("A"));
    }

    @Test
    public void testClear()
    {
        map.add("A", 1);
        map.add("A", 2);
        map.add("B", 2);
        map.add("B", 1);
        map.add("C", 3);
        map.add("D", 4);
        map.add("D", 5);
        assertFalse(map.isEmpty());
        map.clear();
        assertTrue(map.isEmpty());
    }

    @Test
    public void testSets()
    {
        Map<String, Set<Integer>> mainForComp = new HashMap<>();
        mainForComp.put("A", new TreeSet<>());
        mainForComp.get("A").add(1);
        mainForComp.get("A").add(2);
        mainForComp.put("B", new TreeSet<>());
        mainForComp.get("B").add(2);
        mainForComp.put("C", new TreeSet<>());
        mainForComp.get("C").add(1);
        mainForComp.put("D", new TreeSet<>());
        mainForComp.get("D").add(3);
        Map<Integer, Set<String>> invertedForComp = new HashMap<>();
        invertedForComp.put(1, new TreeSet<>());
        invertedForComp.get(1).add("A");
        invertedForComp.get(1).add("C");
        invertedForComp.put(2, new TreeSet<>());
        invertedForComp.get(2).add("A");
        invertedForComp.get(2).add("B");
        invertedForComp.put(3, new TreeSet<>());
        invertedForComp.get(3).add("D");
        map.add("A", 1);
        map.add("A", 2);
        map.add("B", 2);
        map.add("C", 1);
        map.add("D", 3);
        Set<String> keySet = map.keySet();
        assertEquals(4, keySet.size());
        assertTrue(keySet.contains("A"));
        assertTrue(keySet.contains("B"));
        assertTrue(keySet.contains("C"));
        assertTrue(keySet.contains("D"));
        Set<Integer> valueSet = map.valueSet();
        assertEquals(3, valueSet.size());
        assertTrue(valueSet.contains(1));
        assertTrue(valueSet.contains(2));
        assertTrue(valueSet.contains(3));
        Set<Entry<String, Set<Integer>>> entrySet = map.entrySet();
        assertEquals(4, entrySet.size());
        assertEquals(mainForComp.entrySet(), entrySet);
        Set<Entry<Integer, Set<String>>> entrySetInverted = map.entrySetInverted();
        assertEquals(3, entrySetInverted.size());
        assertEquals(invertedForComp.entrySet(), entrySetInverted);
    }

    @Test
    public void testGets() {
        map.add("A", 1);
        map.add("A", 2);
        map.add("B", 1);
        map.add("C", 1);
        Set<Integer> byKey = map.getByKey("A");
        Set<Integer> byKeyComp = new TreeSet<>(Arrays.asList(1, 2));
        assertEquals(byKeyComp, byKey);
        Set<String> byValue = map.getByValue(1);
        Set<String> byValueComp = new TreeSet<>(Arrays.asList("A", "B", "C"));
        assertEquals(byValueComp, byValue);
        Set<Integer> byKeyNotEmpty = map.getByKeyOrEmpty("C");
        Set<Integer> byKeyNotEmptyComp = Collections.singleton(1);
        assertEquals(byKeyNotEmptyComp, byKeyNotEmpty);
        Set<Integer> byKeyIsEmpty = map.getByKeyOrEmpty("D");
        Set<Integer> byKeyIsEmptyComp = Collections.emptySet();
        assertEquals(byKeyIsEmptyComp, byKeyIsEmpty);
        Set<String> byValueNotEmpty = map.getByValueOrEmpty(2);
        Set<String> byValueNotEmptyComp = Collections.singleton("A");
        assertEquals(byValueNotEmptyComp, byValueNotEmpty);
        Set<String> byValueIsEmpty = map.getByValueOrEmpty(3);
        Set<String> byValueIsEmptyComp = Collections.emptySet();
        assertEquals(byValueIsEmptyComp, byValueIsEmpty);
        Set<Integer> byKeyNotDefault = map.getByKeyOrDefault("B", Collections.singleton(-1));
        Set<Integer> byKeyNotDefaultComp = Collections.singleton(1);
        assertEquals(byKeyNotDefaultComp, byKeyNotDefault);
        Set<Integer> byKeyIsDefault = map.getByKeyOrDefault("D", Collections.singleton(-1));
        Set<Integer> byKeyIsDefaultComp = Collections.singleton(-1);
        assertEquals(byKeyIsDefaultComp, byKeyIsDefault);
        Set<String> byValueNotDefault = map.getByValueOrDefault(2, Collections.singleton("ups"));
        Set<String> byValueNotDefaultComp = Collections.singleton("A");
        assertEquals(byValueNotDefaultComp, byValueNotDefault);
        Set<String> byValueIsDefault = map.getByValueOrDefault(3, Collections.singleton("ups"));
        Set<String> byValueIsDefaultComp = Collections.singleton("ups");
        assertEquals(byValueIsDefaultComp, byValueIsDefault);
    }

    @Test
    public void testEqualsAndHashCode()
    {
        BidirectionalMultiMap<String, Integer> otherMap = new BidirectionalMultiMap<>();
        assertEquals(map.hashCode(), otherMap.hashCode());
        assertEquals(map, otherMap);
        map.add("A", 1);
        map.add("B", 2);
        assertNotEquals(map.hashCode(), otherMap.hashCode());
        assertNotEquals(map, otherMap);
        otherMap.add("A", 1);
        otherMap.add("B", 2);
        assertEquals(map.hashCode(), otherMap.hashCode());
        assertEquals(map, otherMap);
    }
}
