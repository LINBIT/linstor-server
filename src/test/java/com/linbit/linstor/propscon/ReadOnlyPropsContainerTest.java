package com.linbit.linstor.propscon;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.prop.LinStorObject;
import com.linbit.linstor.dbdrivers.SatellitePropDriver;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.transaction.manager.SatelliteTransactionMgr;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import static com.linbit.linstor.propscon.CommonPropsTestUtils.FIRST_AMOUNT;
import static com.linbit.linstor.propscon.CommonPropsTestUtils.FIRST_KEY;
import static com.linbit.linstor.propscon.CommonPropsTestUtils.SECOND_AMOUNT;
import static com.linbit.linstor.propscon.CommonPropsTestUtils.SECOND_KEY;
import static com.linbit.linstor.propscon.CommonPropsTestUtils.assertIteratorEqual;
import static com.linbit.linstor.propscon.CommonPropsTestUtils.checkIfEntrySetIsValid;
import static com.linbit.linstor.propscon.CommonPropsTestUtils.createEntry;
import static com.linbit.linstor.propscon.CommonPropsTestUtils.fillProps;
import static com.linbit.linstor.propscon.CommonPropsTestUtils.generateEntries;
import static com.linbit.linstor.propscon.CommonPropsTestUtils.generateKeys;
import static com.linbit.linstor.propscon.CommonPropsTestUtils.generateValues;
import static com.linbit.linstor.propscon.CommonPropsTestUtils.glue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ReadOnlyPropsContainerTest
{
    private static final String TEST_INSTANCE_NAME = "testInstanceName";

    private TransactionMgr transactionMgr;

    private PropsContainerFactory propsContainerFactory;
    private PropsContainer writableProp;
    private ReadOnlyPropsImpl roProp;
    private Map<String, String> roMap;
    private Set<String> roKeySet;
    private Set<Map.Entry<String, String>> roEntrySet;
    private Collection<String> roValues;

    @Before
    public void setUp() throws Exception
    {
        transactionMgr = new SatelliteTransactionMgr();

        propsContainerFactory = new PropsContainerFactory(new SatellitePropDriver(), () -> transactionMgr);
        writableProp = propsContainerFactory.getInstance(TEST_INSTANCE_NAME, "", LinStorObject.CTRL);
        roProp = new ReadOnlyPropsImpl(writableProp);

        fillProps(writableProp, FIRST_KEY, FIRST_AMOUNT, SECOND_KEY, SECOND_AMOUNT);

        roMap = roProp.map();
        roEntrySet = roProp.entrySet();
        roKeySet = roProp.keySet();
        roValues = roProp.values();
    }

    @Test
    public void testSize() throws Throwable
    {
        writableProp.clear();

        final String firstKey = FIRST_KEY;
        final String firstValue = "value";

        final String secondKey = SECOND_KEY;
        final String secondValue = "other value";

        assertEquals(0, roProp.size());

        writableProp.setProp(firstKey, firstValue);
        assertEquals(1, roProp.size());

        writableProp.setProp(secondKey, secondValue);
        assertEquals(2, roProp.size());

        writableProp.setProp(secondKey, firstValue);
        assertEquals(2, roProp.size());

        writableProp.removeProp(secondKey);
        assertEquals(1, roProp.size());

        writableProp.removeProp("nonExistent");
        assertEquals(1, roProp.size());
    }

    @Test
    public void testIsEmpty() throws Throwable
    {
        writableProp.clear();
        assertTrue(roProp.isEmpty());

        final String key = "key";
        final String value = "value";
        writableProp.setProp(key, value);

        assertFalse(roProp.isEmpty());

        writableProp.clear();
        assertTrue(roProp.isEmpty());

        writableProp.setProp(key, value);
        assertFalse(roProp.isEmpty());
        writableProp.removeProp(key);
        assertTrue(roProp.isEmpty());
    }

    @Test
    public void testGetProp() throws Throwable
    {
        final String key = "key";
        final String value = "value";

        writableProp.setProp(key, value);

        assertEquals(value, roProp.getProp(key));
    }


    @Test
    public void testGetProbWithNamespace() throws Throwable
    {
        final String first = FIRST_KEY;
        final String second = SECOND_KEY;

        final String key = glue(first, second);
        final String value = "value";
        writableProp.setProp(key, value);
        assertEquals(value, roProp.getProp(second, first));
    }

    @Test
    public void testGetRemovedEntry() throws Throwable
    {
        final String key = "key";
        final String value = "value";
        writableProp.setProp(key, value);
        assertEquals(value, roProp.getProp(key));

        writableProp.removeProp(key);
        assertNull(roProp.getProp(key));
    }


    @Test(expected = AccessDeniedException.class)
    public void testSet() throws Throwable
    {
        roProp.setProp("key", "value");
    }

    @Test(expected = AccessDeniedException.class)
    public void testSetWithNamespace() throws Throwable
    {
        roProp.setProp("key", "value", "namespace");
    }

    @Test(expected = AccessDeniedException.class)
    public void testRemove() throws Throwable
    {
        roProp.removeProp(FIRST_KEY + "0");
    }

    @Test(expected = AccessDeniedException.class)
    public void testRemoveWithNamespace() throws Throwable
    {
        roProp.removeProp(SECOND_KEY + "0", FIRST_KEY + "0");
    }


    @Test
    public void testGetPath() throws Throwable
    {
        assertEquals("", roProp.getPath());

        writableProp.setProp("a/b/c/d", "value");
        final @Nullable Props namespaceA = roProp.getNamespace("a");

        assertEquals("a/", namespaceA.getPath());

        final @Nullable Props namespaceB = namespaceA.getNamespace("b");
        assertEquals("a/b/", namespaceB.getPath());

        final @Nullable Props namespaceC = roProp.getNamespace("a/b/c");
        assertEquals("a/b/c/", namespaceC.getPath());
    }

    @Test
    public void testGetPathTrailingSlash() throws Throwable
    {
        writableProp.setProp("a/b/c/d", "value");
        final @Nullable Props namespaceC = roProp.getNamespace("a/b/c");
        assertNotNull(namespaceC);

        assertEquals(namespaceC, roProp.getNamespace("a/b/c/"));
    }

    @Test
    public void testIterator() throws Throwable
    {
        // PropsContainer iterates the entries first, then the containers
        // thus, we add a container which should be come before the "first" alpha-numerically entry

        final String insertedKey = "a/b";
        final String insertedValue = "value";
        writableProp.setProp(insertedKey, insertedValue);

        final Iterator<Entry<String, String>> iterator = roProp.iterator();

        final ArrayList<Entry<String, String>> generatedEntries = generateEntries(
            FIRST_KEY, FIRST_AMOUNT, SECOND_KEY, SECOND_AMOUNT
        );

        // insert the "a/b" key after "first2" and before "first0/second0"
        generatedEntries.add(FIRST_AMOUNT + 1, createEntry(insertedKey, insertedValue));

        assertIteratorEqual(iterator, generatedEntries, true);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testIteratorUnsupportedRemove() throws Throwable
    {
        final Iterator<Entry<String, String>> iterator = roProp.iterator();
        iterator.next();

        iterator.remove();

        fail("iterator.remove should not be supported");
    }

    @Test
    public void testKeyIterator() throws Throwable
    {
        // PropsContainer iterates the entries first, then the containers
        // thus, we add a container which should be come before the "first" alpha-numerically entry

        final String insertedKey = "a/b";
        writableProp.setProp(insertedKey, "value");

        final Iterator<String> iterator = roProp.keysIterator();

        final ArrayList<String> generatedKeys = generateKeys(FIRST_KEY, FIRST_AMOUNT, SECOND_KEY, SECOND_AMOUNT);
        generatedKeys.add(FIRST_AMOUNT + 1, insertedKey);

        assertIteratorEqual(iterator, generatedKeys, true);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testKeyIteratorUnsupportedRemove()
        throws Throwable
    {
        final Iterator<String> iterator = roProp.keysIterator();
        iterator.next();

        iterator.remove();

        fail("iterator.remove should not be supported");
    }

    @Test
    public void testValuesIterator() throws Throwable
    {
        // PropsContainer iterates the entries first, then the containers
        // thus, we add a container which should be come before the "first" alpha-numerically entry

        final String insertedValue = "value";
        writableProp.setProp("a/b", insertedValue);

        final Iterator<String> iterator = roProp.valuesIterator();

        final ArrayList<String> generatedValues = generateValues(FIRST_KEY, FIRST_AMOUNT, SECOND_KEY, SECOND_AMOUNT);
        generatedValues.add(FIRST_AMOUNT + 1, insertedValue);

        assertIteratorEqual(iterator, generatedValues, true);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testValuesIteratorUnsupportedRemove()
        throws Throwable
    {
        final Iterator<String> iterator = roProp.valuesIterator();
        iterator.next();

        iterator.remove();

        fail("iterator.remove should not be supported");
    }

    @Test
    public void testGetNamespace() throws Throwable
    {
        final String first = FIRST_KEY;
        final String second = SECOND_KEY;

        final String key = glue(first, second);
        final String value = "value";
        writableProp.setProp(key, value);
        assertEquals(value, roProp.getProp(key));

        final @Nullable Props firstNamespace = roProp.getNamespace(first);
        assertEquals(value, firstNamespace.getProp(second));

        assertNull(roProp.getNamespace("non existent"));

        writableProp.removeProp(key);
        assertNull(roProp.getNamespace(first));
    }

    @Test(expected = AccessDeniedException.class)
    public void testGetNamespaceSet() throws Throwable
    {
        final @Nullable Props firstNamespace = roProp.getNamespace(FIRST_KEY + "0");
        firstNamespace.setProp("key", "value");
    }

    /*
     * EntrySet
     */

    @Test
    public void testEntrySet() throws Throwable
    {
        final Set<Entry<String, String>> expectedEntries = new HashSet<>(
            generateEntries(FIRST_KEY, FIRST_AMOUNT, SECOND_KEY, SECOND_AMOUNT)
        );

        assertEquals(FIRST_AMOUNT * (SECOND_AMOUNT + 1) + 1, roEntrySet.size());
        assertEquals(expectedEntries, roEntrySet);
    }

    @Test
    public void testEntrySetInsertToProps() throws Throwable
    {
        writableProp.clear();

        Set<Entry<String, String>> entrySet = roProp.entrySet();

        assertEquals(0, entrySet.size());

        final String insertedKey = "test";
        final String insertedValue = "chicken";

        writableProp.setProp(insertedKey, insertedValue);

        assertEquals(1, entrySet.size());

        final Entry<String, String> expectedEntry = createEntry(insertedKey, insertedValue);
        final Entry<String, String> actualEntry = entrySet.iterator().next();

        assertEquals(expectedEntry, actualEntry);
    }

    @SuppressWarnings("unlikely-arg-type")
    @Test
    public void testEntrySetRemoveFromProps() throws Throwable
    {
        final String removedKey = FIRST_KEY + "0";

        assertTrue(roEntrySet.contains(removedKey));

        writableProp.removeProp(removedKey);

        assertFalse(roEntrySet.contains(removedKey));
    }

    @Test
    public void testEntrySetSize() throws Throwable
    {
        assertEquals(roProp.size(), roEntrySet.size());

        final String key = "key";
        writableProp.setProp(key, "value");
        assertEquals(roProp.size(), roEntrySet.size());

        writableProp.setProp(key, "other value");
        assertEquals(roProp.size(), roEntrySet.size());

        writableProp.removeProp(key);
        assertEquals(roProp.size(), roEntrySet.size());

        writableProp.clear();
        assertEquals(roProp.size(), roEntrySet.size());
    }

    @Test
    public void testEntrySetIsEmpty() throws Throwable
    {
        // we have filled roProp and keySet in the setup of JUnit
        assertFalse(roEntrySet.isEmpty());

        writableProp.clear();
        assertTrue(roEntrySet.isEmpty());

        writableProp.setProp("key", "value");
        assertFalse(roEntrySet.isEmpty());
    }

    @SuppressWarnings("unlikely-arg-type")
    @Test
    public void testEntrySetContains()
    {
        final ArrayList<String> generatedKeys = generateKeys(FIRST_KEY, FIRST_AMOUNT, SECOND_KEY, SECOND_AMOUNT);
        for (final String key : generatedKeys)
        {
            assertTrue(roEntrySet.contains(key));
        }
        assertFalse(roEntrySet.contains("non existent"));
    }

    @Test
    @SuppressWarnings("checkstyle:magicnumber")
    public void testEntrySetIterator() throws Throwable
    {
        // PropsContainer iterates the entries first, then the containers
        // thus, we add a container which should be come before the "first" alpha-numerically entry

        final String insertedKey = "a/b";
        final String insertedValue = "value";
        writableProp.setProp(insertedKey, insertedValue);

        final Iterator<Entry<String, String>> iterator = roEntrySet.iterator();

        final ArrayList<Entry<String, String>> generatedEntries = generateEntries(
            FIRST_KEY, FIRST_AMOUNT, SECOND_KEY, SECOND_AMOUNT
        );

        generatedEntries.add(4, createEntry(insertedKey, insertedValue));
        // insert the "a/b" key after "first2" and before "first0/second0"

        assertIteratorEqual(iterator, generatedEntries, true);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testEntrySetIteratorUnsupportedRemove()
        throws Throwable
    {
        final Iterator<Entry<String, String>> iterator = roEntrySet.iterator();

        iterator.next();

        iterator.remove();
    }

    @Test
    public void testEntrySetToArray()
    {
        final ArrayList<Entry<String, String>> generatedEntryList = generateEntries(
            FIRST_KEY, FIRST_AMOUNT, SECOND_KEY, SECOND_AMOUNT
        );
        assertArrayEquals(generatedEntryList.toArray(), roEntrySet.toArray());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testEntrySetToArrayParam()
    {
        final ArrayList<Entry<String, String>> generatedEntryList = generateEntries(
            FIRST_KEY, FIRST_AMOUNT, SECOND_KEY, SECOND_AMOUNT
        );
        final Entry<String, String>[] expectedEntries = new Entry[generatedEntryList.size()];
        generatedEntryList.toArray(expectedEntries);
        final Entry<String, String>[] actualEntries = new Entry[roEntrySet.size()];
        roEntrySet.toArray(actualEntries);

        assertArrayEquals(expectedEntries, actualEntries);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testEntrySetAdd() throws Throwable
    {
        // create a new entry with defined key and empty string as value
        final String insertedKey = "test";
        final String insertedValue = "testValue";
        final Entry<String, String> insertedEntry = createEntry(insertedKey, insertedValue);
        roEntrySet.add(insertedEntry);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testEntrySetRemove() throws Throwable
    {
        Entry<String, String> entryToRemove = createEntry(FIRST_KEY + "0", "0");
        roEntrySet.remove(entryToRemove);
    }

    @Test
    @SuppressWarnings({"unlikely-arg-type", "checkstyle:magicnumber"})
    public void testEntrySetContainsAll()
    {
        final ArrayList<String> generatedKeys = generateKeys(FIRST_KEY, FIRST_AMOUNT, SECOND_KEY, SECOND_AMOUNT);
        assertTrue(roEntrySet.containsAll(generatedKeys));

        generatedKeys.remove(3); // randomly diced :)
        assertTrue(roEntrySet.containsAll(generatedKeys));

        generatedKeys.add("unknown key");
        assertFalse(roEntrySet.containsAll(generatedKeys));

        assertTrue(roEntrySet.containsAll(new ArrayList<>())); // empty list
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testEntrySetAddAll()
    {
        final ArrayList<Entry<String, String>> entriesToAdd = new ArrayList<>();
        entriesToAdd.add(createEntry("new key", "value"));
        entriesToAdd.add(createEntry(FIRST_KEY + "0", "other value"));

        roEntrySet.addAll(entriesToAdd);
    }

    @SuppressWarnings("unlikely-arg-type")
    @Test(expected = UnsupportedOperationException.class)
    public void testEntrySetRetainAll()
    {
        final HashSet<String> retainedKeys = new HashSet<>();
        retainedKeys.add(FIRST_KEY + "0");
        retainedKeys.add(glue(FIRST_KEY + "1", SECOND_KEY + "2"));

        roEntrySet.retainAll(retainedKeys);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testEntrySetRemoveAll()
    {
        final HashSet<Entry<String, String>> generatedEntries = new HashSet<>(
            generateEntries(FIRST_KEY, FIRST_AMOUNT, SECOND_KEY, SECOND_AMOUNT)
        );

        final HashSet<Entry<String, String>> entriesToRemove = new HashSet<>();
        entriesToRemove.add(createEntry(FIRST_KEY + "0", "0"));
        entriesToRemove.add(createEntry(glue(FIRST_KEY + "1", SECOND_KEY + "2"), "1_2"));

        generatedEntries.removeAll(entriesToRemove);
        roEntrySet.removeAll(entriesToRemove);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testEntrySetClear()
    {
        roEntrySet.clear();
    }

    @Test
    @SuppressWarnings("unlikely-arg-type")
    public void testEntrySetEquals()
    {
        final HashSet<Entry<String, String>> clone = new HashSet<>(roEntrySet);

        assertTrue(roEntrySet.equals(roEntrySet));
        assertTrue(roEntrySet.equals(clone));
        assertFalse(roEntrySet.equals(null));
        assertFalse(roEntrySet.equals(roProp));

        clone.remove(createEntry(FIRST_KEY + "0", "0"));
        assertFalse(roEntrySet.equals(clone));
    }

    @Test
    public void testEntrySetHashCode() throws Throwable
    {
        final int origHashCode = roEntrySet.hashCode();

        final String insertedKey = "key";
        writableProp.setProp(insertedKey, "value");

        final int changedHashCode = roEntrySet.hashCode();

        assertNotEquals(origHashCode, changedHashCode);

        writableProp.removeProp(insertedKey);

        assertEquals(origHashCode, roEntrySet.hashCode());
    }

    /*
     * KeySet
     */

    @Test
    public void testKeySet() throws Throwable
    {
        final Set<String> expectedKeys = new HashSet<>(
            generateKeys(FIRST_KEY, FIRST_AMOUNT, SECOND_KEY, SECOND_AMOUNT)
        );

        assertEquals(FIRST_AMOUNT * (SECOND_AMOUNT + 1) + 1, roKeySet.size());
        assertEquals(expectedKeys, roKeySet);
    }

    @Test
    public void testKeySetInsertToProps() throws Throwable
    {
        writableProp.clear();

        Set<String> keySet = roProp.keySet();

        assertEquals(0, keySet.size());

        final String insertedKey = "test";
        final String insertedValue = "chicken";

        writableProp.setProp(insertedKey, insertedValue);

        assertEquals(1, keySet.size());

        final String actualKey = keySet.iterator().next();

        assertEquals(insertedKey, actualKey);
    }

    @Test
    public void testKeySetRemoveFromProps() throws Throwable
    {

        final String removedKey = FIRST_KEY + "0";

        assertTrue(roKeySet.contains(removedKey));

        writableProp.removeProp(removedKey);

        assertFalse(roKeySet.contains(removedKey));
    }

    @Test
    public void testKeySetSize() throws Throwable
    {
        assertEquals(roProp.size(), roKeySet.size());

        final String key = "key";
        writableProp.setProp(key, "value");
        assertEquals(roProp.size(), roKeySet.size());

        writableProp.setProp(key, "other value");
        assertEquals(roProp.size(), roKeySet.size());

        writableProp.removeProp(key);
        assertEquals(roProp.size(), roKeySet.size());

        writableProp.clear();
        assertEquals(roProp.size(), roKeySet.size());
    }

    @Test
    public void testKeySetIsEmpty() throws Throwable
    {
        // we have filled roProp and keySet in the setup of JUnit
        assertFalse(roKeySet.isEmpty());

        writableProp.clear();
        assertTrue(roKeySet.isEmpty());

        writableProp.setProp("key", "value");
        assertFalse(roKeySet.isEmpty());
    }

    @Test
    public void testKeySetContains()
    {
        final Collection<String> generatedKeys = generateKeys(FIRST_KEY, FIRST_AMOUNT, SECOND_KEY, SECOND_AMOUNT);
        for (final String key : generatedKeys)
        {
            assertTrue(roKeySet.contains(key));
        }
        assertFalse(roKeySet.contains("non existent"));
    }

    @Test
    @SuppressWarnings("checkstyle:magicnumber")
    public void testKeySetIterator() throws Throwable
    {
        // PropsContainer iterates the entries first, then the containers
        // thus, we add a container which should be come before the "first" alpha-numerically entry

        writableProp.setProp("a/b", "value");

        final Iterator<String> iterator = roProp.keySet().iterator();

        final ArrayList<String> generatedKeys = generateKeys(FIRST_KEY, FIRST_AMOUNT, SECOND_KEY, SECOND_AMOUNT);

        generatedKeys.add(4, "a/b"); // insert the "a/b" key after "first2" and before "first0/second0"

        assertIteratorEqual(iterator, generatedKeys, true);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testKeySetIteratorUnsupportedRemove()
        throws Throwable
    {
        final Iterator<String> iterator = roKeySet.iterator();

        iterator.next();

        iterator.remove();
    }

    @Test
    public void testKeySetToArray()
    {
        final ArrayList<String> generatedKeyList = generateKeys(FIRST_KEY, FIRST_AMOUNT, SECOND_KEY, SECOND_AMOUNT);
        assertArrayEquals(generatedKeyList.toArray(), roKeySet.toArray());
    }

    @Test
    public void testKeySetToArrayParam()
    {
        final ArrayList<String> generatedKeyList = generateKeys(FIRST_KEY, FIRST_AMOUNT, SECOND_KEY, SECOND_AMOUNT);
        final String[] expectedKeys = new String[generatedKeyList.size()];
        generatedKeyList.toArray(expectedKeys);
        final String[] actualKeys = new String[roKeySet.size()];
        roKeySet.toArray(actualKeys);

        assertArrayEquals(expectedKeys, actualKeys);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testKeySetAdd() throws Throwable
    {
        roKeySet.add("test");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testKeySetRemove() throws Throwable
    {
        roKeySet.remove(FIRST_KEY + "0");
    }

    @Test
    @SuppressWarnings("checkstyle:magicnumber")
    public void testKeySetContainsAll()
    {
        final ArrayList<String> generatedKeys = generateKeys(FIRST_KEY, FIRST_AMOUNT, SECOND_KEY, SECOND_AMOUNT);
        assertTrue(roKeySet.containsAll(generatedKeys));

        generatedKeys.remove(3); // randomly diced :)
        assertTrue(roKeySet.containsAll(generatedKeys));

        generatedKeys.add("unknown key");
        assertFalse(roKeySet.containsAll(generatedKeys));

        assertTrue(roKeySet.containsAll(new ArrayList<>())); // empty list
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testKeySetAddAll()
    {
        final ArrayList<String> keysToAdd = new ArrayList<>();
        keysToAdd.add("new key");
        keysToAdd.add(FIRST_KEY + "0");

        roKeySet.addAll(keysToAdd);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testKeySetRetainAll()
    {
        final HashSet<String> retainedKeys = new HashSet<>();
        retainedKeys.add(FIRST_KEY + "0");
        retainedKeys.add(glue(FIRST_KEY + "1", SECOND_KEY + "2"));

        roKeySet.retainAll(retainedKeys);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testKeySetRemoveAll()
    {
        final HashSet<String> keysToRemove = new HashSet<>();
        keysToRemove.add(FIRST_KEY + "0");
        keysToRemove.add(glue(FIRST_KEY + "1", SECOND_KEY + "2"));

        roKeySet.removeAll(keysToRemove);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testKeySetClear()
    {
        roKeySet.clear();
    }

    @Test
    @SuppressWarnings("unlikely-arg-type")
    public void testKeySetEquals()
    {
        final HashSet<String> clone = new HashSet<>(roKeySet);

        assertTrue(roKeySet.equals(roKeySet));
        assertTrue(roKeySet.equals(clone));
        assertFalse(roKeySet.equals(null));
        assertFalse(roKeySet.equals(roProp));

        clone.remove(FIRST_KEY + "0");
        assertFalse(roKeySet.equals(clone));
    }

    @Test
    public void testKeySetHashCode() throws Throwable
    {
        final int origHashCode = roKeySet.hashCode();

        final String insertedKey = "key";
        writableProp.setProp(insertedKey, "value");

        final int changedHashCode = roKeySet.hashCode();

        assertNotEquals(origHashCode, changedHashCode);

        writableProp.removeProp(insertedKey);

        assertEquals(origHashCode, roKeySet.hashCode());
    }

    /*
     * Map
     */

    @Test
    public void testMap() throws Throwable
    {
        final Set<Entry<String, String>> entrySet = roMap.entrySet();

        checkIfEntrySetIsValid(FIRST_KEY, SECOND_KEY, FIRST_AMOUNT, SECOND_AMOUNT, entrySet);
    }

    @Test
    public void testMapInsertIntoProps() throws Throwable
    {
        final String key = "key";
        final String value = "value";

        writableProp.setProp(key, value);

        assertEquals(value, roMap.get(key));
    }

    @Test
    public void testMapRemoveFromProps() throws Throwable
    {
        final String key = FIRST_KEY + "0";
        assertEquals("0", roMap.get(key));

        writableProp.removeProp(key);

        assertNull(roMap.get(key));
    }


    @Test
    public void testMapSize() throws Throwable
    {
        assertEquals(roProp.size(), roMap.size());

        final String key = "key";
        writableProp.setProp(key, "value");
        assertEquals(roProp.size(), roMap.size());

        writableProp.setProp(key, "other value");
        assertEquals(roProp.size(), roMap.size());

        writableProp.removeProp(FIRST_KEY + "0");
        assertEquals(roProp.size(), roMap.size());

        writableProp.removeProp("non existent");
        assertEquals(roProp.size(), roMap.size());

        writableProp.clear();
        assertEquals(roProp.size(), roMap.size());
    }

    @Test
    public void testMapIsEmpty() throws Throwable
    {
        // map is filled in setup
        assertFalse(roMap.isEmpty());

        writableProp.clear();
        assertTrue(roMap.isEmpty());

        final String insertedKey = "test";
        writableProp.setProp(insertedKey, insertedKey);
        assertFalse(roMap.isEmpty());

        writableProp.removeProp(insertedKey);
        assertTrue(roMap.isEmpty());
    }

    @Test
    public void testMapContainsKey() throws Throwable
    {
        final Collection<String> keys = generateKeys(FIRST_KEY, FIRST_AMOUNT, SECOND_KEY, SECOND_AMOUNT);
        for (final String key : keys)
        {
            assertTrue(roMap.containsKey(key));
        }
        assertFalse(roMap.containsKey("non existent"));

        final String removedKey = FIRST_KEY + "0";
        writableProp.removeProp(removedKey);
        assertFalse(roMap.containsKey(removedKey));
    }

    @Test
    public void testMapContainsValue() throws Throwable
    {
        final Collection<String> values = generateValues(FIRST_KEY, FIRST_AMOUNT, SECOND_KEY, SECOND_AMOUNT);
        for (final String value : values)
        {
            assertTrue(roMap.containsValue(value));
        }
        assertFalse(roMap.containsValue("non existent"));

        writableProp.removeProp(FIRST_KEY + "0");
        assertFalse(roMap.containsValue("0"));
    }

    @Test
    public void testMapGet() throws Throwable
    {
        final Collection<String> keys = generateKeys(FIRST_KEY, FIRST_AMOUNT, SECOND_KEY, SECOND_AMOUNT);
        for (final String key : keys)
        {
            assertEquals(roProp.getProp(key), roMap.get(key));
        }
        assertNull(roMap.get("non existant"));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testMapPut() throws Throwable
    {
        roMap.put("new key", "new value");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testMapRemove() throws Throwable
    {
        roMap.remove(FIRST_KEY + "0");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testMapPutAll() throws Throwable
    {
        final String[][] entriesToInsert = new String[][]
        {
            {"new", "value"},
            {glue(FIRST_KEY + "0", "other"), "value2"},
            {glue(FIRST_KEY + "0", SECOND_KEY + "1"), "override"}
        };

        final HashMap<String, String> mapToInsert = new HashMap<>();

        for (final String[] entry : entriesToInsert)
        {
            mapToInsert.put(entry[0], entry[1]);
        }

        roMap.putAll(mapToInsert);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testMapClear()
    {
        roMap.clear();
    }

    @Test
    @SuppressWarnings("unlikely-arg-type")
    public void testMapEquals() throws Throwable
    {
        // we assume that map contains the correct data
        // thus we clone all entries into a new hashmap, and call map.equals

        final HashMap<String, String> clone = new HashMap<>(roMap);

        assertTrue(roMap.equals(roMap));
        assertTrue(roMap.equals(clone));
        assertFalse(roMap.equals(null));
        assertFalse(roMap.equals(roProp));

        clone.remove(FIRST_KEY + "0");
        assertFalse(roMap.equals(clone));

        PropsContainer container = propsContainerFactory.getInstance(
            TEST_INSTANCE_NAME,
            "",
            LinStorObject.CTRL
        );
        fillProps(container, FIRST_KEY, FIRST_AMOUNT, SECOND_KEY, SECOND_AMOUNT);
        assertTrue(roMap.equals(container.map()));
    }

    @Test
    public void testMapHashCode() throws Throwable
    {
        final int origHashCode = roMap.hashCode();

        final String key = "key";
        final String value = "value";
        writableProp.setProp(key, value);

        final int changedHashCode = roMap.hashCode();

        assertNotEquals(origHashCode, changedHashCode);

        writableProp.removeProp(key);

        assertEquals(origHashCode, roMap.hashCode());
    }

    @Test
    public void testMapEntryGetKey() throws Throwable
    {
        writableProp.clear();
        final String key = "key";
        final String value = "value";
        writableProp.setProp(key, value);

        roMap = roProp.map();

        final Entry<String, String> entry = roMap.entrySet().iterator().next();

        assertEquals(key, entry.getKey());
    }

    @Test
    public void testMapEntryGetValue() throws Throwable
    {
        writableProp.clear();
        final String key = "key";
        final String value = "value";
        writableProp.setProp(key, value);

        roMap = roProp.map();

        final Entry<String, String> entry = roMap.entrySet().iterator().next();

        assertEquals(value, entry.getValue());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testMapEntrySetValue() throws Throwable
    {
        writableProp.clear();
        final String key = "key";
        final String value = "value";
        writableProp.setProp(key, value);

        roMap = roProp.map();

        final Entry<String, String> entry = roMap.entrySet().iterator().next();

        final String otherValue = "other";
        entry.setValue(otherValue);
    }

    @Test
    @SuppressWarnings("unlikely-arg-type")
    public void testMapEntryEquals() throws Throwable
    {
        writableProp.clear();
        final String key = "key";
        final String value = "value";
        writableProp.setProp(key, value);

        roMap = roProp.map();

        final Entry<String, String> entry = roMap.entrySet().iterator().next();

        assertTrue(entry.equals(entry));

        final HashMap<String, String> otherMap = new HashMap<>();
        otherMap.put(key, value);
        final Entry<String, String> otherEntry = otherMap.entrySet().iterator().next();

        assertTrue(entry.equals(otherEntry));
        assertFalse(entry.equals(null));
        assertFalse(entry.equals(otherMap));

        otherEntry.setValue("otherValue");
        assertFalse(entry.equals(otherEntry));
    }

    /*
     * Values
     */

    @Test
    public void testValueSet() throws Throwable
    {
        final Collection<String> expectedValues = generateValues(FIRST_KEY, FIRST_AMOUNT, SECOND_KEY, SECOND_AMOUNT);

        assertEquals(FIRST_AMOUNT * (SECOND_AMOUNT + 1) + 1, roValues.size());
        assertTrue(expectedValues.containsAll(roValues));
        assertTrue(roValues.containsAll(expectedValues));
    }

    @Test
    public void testValuesInsertToProps() throws Throwable
    {
        writableProp.clear();

        Collection<String> values = roProp.values();

        assertEquals(0, values.size());

        final String insertedKey = "test";
        final String insertedValue = "chicken";

        writableProp.setProp(insertedKey, insertedValue);

        assertEquals(1, values.size());

        final String actualValue = values.iterator().next();

        assertEquals(insertedValue, actualValue);
    }

    @Test
    public void testValuesRemoveFromProps() throws Throwable
    {
        final String removedKey = FIRST_KEY + "0";
        final String removedValue = "0";

        assertTrue(roValues.contains(removedValue));

        writableProp.removeProp(removedKey);

        assertFalse(roValues.contains(removedKey));
    }

    @Test
    public void testValuesSize() throws Throwable
    {
        assertEquals(roProp.size(), roValues.size());

        final String key = "key";
        writableProp.setProp(key, "value");
        assertEquals(roProp.size(), roValues.size());

        writableProp.setProp(key, "other value");
        assertEquals(roProp.size(), roValues.size());

        writableProp.removeProp(key);
        assertEquals(roProp.size(), roValues.size());

        writableProp.clear();
        assertEquals(roProp.size(), roValues.size());
    }

    @Test
    public void testValuesIsEmpty() throws Throwable
    {
        // we have filled roProp and keySet in the setup of JUnit
        assertFalse(roValues.isEmpty());

        writableProp.clear();
        assertTrue(roValues.isEmpty());

        writableProp.setProp("key", "value");
        assertFalse(roValues.isEmpty());
    }

    @Test
    public void testValuesContains()
    {
        final Collection<String> generatedValues = generateValues(FIRST_KEY, FIRST_AMOUNT, SECOND_KEY, SECOND_AMOUNT);
        for (final String value : generatedValues)
        {
            assertTrue(roValues.contains(value));
        }
        assertFalse(roValues.contains("non existent"));
    }

    @Test
    @SuppressWarnings("checkstyle:magicnumber")
    public void testValuesColIterator() throws Throwable
    {
        // PropsContainer iterates the entries first, then the containers
        // thus, we add a container which should be come before the "first" alpha-numerically entry

        writableProp.setProp("a/b", "value");

        final Iterator<String> iterator = roProp.values().iterator();

        final ArrayList<String> generatedValues = generateValues(FIRST_KEY, FIRST_AMOUNT, SECOND_KEY, SECOND_AMOUNT);

        generatedValues.add(4, "value");
        // insert the value of the "a/b" key after "first2" and before "first0/second0"

        assertIteratorEqual(iterator, generatedValues, true);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testValuesColIteratorUnsupportedRemove()
        throws Throwable
    {
        final Iterator<String> iterator = roValues.iterator();

        iterator.next();

        iterator.remove();

        fail("Values's iterator.remove should not be supported");
    }

    @Test
    public void testValuesToArray()
    {
        final ArrayList<String> generatedValueList = generateValues(
            FIRST_KEY, FIRST_AMOUNT, SECOND_KEY, SECOND_AMOUNT
        );
        assertArrayEquals(generatedValueList.toArray(), roValues.toArray());
    }

    @Test
    public void testValuesToArrayParam()
    {
        final ArrayList<String> generatedValueList = generateValues(
            FIRST_KEY, FIRST_AMOUNT, SECOND_KEY, SECOND_AMOUNT
        );
        final String[] expectedValues = new String[generatedValueList.size()];
        generatedValueList.toArray(expectedValues);
        final String[] actualValues = new String[roValues.size()];
        roValues.toArray(actualValues);

        assertArrayEquals(expectedValues, actualValues);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testValuesAdd() throws Throwable
    {
        roValues.add("some value");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testValuesRemove() throws Throwable
    {
        roValues.remove("0");
    }

    @Test
    @SuppressWarnings("checkstyle:magicnumber")
    public void testValuesContainsAll()
    {
        final ArrayList<String> generatedValues = generateValues(FIRST_KEY, FIRST_AMOUNT, SECOND_KEY, SECOND_AMOUNT);
        assertTrue(roValues.containsAll(generatedValues));

        generatedValues.remove(3); // randomly diced :)
        assertTrue(roValues.containsAll(generatedValues));

        generatedValues.add("unknown key");
        assertFalse(roValues.containsAll(generatedValues));

        assertTrue(roValues.containsAll(new ArrayList<>())); // empty list
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testValuesAddAll()
    {
        roValues.addAll(new ArrayList<>());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testValuesRetainAll()
    {
        final HashSet<String> retainedValues = new HashSet<>();
        retainedValues.add("0");
        retainedValues.add("1_2");

        roValues.retainAll(retainedValues);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testValuesRemoveAll()
    {
        final HashSet<String> generatedValues = new HashSet<>(
            generateValues(FIRST_KEY, FIRST_AMOUNT, SECOND_KEY, SECOND_AMOUNT)
        );

        final HashSet<String> valuesToRemove = new HashSet<>();
        valuesToRemove.add("0");
        valuesToRemove.add("1_2");

        generatedValues.removeAll(valuesToRemove);
        roValues.removeAll(valuesToRemove);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testValuesClear()
    {
        roValues.clear();
    }

    // roValues is currently based on Collections.unmodifiableCollection,
    // which does not implement equals (thus, default Object.equals is used)
    // however, Object.equals does not compare the data of the object

    /*
    @Test
    public void testValuesEquals()
    {
        final HashSet<String> clone = new HashSet<>(roValues);

        assertTrue(roValues.equals(roValues));
        assertTrue(roValues.equals(clone));
        assertFalse(roValues.equals(null));
        assertFalse(roValues.equals(roProp));

        clone.remove("0");
        assertFalse(roValues.equals(clone));
    }
    */

    // roValues is currently based on Collections.unmodifiableCollection,
    // which does not implement hashCode (thus, default Object.hashCode is used)
    // however, Object.hashCode gets calculated exactly once, regardless if the data
    // of the object changes.

    /*
    @Test
    public void testValuesHashCode() throws Throwable
    {
        final int origHashCode = roValues.hashCode();

        final String key = "key";
        writableProp.setProp(key, "value");


        assertNotEquals(origHashCode, roValues.hashCode());

        writableProp.removeProp(key);

        assertEquals(origHashCode, roValues.hashCode());
    }
    */
}
