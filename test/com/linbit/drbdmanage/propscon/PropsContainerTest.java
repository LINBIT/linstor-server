package com.linbit.drbdmanage.propscon;

import static com.linbit.drbdmanage.propscon.CommonPropsTestUtils.*;
import static org.junit.Assert.*;

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

import com.linbit.drbdmanage.security.AccessDeniedException;

public class PropsContainerTest
{
    private PropsContainer root;
    private Map<String, String> map;
    private Set<String> keySet;
    private Set<Entry<String, String>> entrySet;
    private Collection<String> values;

    @Before
    public void setUp() throws Exception
    {
        root = PropsContainer.createRootContainer();

        fillProps(root, FIRST_KEY, FIRST_AMOUNT, SECOND_KEY, SECOND_AMOUNT);

        map = root.map();
        entrySet = root.entrySet();
        keySet = root.keySet();
        values = root.values();
    }

    @Test
    public void testSize() throws InvalidKeyException, InvalidValueException
    {
        final String firstKey = FIRST_KEY;
        final String firstValue = "value";

        final String secondKey = SECOND_KEY;
        final String secondValue = "other value";

        assertEquals(0, root.size());

        root.setProp(firstKey, firstValue);
        assertEquals(1, root.size());

        root.setProp(secondKey, secondValue);
        assertEquals(2, root.size());

        root.setProp(secondKey, firstValue);
        assertEquals(2, root.size());

        root.removeProp(secondKey);
        assertEquals(1, root.size());

        root.removeProp("nonExistent");
        assertEquals(1, root.size());
    }

    @Test
    public void testIsEmpty() throws InvalidKeyException, InvalidValueException
    {
        assertTrue(root.isEmpty());

        final String key = "key";
        final String value = "value";
        root.setProp(key, value);

        assertFalse(root.isEmpty());

        root.clear();
        assertTrue(root.isEmpty());

        root.setProp(key, value);
        assertFalse(root.isEmpty());
        root.removeProp(key);
        assertTrue(root.isEmpty());
    }

    @Test
    public void testGetProp() throws InvalidKeyException, InvalidValueException
    {
        final String key = "key";
        final String value = "value";

        root.setProp(key, value);

        assertEquals(value, root.getProp(key));
    }


    @Test
    public void testGetProbWithNamespace() throws InvalidKeyException, InvalidValueException
    {
        final String first = FIRST_KEY;
        final String second = SECOND_KEY;

        final String key = glue(first, second);
        final String value = "value";
        root.setProp(key, value);
        assertEquals(value, root.getProp(second, first));
    }

    @Test
    public void testGetRemovedEntry() throws InvalidKeyException, InvalidValueException
    {
        final String key = "key";
        final String value = "value";
        root.setProp(key, value);
        assertEquals(value, root.getProp(key));

        root.removeProp(key);
        assertNull(root.getProp(key));
    }


    @Test
    public void testSetWhitespaceEntry() throws InvalidKeyException, InvalidValueException
    {
        final String key = "key with blanks";
        final String value = "value with \nnewline";
        root.setProp(key, value);

        assertEquals(value, root.getProp(key));
    }

    @Test
    public void testSetReturnOldValue() throws InvalidKeyException, InvalidValueException
    {
        final String key = FIRST_KEY+"0";
        final String origValue = "value";
        root.setProp(key, origValue);

        final String otherValue = "otherValue";

        assertEquals(origValue, root.setProp(key, otherValue));
        assertEquals(otherValue, root.getProp(key));
    }

    @Test(expected = InvalidKeyException.class)
    public void testSetTooLongKey() throws InvalidKeyException, InvalidValueException
    {
        final StringBuilder sb = new StringBuilder();
        for (int idx = 0; idx < PropsContainer.PATH_MAX_LENGTH; idx++)
        {
            sb.append("a");
        }
        root.setProp(sb.toString(), "value");

        fail("Key should be too long");
    }

    @Test
    public void testSetDeepPaths() throws InvalidKeyException, InvalidValueException
    {
        final StringBuilder sb = new StringBuilder();
        for (int idx = 0; idx < (PropsContainer.PATH_MAX_LENGTH >> 1) - 1; idx++)
        {
            sb.append("a").append("/");
        }
        root.setProp(sb.toString(), "value");
    }

    @Test(expected = InvalidValueException.class)
    public void testSetInvalidValue() throws InvalidKeyException, InvalidValueException
    {
        root.setProp("key", null);

        fail("Null values should not be allowed");
    }

    @Test
    public void testSetSimpleEntryOverride() throws InvalidKeyException, InvalidValueException
    {
        final String key = "key";
        final String value = "value";
        final String value2 = "other value";
        root.setProp(key, value);
        assertEquals(value, root.getProp(key));
        root.setProp(key, value2);
    }

    @Test
    public void testSetEntryAndContainer() throws InvalidKeyException, InvalidValueException
    {
        final String key = "key";
        final String value = "value";
        root.setProp(key, value);
        assertEquals(value, root.getProp(key));

        final String subKey = "sub";
        final String absoluteSubKey = glue(key, subKey);
        final String subValue = "other value";
        root.setProp(absoluteSubKey, subValue);
        assertEquals(subValue, root.getProp(absoluteSubKey));
    }

    @Test
    public void testSetIntoSubcontainer() throws InvalidKeyException, InvalidValueException, AccessDeniedException
    {
        final String first = FIRST_KEY;
        final String second1 = "second1";
        final String second2 = "second2";

        final String key = glue("/", first, second1);
        final String firstValue = "value";
        final String secondValue = "other value";
        root.setProp(key, firstValue);

        final Props firstNamespace = root.getNamespace(first);
        firstNamespace.setProp(second2, secondValue);

        assertEquals(secondValue, firstNamespace.getProp(second2));
        assertEquals(secondValue, root.getProp(glue(first, second2)));
    }

    @Test
    public void testSetIntoDetachedContainer() throws InvalidKeyException, InvalidValueException, AccessDeniedException
    {
        root.setProp("a/b/c", "abc");

        final Props abNamespace = root.getNamespace("a/b");

        root.clear();

        abNamespace.setProp("c", "new abc");

        assertTrue(root.isEmpty());
    }

    @Test
    public void testSetAllProps() throws InvalidKeyException
    {
        final Map<String, String> map = new HashMap<>();
        map.put("a", "a");
        map.put("b", "b");
        map.put("a/a2", "aa2");
        map.put("", "root");

        root.setAllProps(map, null);

        final Set<Entry<String,String>> entrySet = root.entrySet();

        assertEquals(root.size(), map.size());

        for(final Entry<String, String> entry : entrySet)
        {
            assertTrue("Missing expected entry", map.remove(entry.getKey(), entry.getValue()));
        }
    }

    @Test
    public void testSetAllPropsWithNamespace() throws InvalidKeyException
    {
        final String namespace = "namespace";

        final Map<String, String> map = new HashMap<>();
        map.put("a", "a");
        map.put("b", "b");
        map.put("a/a2", "aa2");
        map.put("", "root");

        root.setAllProps(map, namespace);

        final Set<Entry<String,String>> entrySet = root.entrySet();

        assertEquals(root.size(), map.size());

        for(final Entry<String, String> entry : entrySet)
        {
            String key = entry.getKey();
            assertTrue(key.startsWith(namespace));
            key = key.substring(namespace.length() + 1); // cut the namepsace and the first /

            assertTrue("Missing expected entry", map.remove(key, entry.getValue()));
        }
    }


    @Test
    public void testRemoveSimpleEntry() throws InvalidKeyException, InvalidValueException, AccessDeniedException
    {
        final String key = "key";
        final String value = "value";
        root.setProp(key, value);

        final String removedValue = root.removeProp(key);

        assertEquals(value, removedValue);
        assertNull(root.getProp(key));
    }

    @Test
    public void testRemoveFromSubcontainer() throws InvalidKeyException, InvalidValueException, AccessDeniedException
    {
        final String first = FIRST_KEY;
        final String second = "second1";

        final String key = glue("/", first, second);
        final String value = "value";
        root.setProp(key, value);

        Props firstNamespace = root.getNamespace(first);

        final String removedValue = firstNamespace.removeProp(second);

        assertEquals(value, removedValue);
        assertNull(firstNamespace.getProp(second));
        assertNull(root.getProp(first));
    }

    @Test
    public void testRemoveWithNamespace() throws InvalidKeyException, InvalidValueException, AccessDeniedException
    {
        final String removedContainer = FIRST_KEY+"0";
        final String removedEntryKey = SECOND_KEY+"2";
        final String removedKey = glue(removedContainer, removedEntryKey);

        root.removeProp(removedEntryKey, removedContainer);

        assertNull(root.getProp(removedKey));

        final Props containerNamespace = root.getNamespace(removedContainer);
        assertNull(containerNamespace.getProp(removedEntryKey));
    }

    @Test
    public void testGetPath() throws InvalidKeyException, InvalidValueException
    {
        assertEquals("", root.getPath());

        root.setProp("a/b/c/d", "value");
        final Props namespaceA = root.getNamespace("a");

        assertEquals("a/", namespaceA.getPath());

        final Props namespaceB = namespaceA.getNamespace("b");
        assertEquals("a/b/", namespaceB.getPath());

        final Props namespaceC = root.getNamespace("a/b/c");
        assertEquals("a/b/c/", namespaceC.getPath());
    }

    @Test
    public void testGetPathTrailingSlash() throws InvalidKeyException, InvalidValueException
    {
        root.setProp("a/b/c/d", "value");
        final Props namespaceC = root.getNamespace("a/b/c");

        assertEquals(namespaceC, root.getNamespace("a/b/c/"));
    }

    @Test
    public void testIterator() throws InvalidKeyException, InvalidValueException, AccessDeniedException
    {
        // PropsContainer iterates the entries first, then the containers
        // thus, we add a container which should be come before the "first" alpha-numerically entry

        final String insertedKey = "a/b";
        final String insertedValue = "value";
        root.setProp(insertedKey, insertedValue);

        final Iterator<Entry<String, String>> iterator = root.iterator();

        final ArrayList<Entry<String, String>> generatedEntries = generateEntries(FIRST_KEY, FIRST_AMOUNT, SECOND_KEY, SECOND_AMOUNT);

        generatedEntries.add(FIRST_AMOUNT + 1, createEntry(insertedKey, insertedValue)); // insert the "a/b" key after "first2" and before "first0/second0"

        assertIteratorEqual(iterator, generatedEntries, true);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testIteratorUnsupportedRemove() throws InvalidKeyException, InvalidValueException, AccessDeniedException
    {
        final Iterator<Entry<String, String>> iterator = root.iterator();
        iterator.next();

        iterator.remove();

        fail("iterator.remove should not be supported");
    }

    @Test
    public void testKeyIterator() throws InvalidKeyException, InvalidValueException, AccessDeniedException
    {
        // PropsContainer iterates the entries first, then the containers
        // thus, we add a container which should be come before the "first" alpha-numerically entry

        final String insertedKey = "a/b";
        root.setProp(insertedKey , "value");

        final Iterator<String> iterator = root.keysIterator();

        final ArrayList<String> generatedKeys = generateKeys(FIRST_KEY, FIRST_AMOUNT, SECOND_KEY, SECOND_AMOUNT);
        generatedKeys.add(FIRST_AMOUNT + 1, insertedKey);

        assertIteratorEqual(iterator, generatedKeys, true);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testKeyIteratorUnsupportedRemove() throws InvalidKeyException, InvalidValueException, AccessDeniedException
    {
        final Iterator<String> iterator = root.keysIterator();
        iterator.next();

        iterator.remove();

        fail("iterator.remove should not be supported");
    }

    @Test
    public void testValuesIterator() throws InvalidKeyException, InvalidValueException, AccessDeniedException
    {
        // PropsContainer iterates the entries first, then the containers
        // thus, we add a container which should be come before the "first" alpha-numerically entry

        final String insertedValue = "value";
        root.setProp("a/b", insertedValue);

        final Iterator<String> iterator = root.valuesIterator();

        final ArrayList<String> generatedValues = generateValues(FIRST_KEY, FIRST_AMOUNT, SECOND_KEY, SECOND_AMOUNT);
        generatedValues.add(FIRST_AMOUNT + 1, insertedValue);

        assertIteratorEqual(iterator, generatedValues, true);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testValuesIteratorUnsupportedRemove() throws InvalidKeyException, InvalidValueException, AccessDeniedException
    {
        final Iterator<String> iterator = root.valuesIterator();
        iterator.next();

        iterator.remove();

        fail("iterator.remove should not be supported");
    }

    @Test
    public void testGetNamespace() throws InvalidKeyException, InvalidValueException
    {
        final String first = FIRST_KEY;
        final String second = SECOND_KEY;

        final String key = glue(first, second);
        final String value = "value";
        root.setProp(key, value);
        assertEquals(value, root.getProp(key));

        final Props firstNamespace = root.getNamespace(first);
        assertEquals(value, firstNamespace.getProp(second));

        assertNull(root.getNamespace("non existent"));

        root.removeProp(key);
        assertNull(root.getNamespace(first));
    }

    /*
     * protected methods
     */

    @Test
    public void testRemoveAllProps() throws InvalidKeyException
    {
        final Map<String, String> map = new HashMap<>();
        map.put("a", "a");
        map.put("b", "b");
        map.put("a/a2", "aa2");
        map.put("", "root");

        root.setAllProps(map, null);

        final Set<String> remove = new HashSet<>();
        remove.add("a");
        remove.add("");

        root.removeAllProps(remove, null);

        for(String removedKey : remove)
        {
            map.remove(removedKey);
        }

        final Set<Entry<String,String>> entrySet = root.entrySet();

        assertEquals(root.size(), map.size());

        for(final Entry<String, String> entry : entrySet)
        {
            assertTrue("Missing expected entry", map.remove(entry.getKey(), entry.getValue()));
        }
    }

    @Test
    public void testRemoveAllPropsWithNamespace() throws InvalidKeyException
    {
        final Map<String, String> map = new HashMap<>();
        map.put("a", "a");
        map.put("b", "b");
        map.put("a/a2", "aa2");
        map.put("", "root");

        root.setAllProps(map, null);

        final Set<String> remove = new HashSet<>();
        remove.add("a2");
        remove.add("");

        root.removeAllProps(remove, "a");

        map.remove("a/a2");

        final Set<Entry<String,String>> entrySet = root.entrySet();

        assertEquals(root.size(), map.size());

        for(final Entry<String, String> entry : entrySet)
        {
            assertTrue("Missing expected entry", map.remove(entry.getKey(), entry.getValue()));
        }
    }

    @Test
    public void testRetainAllProps() throws InvalidKeyException
    {
        final Map<String, String> map = new HashMap<>();
        map.put("a", "a");
        map.put("b", "b");
        map.put("a/a2", "aa2");
        map.put("", "root");

        root.setAllProps(map, null);

        final Set<String> remove = new HashSet<>();
        remove.add("a");
        remove.add("");

        root.retainAllProps(remove, null);

        map.remove("b");
        map.remove("a/a2");

        final Set<Entry<String,String>> entrySet = root.entrySet();

        assertEquals(root.size(), map.size());

        for(final Entry<String, String> entry : entrySet)
        {
            assertTrue("Missing expected entry", map.remove(entry.getKey(), entry.getValue()));
        }
    }


    // TODO maybe test ctor with a child as parent?

    /*
     * EntrySet
     */

    @Test
    public void testEntrySet() throws InvalidKeyException, InvalidValueException, AccessDeniedException
    {
        final Set<Entry<String, String>> expectedEntries = new HashSet<>(generateEntries(FIRST_KEY, FIRST_AMOUNT, SECOND_KEY, SECOND_AMOUNT));

        assertEquals(FIRST_AMOUNT * (SECOND_AMOUNT + 1) + 1, entrySet.size());
        assertEquals(expectedEntries, entrySet);
    }

    @Test
    public void testEntrySetInsertToProps() throws InvalidKeyException, InvalidValueException
    {
        root.clear();

        Set<Entry<String, String>> entrySet = root.entrySet();

        assertEquals(0, entrySet.size());

        final String insertedKey = "test";
        final String insertedValue = "chicken";

        root.setProp(insertedKey, insertedValue);

        assertEquals(1, entrySet.size());

        final Entry<String, String> expectedEntry = createEntry(insertedKey, insertedValue);
        final Entry<String, String> actualEntry = entrySet.iterator().next();

        assertEquals(expectedEntry, actualEntry);
    }

    @Test
    public void testEntrySetRemoveFromProps() throws InvalidKeyException, InvalidValueException, AccessDeniedException
    {
        final String removedKey = FIRST_KEY + "0";

        assertTrue(entrySet.contains(removedKey));

        root.removeProp(removedKey);

        assertFalse(entrySet.contains(removedKey));
    }

    @Test
    public void testEntrySetSize() throws InvalidKeyException, InvalidValueException
    {
        assertEquals(root.size(), entrySet.size());

        final String key = "key";
        root.setProp(key, "value");
        assertEquals(root.size(), entrySet.size());

        root.setProp(key, "other value");
        assertEquals(root.size(), entrySet.size());

        root.removeProp(key);
        assertEquals(root.size(), entrySet.size());

        root.clear();
        assertEquals(root.size(), entrySet.size());

        final Entry<String, String> entry = createEntry("key", "value");
        entrySet.add(entry);
        assertEquals(root.size(), entrySet.size());

        entrySet.remove(entry.getKey());
        assertEquals(root.size(), entrySet.size());
    }

    @Test
    public void testEntrySetIsEmpty()
    {
        // we have filled root and keySet in the setup of JUnit
        assertFalse(entrySet.isEmpty());

        entrySet.clear();
        assertTrue(entrySet.isEmpty());

        final Entry<String, String> entry = createEntry("key", "value");
        entrySet.add(entry);
        assertFalse(entrySet.isEmpty());
    }

    @Test
    public void testEntrySetContains()
    {
        final ArrayList<String> generatedKeys = generateKeys(FIRST_KEY, FIRST_AMOUNT, SECOND_KEY, SECOND_AMOUNT);
        for(final String key : generatedKeys)
        {
            assertTrue(entrySet.contains(key));
        }
        assertFalse(entrySet.contains("non existent"));
    }

    @Test
    public void testEntrySetIterator() throws InvalidKeyException, InvalidValueException, AccessDeniedException
    {
        // PropsContainer iterates the entries first, then the containers
        // thus, we add a container which should be come before the "first" alpha-numerically entry

        final String insertedKey = "a/b";
        final String insertedValue = "value";
        root.setProp(insertedKey, insertedValue);

        final Iterator<Entry<String, String>> iterator = root.entrySet().iterator();

        final ArrayList<Entry<String, String>> generatedEntries = generateEntries(FIRST_KEY, FIRST_AMOUNT, SECOND_KEY, SECOND_AMOUNT);

        generatedEntries.add(FIRST_AMOUNT + 1, createEntry(insertedKey, insertedValue)); // insert the "a/b" key after "first2" and before "first0/second0"

        assertIteratorEqual(iterator, generatedEntries, true);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testEntrySetIteratorUnsupportedRemove() throws InvalidKeyException, InvalidValueException, AccessDeniedException
    {
        final Iterator<Entry<String, String>> iterator = entrySet.iterator();

        iterator.next();

        iterator.remove();

        fail("EntrySet's iterator.remove should not be supported");
    }

    @Test
    public void testEntrySetToArray()
    {
        final ArrayList<Entry<String, String>> generatedEntryList = generateEntries(FIRST_KEY, FIRST_AMOUNT, SECOND_KEY, SECOND_AMOUNT);
        assertArrayEquals(generatedEntryList.toArray(), entrySet.toArray());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testEntrySetToArrayParam()
    {
        final ArrayList<Entry<String, String>> generatedEntryList = generateEntries(FIRST_KEY, FIRST_AMOUNT, SECOND_KEY, SECOND_AMOUNT);
        final Entry<String, String>[] expectedEntries = new Entry[generatedEntryList.size()];
        generatedEntryList.toArray(expectedEntries);
        final Entry<String, String>[] actualEntries = new Entry[entrySet.size()];
        entrySet.toArray(actualEntries);

        assertArrayEquals(expectedEntries, actualEntries);
    }

    @Test
    public void testEntrySetAdd() throws InvalidKeyException, InvalidValueException, AccessDeniedException
    {
        // create a new entry with defined key and empty string as value
        final String insertedKey = "test";
        final String insertedValue = "testValue";
        final Entry<String, String> insertedEntry = createEntry(insertedKey, insertedValue);
        entrySet.add(insertedEntry);
        assertEquals(insertedValue, root.getProp(insertedKey));

        final String insertedContainer = "container";
        final String insertedEntryKey = "test";
        final String insertedContainerKey = glue(insertedContainer, insertedEntryKey);
        final String insertedContainerValue = "containerValue";
        final Entry<String, String> insertedContainerEntry = createEntry(insertedContainerKey, insertedContainerValue);

        entrySet.add(insertedContainerEntry);
        assertEquals(insertedContainerValue, root.getProp(insertedContainerKey));
        assertEquals(insertedContainerValue, root.getProp(insertedEntryKey, insertedContainer));
        final Props containerNamespace = root.getNamespace(insertedContainer);
        assertEquals(insertedContainerValue, containerNamespace.getProp(insertedEntryKey));

        // do not override existing key
        final String existingKey = FIRST_KEY + "0";
        final String overriddenValue = "override";
        entrySet.add(createEntry(existingKey, overriddenValue));
        assertNotEquals(overriddenValue, root.getProp(existingKey));
    }

    @Test
    public void testEntrySetRemove() throws InvalidKeyException, InvalidValueException, AccessDeniedException
    {
        final Set<Entry<String, String>> expectedEntries = new HashSet<>(generateEntries(FIRST_KEY, FIRST_AMOUNT, SECOND_KEY, SECOND_AMOUNT));

        assertEquals(FIRST_AMOUNT * (SECOND_AMOUNT + 1) + 1, entrySet.size());
        assertEquals(expectedEntries, entrySet);

        final String keyToRemove = FIRST_KEY+"0";
        Entry<String, String> entryToRemove = createEntry(keyToRemove, "0");
        entrySet.remove(entryToRemove);
        expectedEntries.remove(entryToRemove);
        assertEquals(expectedEntries, entrySet);

        entrySet.remove(createEntry("nonExistent", "some value"));
        assertEquals(expectedEntries, entrySet);
    }

    @Test
    public void testEntrySetContainsAll()
    {
        //        final ArrayList<Entry<String, String>> generatedEntries = generateEntries(FIRST_KEY, FIRST_AMOUNT, SECOND_KEY, SECOND_AMOUNT);
        final ArrayList<String> generatedKeys = generateKeys(FIRST_KEY, FIRST_AMOUNT, SECOND_KEY, SECOND_AMOUNT);
        assertTrue(entrySet.containsAll(generatedKeys));

        generatedKeys.remove(3); // randomly diced :)
        assertTrue(entrySet.containsAll(generatedKeys));

        //        generatedEntries.add(createEntry("unknown key", "some value"));
        generatedKeys.add("unknown key");
        assertFalse(entrySet.containsAll(generatedKeys));

        assertTrue(entrySet.containsAll(new ArrayList<>())); // empty list
    }

    @Test
    public void testEntrySetAddAll()
    {
        final ArrayList<Entry<String, String>> generatedEntries = generateEntries(FIRST_KEY, FIRST_AMOUNT, SECOND_KEY, SECOND_AMOUNT);
        final ArrayList<Entry<String, String>> entriesToAdd = new ArrayList<>();
        entriesToAdd.add(createEntry("new key", "value"));
        entriesToAdd.add(createEntry(FIRST_KEY+"0", "other value"));

        entrySet.addAll(entriesToAdd);
        generatedEntries.addAll(entriesToAdd);

        assertEquals(generatedEntries, entrySet);
    }

    @Test
    public void testEntrySetRetainAll()
    {
        final HashSet<String> retainedKeys = new HashSet<>();
        retainedKeys.add(FIRST_KEY + "0");
        retainedKeys.add(glue(FIRST_KEY + "1",SECOND_KEY + "2"));

        entrySet.retainAll(retainedKeys);

        assertTrue(entrySet.containsAll(retainedKeys));
        assertTrue(retainedKeys.containsAll(entrySet));
    }

    @Test
    public void testEntrySetRemoveAll()
    {
        final HashSet<Entry<String, String>> generatedEntries = new HashSet<>(generateEntries(FIRST_KEY, FIRST_AMOUNT, SECOND_KEY, SECOND_AMOUNT));

        final HashSet<Entry<String, String>> entriesToRemove = new HashSet<>();
        entriesToRemove.add(createEntry(FIRST_KEY + "0", "0"));
        entriesToRemove.add(createEntry(glue(FIRST_KEY + "1",SECOND_KEY + "2"), "1_2"));

        generatedEntries.removeAll(entriesToRemove);
        entrySet.removeAll(entriesToRemove);

        assertEquals(generatedEntries, entrySet);
    }

    @Test
    public void testEntrySetClear()
    {
        assertFalse(entrySet.isEmpty());
        entrySet.clear();
        assertTrue(entrySet.isEmpty());
    }

    @Test
    public void testEntrySetEquals()
    {
        final HashSet<Entry<String, String>> clone = new HashSet<>(entrySet);

        assertTrue(entrySet.equals(entrySet));
        assertTrue(entrySet.equals(clone));
        assertFalse(entrySet.equals(null));
        assertFalse(entrySet.equals(root));

        clone.remove(createEntry(FIRST_KEY+"0","0"));
        assertFalse(entrySet.equals(clone));
    }

    @Test
    public void testEntrySetHashCode()
    {
        final int origHashCode = entrySet.hashCode();

        final Entry<String, String> entry = createEntry("key", "value");
        entrySet.add(entry);

        final int changedHashCode = entrySet.hashCode();

        assertNotEquals(origHashCode, changedHashCode);

        entrySet.remove(entry);

        assertEquals(origHashCode, entrySet.hashCode());
    }

    /*
     * KeySet
     */

    @Test
    public void testKeySet() throws InvalidKeyException, InvalidValueException, AccessDeniedException
    {
        final Set<String> expectedKeys = new HashSet<>(generateKeys(FIRST_KEY, FIRST_AMOUNT, SECOND_KEY, SECOND_AMOUNT));

        assertEquals(FIRST_AMOUNT * (SECOND_AMOUNT + 1) + 1, keySet.size());
        assertEquals(expectedKeys, keySet);
    }

    @Test
    public void testKeySetInsertToProps() throws InvalidKeyException, InvalidValueException
    {
        root.clear();

        Set<String> keySet = root.keySet();

        assertEquals(0, keySet.size());

        final String insertedKey = "test";
        final String insertedValue = "chicken";

        root.setProp(insertedKey, insertedValue);

        assertEquals(1, keySet.size());

        final String actualKey = keySet.iterator().next();

        assertEquals(insertedKey, actualKey);
    }

    @Test
    public void testKeySetRemoveFromProps() throws InvalidKeyException, InvalidValueException, AccessDeniedException
    {

        final String removedKey = FIRST_KEY + "0";

        assertTrue(keySet.contains(removedKey));

        root.removeProp(removedKey);

        assertFalse(keySet.contains(removedKey));
    }

    @Test
    public void testKeySetSize() throws InvalidKeyException, InvalidValueException
    {
        assertEquals(root.size(), keySet.size());

        final String key = "key";
        root.setProp(key, "value");
        assertEquals(root.size(), keySet.size());

        root.setProp(key, "other value");
        assertEquals(root.size(), keySet.size());

        root.removeProp(key);
        assertEquals(root.size(), keySet.size());

        root.clear();
        assertEquals(root.size(), keySet.size());

        keySet.add(key);
        assertEquals(root.size(), keySet.size());

        keySet.remove(key);
        assertEquals(root.size(), keySet.size());
    }

    @Test
    public void testKeySetIsEmpty()
    {
        // we have filled root and keySet in the setup of JUnit
        assertFalse(keySet.isEmpty());

        keySet.clear();
        assertTrue(keySet.isEmpty());

        keySet.add("key");
        assertFalse(keySet.isEmpty());
    }

    @Test
    public void testKeySetContains()
    {
        final Collection<String> generatedKeys = generateKeys(FIRST_KEY, FIRST_AMOUNT, SECOND_KEY, SECOND_AMOUNT);
        for(final String key : generatedKeys)
        {
            assertTrue(keySet.contains(key));
        }
        assertFalse(keySet.contains("non existent"));
    }

    @Test
    public void testKeySetIterator() throws InvalidKeyException, InvalidValueException, AccessDeniedException
    {
        // PropsContainer iterates the entries first, then the containers
        // thus, we add a container which should be come before the "first" alpha-numerically entry

        root.setProp("a/b", "value");

        final Iterator<String> iterator = root.keySet().iterator();

        final ArrayList<String> generatedKeys = generateKeys(FIRST_KEY, FIRST_AMOUNT, SECOND_KEY, SECOND_AMOUNT);

        generatedKeys.add(FIRST_AMOUNT + 1, "a/b"); // insert the "a/b" key after "first2" and before "first0/second0"

        assertIteratorEqual(iterator, generatedKeys, true);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testKeySetIteratorUnsupportedRemove() throws InvalidKeyException, InvalidValueException, AccessDeniedException
    {
        final Iterator<String> iterator = keySet.iterator();

        iterator.next();

        iterator.remove();

        fail("KeySet's iterator.remove should not be supported");
    }

    @Test
    public void testKeySetToArray()
    {
        final ArrayList<String> generatedKeyList = generateKeys(FIRST_KEY, FIRST_AMOUNT, SECOND_KEY, SECOND_AMOUNT);
        assertArrayEquals(generatedKeyList.toArray(), keySet.toArray());
    }

    @Test
    public void testKeySetToArrayParam()
    {
        final ArrayList<String> generatedKeyList = generateKeys(FIRST_KEY, FIRST_AMOUNT, SECOND_KEY, SECOND_AMOUNT);
        final String[] expectedKeys = new String[generatedKeyList.size()];
        generatedKeyList.toArray(expectedKeys);
        final String[] actualKeys = new String[keySet.size()];
        keySet.toArray(actualKeys);

        assertArrayEquals(expectedKeys, actualKeys);
    }

    @Test
    public void testKeySetAdd() throws InvalidKeyException, InvalidValueException, AccessDeniedException
    {
        // create a new entry with defined key and empty string as value
        final String insertedKey = "test";
        keySet.add(insertedKey);
        assertEquals("", root.getProp(insertedKey));

        final String insertedContainer = "container";
        final String insertedEntryKey = "test";
        final String insertedContainerkey = glue(insertedContainer, insertedEntryKey);
        keySet.add(insertedContainerkey);
        assertEquals("", root.getProp(insertedContainerkey));
        assertEquals("", root.getProp(insertedEntryKey, insertedContainer));
        final Props containerNamespace = root.getNamespace(insertedContainer);
        assertEquals("", containerNamespace.getProp(insertedEntryKey));

        // do not override existing key
        final String existingKey = FIRST_KEY + "0";
        keySet.add(existingKey);
        assertNotEquals("", root.getProp(existingKey));
    }

    @Test
    public void testKeySetRemove() throws InvalidKeyException, InvalidValueException, AccessDeniedException
    {
        final Set<String> expectedKeys = new HashSet<>(generateKeys(FIRST_KEY, FIRST_AMOUNT, SECOND_KEY, SECOND_AMOUNT));

        assertEquals(FIRST_AMOUNT * (SECOND_AMOUNT + 1) + 1, keySet.size());
        assertEquals(expectedKeys, keySet);

        final String keyToRemove = FIRST_KEY+"0";
        keySet.remove(keyToRemove);
        expectedKeys.remove(keyToRemove);
        assertEquals(expectedKeys, keySet);

        keySet.remove("non existent");
        assertEquals(expectedKeys, keySet);
    }

    @Test
    public void testKeySetContainsAll()
    {
        final ArrayList<String> generatedKeys = generateKeys(FIRST_KEY, FIRST_AMOUNT, SECOND_KEY, SECOND_AMOUNT);
        assertTrue(keySet.containsAll(generatedKeys));

        generatedKeys.remove(3); // randomly diced :)
        assertTrue(keySet.containsAll(generatedKeys));

        generatedKeys.add("unknown key");
        assertFalse(keySet.containsAll(generatedKeys));

        assertTrue(keySet.containsAll(new ArrayList<>())); // empty list
    }

    @Test
    public void testKeySetAddAll()
    {
        final HashSet<String> generatedKeys = new HashSet<>(generateKeys(FIRST_KEY, FIRST_AMOUNT, SECOND_KEY, SECOND_AMOUNT));
        final ArrayList<String> keysToAdd = new ArrayList<>();
        keysToAdd.add("new key");
        keysToAdd.add(FIRST_KEY+"0");

        keySet.addAll(keysToAdd);
        generatedKeys.addAll(keysToAdd);

        assertEquals(generatedKeys, keySet);
    }

    @Test
    public void testKeySetRetainAll()
    {
        final HashSet<String> retainedKeys = new HashSet<>();
        retainedKeys.add(FIRST_KEY + "0");
        retainedKeys.add(glue(FIRST_KEY + "1",SECOND_KEY + "2"));

        keySet.retainAll(retainedKeys);

        assertTrue(keySet.containsAll(retainedKeys));
        assertTrue(retainedKeys.containsAll(keySet));
    }

    @Test
    public void testKeySetRemoveAll()
    {
        final HashSet<String> generatedKeys = new HashSet<>(generateKeys(FIRST_KEY, FIRST_AMOUNT, SECOND_KEY, SECOND_AMOUNT));

        final HashSet<String> keysToRemove = new HashSet<>();
        keysToRemove.add(FIRST_KEY + "0");
        keysToRemove.add(glue(FIRST_KEY + "1",SECOND_KEY + "2"));

        generatedKeys.removeAll(keysToRemove);
        keySet.removeAll(keysToRemove);

        assertEquals(generatedKeys, keySet);
    }

    @Test
    public void testKeySetClear()
    {
        assertFalse(keySet.isEmpty());
        keySet.clear();
        assertTrue(keySet.isEmpty());
    }

    @Test
    public void testKeySetEquals()
    {
        final HashSet<String> clone = new HashSet<>(keySet);

        assertTrue(keySet.equals(keySet));
        assertTrue(keySet.equals(clone));
        assertFalse(keySet.equals(null));
        assertFalse(keySet.equals(root));

        clone.remove(FIRST_KEY+"0");
        assertFalse(keySet.equals(clone));
    }

    @Test
    public void testKeySetHashCode()
    {
        final int origHashCode = keySet.hashCode();

        final String key = "key";
        keySet.add(key);

        final int changedHashCode = keySet.hashCode();

        assertNotEquals(origHashCode, changedHashCode);

        keySet.remove(key);

        assertEquals(origHashCode, keySet.hashCode());
    }

    /*
     * Map
     */

    @Test
    public void testMap() throws InvalidKeyException, InvalidValueException, AccessDeniedException
    {
        final Set<Entry<String, String>> entrySet = map.entrySet();

        checkIfEntrySetIsValid(FIRST_KEY, SECOND_KEY, FIRST_AMOUNT, SECOND_AMOUNT, entrySet);
    }

    @Test
    public void testMapInsertIntoProps() throws InvalidKeyException, InvalidValueException
    {
        final String key = "key";
        final String value = "value";

        root.setProp(key, value);

        assertEquals(value, map.get(key));
    }

    @Test
    public void testMapRemoveFromProps() throws InvalidKeyException, InvalidValueException, AccessDeniedException
    {
        final String key = FIRST_KEY+"0";
        assertEquals("0", map.get(key));

        root.removeProp(key);

        assertNull(map.get(key));
    }


    @Test
    public void testMapSize() throws InvalidKeyException, InvalidValueException, AccessDeniedException
    {
        assertEquals(root.size(), map.size());

        final String key = "key";
        root.setProp(key, "value");
        assertEquals(root.size(), map.size());

        root.setProp(key, "other value");
        assertEquals(root.size(), map.size());

        root.removeProp(FIRST_KEY+"0");
        assertEquals(root.size(), map.size());

        root.removeProp("non existent");
        assertEquals(root.size(), map.size());

        root.clear();
        assertEquals(root.size(), map.size());
    }

    @Test
    public void testMapIsEmpty()
    {
        // map is filled in setup
        assertFalse(map.isEmpty());

        map.clear();
        assertTrue(map.isEmpty());

        map.put("test", "test");
        assertFalse(map.isEmpty());

        map.remove("test");
        assertTrue(map.isEmpty());
    }

    @Test
    public void testMapContainsKey()
    {
        final Collection<String> keys = generateKeys(FIRST_KEY, FIRST_AMOUNT, SECOND_KEY, SECOND_AMOUNT);
        for(final String key : keys)
        {
            assertTrue(map.containsKey(key));
        }
        assertFalse(map.containsKey("non existent"));

        final String removedKey = FIRST_KEY+"0";
        map.remove(removedKey);
        assertFalse(map.containsKey(removedKey));
    }

    @Test
    public void testMapContainsValue()
    {
        final Collection<String> values = generateValues(FIRST_KEY, FIRST_AMOUNT, SECOND_KEY, SECOND_AMOUNT);
        for(final String value : values)
        {
            assertTrue(map.containsValue(value));
        }
        assertFalse(map.containsValue("non existent"));

        map.remove(FIRST_KEY + "0");
        assertFalse(map.containsValue("0"));
    }

    @Test
    public void testMapGet() throws InvalidKeyException
    {
        final Collection<String> keys = generateKeys(FIRST_KEY, FIRST_AMOUNT, SECOND_KEY, SECOND_AMOUNT);
        for(final String key : keys)
        {
            assertEquals(root.getProp(key), map.get(key));
        }
        assertNull(map.get("non existant"));
    }

    @Test
    public void testMapPut() throws InvalidKeyException, InvalidValueException, AccessDeniedException
    {
        final String insertedKey = "new key";
        final String insertedValue = "new value";
        map.put(insertedKey, insertedValue);

        assertEquals(insertedValue, root.getProp(insertedKey));

        final String container = "new";
        final String containerKey = "key";
        final String insertedContainerKey = glue(container, containerKey);
        final String insertedContainerValue = "old value";
        map.put(insertedContainerKey, insertedContainerValue);

        assertEquals(insertedContainerValue, root.getProp(insertedContainerKey));
        Props containerNamespace = root.getNamespace(container);
        assertEquals(insertedContainerValue, containerNamespace.getProp(containerKey));
    }

    @Test
    public void testMapRemove() throws InvalidKeyException, InvalidValueException, AccessDeniedException
    {
        final String removedEntryKey = FIRST_KEY+"0";
        map.remove(removedEntryKey);
        assertNull(root.getProp(removedEntryKey));


        final String removedNamespace = FIRST_KEY + "0";
        final String removedContainerKey = glue(removedNamespace, SECOND_KEY + "2");
        map.remove(removedContainerKey);
        assertNull(root.getProp(removedContainerKey));

        map.remove(glue(removedNamespace, "second0"));
        map.remove(glue(removedNamespace, "second1"));
        // remove the other two entries, thus the namespace should get deleted too
        assertNull(root.getNamespace(removedNamespace));
    }

    @Test
    public void testMapPutAll() throws InvalidKeyException
    {
        final String[][] entriesToInsert = new String[][]{
            {"new", "value"},
            {glue(FIRST_KEY+"0", "other"), "value2"},
            {glue(FIRST_KEY+"0", SECOND_KEY+"1"), "override"}
        };

        final HashMap<String, String> mapToInsert = new HashMap<>();

        for(final String[] entry : entriesToInsert)
        {
            mapToInsert.put(entry[0], entry[1]);
        }

        map.putAll(mapToInsert);

        for(final String[] entry : entriesToInsert)
        {
            assertEquals(entry[1], map.get(entry[0]));
            assertEquals(entry[1], root.getProp(entry[0]));
        }
    }

    @Test
    public void testMapClear()
    {
        assertFalse(map.isEmpty());

        map.clear();
        assertTrue(map.isEmpty());
    }

    @Test
    public void testMapEquals() throws InvalidKeyException, InvalidValueException, AccessDeniedException
    {
        // we assume that map contains the correct data
        // thus we clone all entries into a new hashmap, and call map.equals

        final HashMap<String, String> clone = new HashMap<>(map);

        assertTrue(map.equals(map));
        assertTrue(map.equals(clone));
        assertFalse(map.equals(null));
        assertFalse(map.equals(root));

        clone.remove(FIRST_KEY+"0");
        assertFalse(map.equals(clone));

        PropsContainer container = PropsContainer.createRootContainer();
        fillProps(container, FIRST_KEY, FIRST_AMOUNT, SECOND_KEY, SECOND_AMOUNT);
        assertTrue(map.equals(container.map()));
    }

    @Test
    public void testMapHashCode()
    {
        final int origHashCode = map.hashCode();

        final String key = "key";
        final String value = "value";
        map.put(key, value);

        final int changedHashCode = map.hashCode();

        assertNotEquals(origHashCode, changedHashCode);

        map.remove(key);

        assertEquals(origHashCode, map.hashCode());
    }

    @Test
    public void testMapEntryGetKey() throws InvalidKeyException, InvalidValueException
    {
        root.clear();
        final String key = "key";
        final String value = "value";
        root.setProp(key, value);

        map = root.map();

        final Entry<String, String> entry = map.entrySet().iterator().next();

        assertEquals(key, entry.getKey());
    }

    @Test
    public void testMapEntryGetValue() throws InvalidKeyException, InvalidValueException
    {
        root.clear();
        final String key = "key";
        final String value = "value";
        root.setProp(key, value);

        map = root.map();

        final Entry<String, String> entry = map.entrySet().iterator().next();

        assertEquals(value, entry.getValue());
    }

    @Test
    public void testMapEntrySetValue() throws InvalidKeyException, InvalidValueException
    {
        root.clear();
        final String key = "key";
        final String value = "value";
        root.setProp(key, value);

        map = root.map();

        final Entry<String, String> entry = map.entrySet().iterator().next();

        final String otherValue = "other";
        entry.setValue(otherValue);

        assertEquals(otherValue, map.get(key));
        assertEquals(otherValue, root.getProp(key));
    }

    @Test
    public void testMapEntryEquals() throws InvalidKeyException, InvalidValueException
    {
        root.clear();
        final String key = "key";
        final String value = "value";
        root.setProp(key, value);

        map = root.map();

        final Entry<String, String> entry = map.entrySet().iterator().next();

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

    @Test
    public void testMapEntryHashCode()
    {
        final Entry<String, String> entry = map.entrySet().iterator().next();
        final int origHashCode = entry.hashCode();

        final String originalValue = entry.getValue();
        entry.setValue("otherValue");

        final int changedHashCode = entry.hashCode();

        assertNotEquals(origHashCode, changedHashCode);

        entry.setValue(originalValue);

        assertEquals(origHashCode, map.hashCode());
    }

    /*
     * Values
     */

    @Test
    public void testValueSet() throws InvalidKeyException, InvalidValueException, AccessDeniedException
    {
        final Collection<String> expectedValues = generateValues(FIRST_KEY, FIRST_AMOUNT, SECOND_KEY, SECOND_AMOUNT);

        assertEquals(FIRST_AMOUNT * (SECOND_AMOUNT + 1) + 1, values.size());
        assertTrue(expectedValues.containsAll(values));
        assertTrue(values.containsAll(expectedValues));
    }

    @Test
    public void testValuesInsertToProps() throws InvalidKeyException, InvalidValueException
    {
        root.clear();

        Collection<String> values = root.values();

        assertEquals(0, values.size());

        final String insertedKey = "test";
        final String insertedValue = "chicken";

        root.setProp(insertedKey, insertedValue);

        assertEquals(1, values.size());

        final String actualValue = values.iterator().next();

        assertEquals(insertedValue, actualValue);
    }

    @Test
    public void testValuesRemoveFromProps() throws InvalidKeyException, InvalidValueException, AccessDeniedException
    {
        final String removedKey = FIRST_KEY + "0";
        final String removedValue = "0";

        assertTrue(values.contains(removedValue));

        root.removeProp(removedKey);

        assertFalse(values.contains(removedKey));
    }

    @Test
    public void testValuesSize() throws InvalidKeyException, InvalidValueException
    {
        assertEquals(root.size(), values.size());

        final String key = "key";
        root.setProp(key, "value");
        assertEquals(root.size(), values.size());

        root.setProp(key, "other value");
        assertEquals(root.size(), values.size());

        root.removeProp(key);
        assertEquals(root.size(), values.size());

        root.clear();
        assertEquals(root.size(), values.size());
    }

    @Test
    public void testValuesIsEmpty() throws InvalidKeyException, InvalidValueException
    {
        // we have filled root and keySet in the setup of JUnit
        assertFalse(values.isEmpty());

        values.clear();
        assertTrue(values.isEmpty());

        root.setProp("key", "value");
        assertFalse(values.isEmpty());
    }

    @Test
    public void testValuesContains()
    {
        final Collection<String> generatedValues = generateValues(FIRST_KEY, FIRST_AMOUNT, SECOND_KEY, SECOND_AMOUNT);
        for(final String value : generatedValues)
        {
            assertTrue(values.contains(value));
        }
        assertFalse(values.contains("non existent"));
    }

    @Test
    public void testValuesColIterator() throws InvalidKeyException, InvalidValueException, AccessDeniedException
    {
        // PropsContainer iterates the entries first, then the containers
        // thus, we add a container which should be come before the "first" alpha-numerically entry

        root.setProp("a/b", "value");

        final Iterator<String> iterator = root.values().iterator();

        final ArrayList<String> generatedValues = generateValues(FIRST_KEY, FIRST_AMOUNT, SECOND_KEY, SECOND_AMOUNT);

        generatedValues.add(FIRST_AMOUNT + 1, "value"); // insert the value of the "a/b" key after "first2" and before "first0/second0"

        assertIteratorEqual(iterator, generatedValues, true);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testValuesColIteratorUnsupportedRemove() throws InvalidKeyException, InvalidValueException, AccessDeniedException
    {
        final Iterator<String> iterator = values.iterator();

        iterator.next();

        iterator.remove();

        fail("Values's iterator.remove should not be supported");
    }

    @Test
    public void testValuesToArray()
    {
        final ArrayList<String> generatedValueList = generateValues(FIRST_KEY, FIRST_AMOUNT, SECOND_KEY, SECOND_AMOUNT);
        assertArrayEquals(generatedValueList.toArray(), values.toArray());
    }

    @Test
    public void testValuesToArrayParam()
    {
        final ArrayList<String> generatedValueList = generateValues(FIRST_KEY, FIRST_AMOUNT, SECOND_KEY, SECOND_AMOUNT);
        final String[] expectedValues = new String[generatedValueList.size()];
        generatedValueList.toArray(expectedValues);
        final String[] actualValues = new String[values.size()];
        values.toArray(actualValues);

        assertArrayEquals(expectedValues, actualValues);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testValuesAdd() throws InvalidKeyException, InvalidValueException, AccessDeniedException
    {
        values.add("some value");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testValuesRemove() throws InvalidKeyException, InvalidValueException, AccessDeniedException
    {
        values.remove("0");
    }

    @Test
    public void testValuesContainsAll()
    {
        final ArrayList<String> generatedValues = generateValues(FIRST_KEY, FIRST_AMOUNT, SECOND_KEY, SECOND_AMOUNT);
        assertTrue(values.containsAll(generatedValues));

        generatedValues.remove(3); // randomly diced :)
        assertTrue(values.containsAll(generatedValues));

        generatedValues.add("unknown key");
        assertFalse(values.containsAll(generatedValues));

        assertTrue(values.containsAll(new ArrayList<>())); // empty list
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testValuesAddAll()
    {
        values.addAll(new ArrayList<String>());
    }

    @Test
    public void testValuesRetainAll()
    {
        final HashSet<String> retainedValues = new HashSet<>();
        retainedValues.add("0");
        retainedValues.add("1_2");

        values.retainAll(retainedValues);

        assertTrue(values.containsAll(retainedValues));
        assertTrue(retainedValues.containsAll(values));
    }

    @Test
    public void testValuesRemoveAll()
    {
        final HashSet<String> generatedValues = new HashSet<>(generateValues(FIRST_KEY, FIRST_AMOUNT, SECOND_KEY, SECOND_AMOUNT));

        final HashSet<String> valuesToRemove = new HashSet<>();
        valuesToRemove.add("0");
        valuesToRemove.add("1_2");

        generatedValues.removeAll(valuesToRemove);
        values.removeAll(valuesToRemove);

        assertEquals(generatedValues, values);
    }

    @Test
    public void testValuesClear()
    {
        assertFalse(values.isEmpty());
        values.clear();
        assertTrue(values.isEmpty());
    }

    @Test
    public void testValuesEquals()
    {
        final HashSet<String> clone = new HashSet<>(values);

        assertTrue(values.equals(values));
        assertTrue(values.equals(clone));
        assertFalse(values.equals(null));
        assertFalse(values.equals(root));

        clone.remove("0");
        assertFalse(values.equals(clone));
    }

    @Test
    public void testValuesHashCode() throws InvalidKeyException, InvalidValueException
    {
        final int origHashCode = values.hashCode();

        final String key = "key";
        root.setProp(key, "value");

        final int changedHashCode = values.hashCode();

        assertNotEquals(origHashCode, changedHashCode);

        root.removeProp(key);

        assertEquals(origHashCode, values.hashCode());
    }
}
