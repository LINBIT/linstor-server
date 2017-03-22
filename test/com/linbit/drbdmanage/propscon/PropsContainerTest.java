package com.linbit.drbdmanage.propscon;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.Set;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.linbit.drbdmanage.security.AccessDeniedException;

public class PropsContainerTest
{
    private PropsContainer root;

    @Before
    public void setUp() throws Exception
    {
        root = PropsContainer.createRootContainer();
    }

    @After
    public void tearDown() throws Exception
    {
    }

    @Test
    public void testSetSimpleEntry() throws InvalidKeyException, InvalidValueException
    {
        final String key = "key";
        final String value = "value";
        root.setProp(key, value);

        Assert.assertEquals(value, root.getProp(key));
    }

    @Test
    public void testSetWhitespaceEntry() throws InvalidKeyException, InvalidValueException
    {
        final String key = "key with blanks";
        final String value = "value with \nnewline";
        root.setProp(key, value);

        Assert.assertEquals(value, root.getProp(key));
    }

    @Test(expected = InvalidKeyException.class)
    public void testTooLongKey() throws InvalidKeyException, InvalidValueException
    {
        final StringBuilder sb = new StringBuilder();
        for (int idx = 0; idx < PropsContainer.PATH_MAX_LENGTH; idx++)
        {
            sb.append("a");
        }
        root.setProp(sb.toString(), "value");
    }

    @Test
    public void testDeepPaths() throws InvalidKeyException, InvalidValueException
    {
        final StringBuilder sb = new StringBuilder();
        for (int idx = 0; idx < (PropsContainer.PATH_MAX_LENGTH >> 1) - 1; idx++)
        {
            sb.append("a").append("/");
        }
        root.setProp(sb.toString(), "value");
    }

    @Test(expected = InvalidValueException.class)
    public void testInvalidValue() throws InvalidKeyException, InvalidValueException
    {
        root.setProp("key", null);
    }

    @Test
    public void testSetSimpleEntryOverride() throws InvalidKeyException, InvalidValueException
    {
        final String key = "key";
        final String value = "value";
        final String value2 = "other value";
        root.setProp(key, value);
        Assert.assertEquals(value, root.getProp(key));
        root.setProp(key, value2);
    }

    @Test
    public void testSetEntryAndContainer() throws InvalidKeyException, InvalidValueException
    {
        final String key = "key";
        final String value = "value";
        root.setProp(key, value);
        Assert.assertEquals(value, root.getProp(key));

        final String subKey = "sub";
        final String absoluteSubKey = glue(key, subKey);
        final String subValue = "other value";
        root.setProp(absoluteSubKey, subValue);
        Assert.assertEquals(subValue, root.getProp(absoluteSubKey));
    }

    @Test
    public void testGetSubcontainer() throws InvalidKeyException, InvalidValueException
    {
        final String first = "first";
        final String second = "second";

        final String key = glue(first, second);
        final String value = "value";
        root.setProp(key, value);
        Assert.assertEquals(value, root.getProp(key));

        final Props firstNamespace = root.getNamespace(first);
        Assert.assertEquals(value, firstNamespace.getProp(second));
    }

    @Test
    public void testGetProbWithNamespace() throws InvalidKeyException, InvalidValueException
    {
        final String first = "first";
        final String second = "second";

        final String key = glue(first, second);
        final String value = "value";
        root.setProp(key, value);
        Assert.assertEquals(value, root.getProp(second, first));
    }

    @Test
    public void testSetIntoSubcontainer() throws InvalidKeyException, InvalidValueException, AccessDeniedException
    {
        final String first = "first";
        final String second1 = "second1";
        final String second2 = "second2";

        final String key = glue("/", first, second1);
        final String firstValue = "value";
        final String secondValue = "other value";
        root.setProp(key, firstValue);

        final Props firstNamespace = root.getNamespace(first);
        firstNamespace.setProp(second2, secondValue);

        Assert.assertEquals(secondValue, firstNamespace.getProp(second2));
        Assert.assertEquals(secondValue, root.getProp(glue(first, second2)));
    }

    @Test
    public void testSetIntoDetachedContainer() throws InvalidKeyException, InvalidValueException, AccessDeniedException
    {
        root.setProp("a/b/c", "abc");
        
        final Props abNamespace = root.getNamespace("a/b");
        
        root.clear();
        
        abNamespace.setProp("c", "new abc");
        
        Assert.assertTrue(root.isEmpty());
    }
    
    @Test
    public void testRemoveSimpleEntry() throws InvalidKeyException, InvalidValueException, AccessDeniedException
    {
        final String key = "key";
        final String value = "value";
        root.setProp(key, value);

        final String removedValue = root.removeProp(key);

        Assert.assertEquals(value, removedValue);
        Assert.assertEquals(null, root.getProp(key));
    }

    @Test
    public void testRemoveFromSubcontainer() throws InvalidKeyException, InvalidValueException, AccessDeniedException
    {
        final String first = "first";
        final String second = "second1";

        final String key = glue("/", first, second);
        final String value = "value";
        root.setProp(key, value);

        Props firstNamespace = root.getNamespace(first);

        final String removedValue = firstNamespace.removeProp(second);

        Assert.assertEquals(value, removedValue);
        Assert.assertEquals(null, firstNamespace.getProp(second));
        Assert.assertEquals(null, root.getProp(first));
    }

    @Test
    public void testEntrySet() throws InvalidKeyException, InvalidValueException
    {
        final String firstPrefix = "first";
        final String secondPrefix = "second";

        final int firstAmount = 3;
        final int secondAmount = 3;

        fillRoot(firstPrefix, firstAmount, secondPrefix, secondAmount);

        final Set<Entry<String, String>> entrySet = root.entrySet();

        checkIfEntrySetIsValid(firstPrefix, secondPrefix, firstAmount, secondAmount, entrySet);
    }

    private void checkIfEntrySetIsValid(
        final String firstPrefix, 
        final String secondPrefix, 
        final int firstAmount,
        final int secondAmount, 
        final Set<Entry<String, String>> entrySet)
    {
        Assert.assertEquals(firstAmount * (secondAmount + 1) + 1, entrySet.size());

        final Pattern keyPattern = Pattern.compile(
            "(?:" + firstPrefix + "(\\d+))?/?(?:" + secondPrefix + "(\\d+))?");

        for (final Entry<String, String> entry : entrySet)
        {
            final String key = entry.getKey();
            final String value = entry.getValue();
            Matcher matcher = keyPattern.matcher(key);
            if (matcher.find())
            {
                final String firstNr = matcher.group(1);
                final String secondNr = matcher.group(2);

                if (firstNr == null)
                {
                    Assert.assertEquals("/ was added an empty string", "", value);
                }
                else
                {
                    if (secondNr == null)
                    {
                        Assert.assertEquals(firstNr, value);
                    }
                    else
                    {
                        Assert.assertEquals(glueWithDelimiter("_", firstNr, secondNr), value);
                    }
                }
            }
            else
            {
                Assert.fail("Unrecognized key");
            }
        }
    }

    @Test
    public void testKeySet() throws InvalidKeyException, InvalidValueException
    {
        final String firstPrefix = "first";
        final String secondPrefix = "second";

        final int firstAmount = 3;
        final int secondAmount = 3;

        fillRoot(firstPrefix, firstAmount, secondPrefix, secondAmount);
        final Set<String> expectedKeys = generateKeys(firstPrefix, firstAmount, secondPrefix, secondAmount);

        final Set<String> keySet = root.keySet();

        Assert.assertEquals(firstAmount * (secondAmount + 1) + 1, keySet.size());
        Assert.assertEquals(expectedKeys, keySet);
    }

    @Test
    public void testValueSet() throws InvalidKeyException, InvalidValueException
    {
        final String firstPrefix = "first";
        final String secondPrefix = "second";

        final int firstAmount = 3;
        final int secondAmount = 3;

        fillRoot(firstPrefix, firstAmount, secondPrefix, secondAmount);
        final Collection<String> expectedValues = generateValues(firstPrefix, firstAmount, secondPrefix, secondAmount);

        final Collection<String> values = root.values();

        Assert.assertEquals(firstAmount * (secondAmount + 1) + 1, values.size());
        Assert.assertTrue(expectedValues.containsAll(values));
        Assert.assertTrue(values.containsAll(expectedValues));
    }

    @Test
    public void testMap() throws InvalidKeyException, InvalidValueException
    {
        final String firstPrefix = "first";
        final String secondPrefix = "second";

        final int firstAmount = 3;
        final int secondAmount = 3;

        fillRoot(firstPrefix, firstAmount, secondPrefix, secondAmount);
        
        final Map<String, String> map = root.map();
        final Set<Entry<String, String>> entrySet = map.entrySet();
    
        checkIfEntrySetIsValid(firstPrefix, secondPrefix, firstAmount, secondAmount, entrySet);
    }

    @Test
    public void testSize() throws InvalidKeyException, InvalidValueException
    {
        final String firstKey = "first";
        final String firstValue = "value";
        
        final String secondKey = "second";
        final String secondValue = "other value";

        root.setProp(firstKey, firstValue);
        Assert.assertEquals(1, root.size());
        
        root.setProp(secondKey, secondValue);
        Assert.assertEquals(2, root.size());

        root.setProp(secondKey, firstValue);
        Assert.assertEquals(2, root.size());
        
        root.removeProp(secondKey);
        Assert.assertEquals(1, root.size());
    
        Assert.assertEquals(root.size(), root.entrySet().size());
        Assert.assertEquals(root.size(), root.keySet().size());
        Assert.assertEquals(root.size(), root.values().size());
    }

    @Test
    public void testEmpty() throws InvalidKeyException, InvalidValueException
    {
        final String key = "first";
        final String value = "value";

        Assert.assertTrue(root.isEmpty());
        
        root.setProp(key, value);
        Assert.assertFalse(root.isEmpty());
        
        root.removeProp(key);
        Assert.assertTrue(root.isEmpty());
    }

    @Test
    public void testClear() throws InvalidKeyException, InvalidValueException
    {
        Assert.assertTrue(root.isEmpty());
        
        root.setProp("a", "a");
        root.setProp("b", "b");
        root.setProp("c", "c");
        Assert.assertFalse(root.isEmpty());
        
        root.clear();
        Assert.assertTrue(root.isEmpty());
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
        
        Assert.assertEquals(root.size(), map.size());
        
        for(final Entry<String, String> entry : entrySet)
        {
            Assert.assertTrue("Missing expected entry", map.remove(entry.getKey(), entry.getValue()));
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
        
        Assert.assertEquals(root.size(), map.size());
        
        for(final Entry<String, String> entry : entrySet)
        {
            String key = entry.getKey();
            Assert.assertTrue(key.startsWith(namespace));
            key = key.substring(namespace.length() + 1); // cut the namepsace and the first / 
            
            Assert.assertTrue("Missing expected entry", map.remove(key, entry.getValue()));
        }
    }

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
        
        Assert.assertEquals(root.size(), map.size());
        
        for(final Entry<String, String> entry : entrySet)
        {
            Assert.assertTrue("Missing expected entry", map.remove(entry.getKey(), entry.getValue()));
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
        
        Assert.assertEquals(root.size(), map.size());
        
        for(final Entry<String, String> entry : entrySet)
        {
            Assert.assertTrue("Missing expected entry", map.remove(entry.getKey(), entry.getValue()));
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
        
        Assert.assertEquals(root.size(), map.size());
        
        for(final Entry<String, String> entry : entrySet)
        {
            Assert.assertTrue("Missing expected entry", map.remove(entry.getKey(), entry.getValue()));
        }
    }

    @Test
    public void testGetPath() throws InvalidKeyException, InvalidValueException
    {
        Assert.assertEquals("", root.getPath());
        
        root.setProp("a/b/c/d", "value");
        final Props namespaceA = root.getNamespace("a");
        
        Assert.assertEquals("a/", namespaceA.getPath());
        
        final Props namespaceB = namespaceA.getNamespace("b");
        Assert.assertEquals("a/b/", namespaceB.getPath());
        
        final Props namespaceC = root.getNamespace("a/b/c");
        Assert.assertEquals("a/b/c/", namespaceC.getPath());
    }
    
    @Test
    public void testGetPathTrailingSlash() throws InvalidKeyException, InvalidValueException
    {
        root.setProp("a/b/c/d", "value");
        final Props namespaceC = root.getNamespace("a/b/c");

        Assert.assertEquals(namespaceC, root.getNamespace("a/b/c/"));
    }
    
    @Test
    public void testIterator() throws InvalidKeyException, InvalidValueException
    {
        final String firstPrefix = "first";
        final String secondPrefix = "second";

        final int firstAmount = 3;
        final int secondAmount = 3;

        fillRoot(firstPrefix, firstAmount, secondPrefix, secondAmount);
        
        // PropsContainer iterates the entries first, then the containers
        // thus, we add a container which should be come before the "first" alpha-numerically entry
        
        root.setProp("a/b", "value");
        
        final Iterator<Entry<String, String>> iterator = root.iterator();

        assertNextEntryEqual(iterator, "", "");
        assertNextEntryEqual(iterator, firstPrefix+"0", "0"); // entries first
        assertNextEntryEqual(iterator, firstPrefix+"1", "1");
        assertNextEntryEqual(iterator, firstPrefix+"2", "2");
        assertNextEntryEqual(iterator, "a/b", "value"); // "a/b" < "firstX/..."
        assertNextEntryEqual(iterator, firstPrefix+"0/"+secondPrefix+"0", "0_0");
        assertNextEntryEqual(iterator, firstPrefix+"0/"+secondPrefix+"1", "0_1");
        assertNextEntryEqual(iterator, firstPrefix+"0/"+secondPrefix+"2", "0_2");
        assertNextEntryEqual(iterator, firstPrefix+"1/"+secondPrefix+"0", "1_0");
        assertNextEntryEqual(iterator, firstPrefix+"1/"+secondPrefix+"1", "1_1");
        assertNextEntryEqual(iterator, firstPrefix+"1/"+secondPrefix+"2", "1_2");
        assertNextEntryEqual(iterator, firstPrefix+"2/"+secondPrefix+"0", "2_0");
        assertNextEntryEqual(iterator, firstPrefix+"2/"+secondPrefix+"1", "2_1");
        assertNextEntryEqual(iterator, firstPrefix+"2/"+secondPrefix+"2", "2_2");
        
        Assert.assertFalse(iterator.hasNext());
    }
    
    private void assertNextEntryEqual(
        final Iterator<Entry<String, String>> iterator, 
        final String expectedKey, 
        final String expectedValue)
    {
        final Entry<String, String> entry = iterator.next();
        final String actualKey = entry.getKey();
        final String actualValue = entry.getValue();
        
        Assert.assertEquals(expectedKey, actualKey);
        Assert.assertEquals(expectedValue, actualValue);
    }

    @Test
    public void testKeyIterator() throws InvalidKeyException, InvalidValueException
    {
        final String firstPrefix = "first";
        final String secondPrefix = "second";

        final int firstAmount = 3;
        final int secondAmount = 3;

        fillRoot(firstPrefix, firstAmount, secondPrefix, secondAmount);
        
        // PropsContainer iterates the entries first, then the containers
        // thus, we add a container which should be come before the "first" alpha-numerically entry
        
        root.setProp("a/b", "value");
        
        final Iterator<String> iterator = root.keysIterator();
        
        Assert.assertEquals("", iterator.next());
        Assert.assertEquals(firstPrefix+"0", iterator.next()); // entries first
        Assert.assertEquals(firstPrefix+"1", iterator.next());
        Assert.assertEquals(firstPrefix+"2", iterator.next());
        Assert.assertEquals("a/b", iterator.next()); // "a/b" < "firstX/..."
        Assert.assertEquals(firstPrefix+"0/"+secondPrefix+"0", iterator.next());
        Assert.assertEquals(firstPrefix+"0/"+secondPrefix+"1", iterator.next());
        Assert.assertEquals(firstPrefix+"0/"+secondPrefix+"2", iterator.next());
        Assert.assertEquals(firstPrefix+"1/"+secondPrefix+"0", iterator.next());
        Assert.assertEquals(firstPrefix+"1/"+secondPrefix+"1", iterator.next());
        Assert.assertEquals(firstPrefix+"1/"+secondPrefix+"2", iterator.next());
        Assert.assertEquals(firstPrefix+"2/"+secondPrefix+"0", iterator.next());
        Assert.assertEquals(firstPrefix+"2/"+secondPrefix+"1", iterator.next());
        Assert.assertEquals(firstPrefix+"2/"+secondPrefix+"2", iterator.next());
        
        Assert.assertFalse(iterator.hasNext());
    }

    @Test
    public void testValuesIterator() throws InvalidKeyException, InvalidValueException
    {
        final String firstPrefix = "first";
        final String secondPrefix = "second";

        final int firstAmount = 3;
        final int secondAmount = 3;

        fillRoot(firstPrefix, firstAmount, secondPrefix, secondAmount);
        
        // PropsContainer iterates the entries first, then the containers
        // thus, we add a container which should be come before the "first" alpha-numerically entry
        
        root.setProp("a/b", "value");
        
        final Iterator<String> iterator = root.valuesIterator();
        
        Assert.assertEquals("", iterator.next());
        Assert.assertEquals("0", iterator.next()); // entries first
        Assert.assertEquals("1", iterator.next());
        Assert.assertEquals("2", iterator.next());
        Assert.assertEquals("value", iterator.next()); // "a/b" < "firstX/..."
        Assert.assertEquals("0_0", iterator.next());
        Assert.assertEquals("0_1", iterator.next());
        Assert.assertEquals("0_2", iterator.next());
        Assert.assertEquals("1_0", iterator.next());
        Assert.assertEquals("1_1", iterator.next());
        Assert.assertEquals("1_2", iterator.next());
        Assert.assertEquals("2_0", iterator.next());
        Assert.assertEquals("2_1", iterator.next());
        Assert.assertEquals("2_2", iterator.next());
        
        Assert.assertFalse(iterator.hasNext());
    }
    

    private String glue(String... strings)
    {
        return glueWithDelimiter("/", strings);
    }

    private String glueWithDelimiter(String delimiter, String... strings)
    {
        StringBuilder sb = new StringBuilder();
        for (String string : strings)
        {
            sb.append(string).append(delimiter);
        }
        sb.setLength(sb.length() - delimiter.length());

        return sb.toString();
    }


    private void fillRoot(String firstPrefix, int firstAmount, String secondPrefix, int secondAmount)
        throws InvalidKeyException, InvalidValueException
    {
        root.setProp("/", "");
        for (int firstIdx = 0; firstIdx < firstAmount; firstIdx++)
        {
            final String firstNr = Integer.toString(firstIdx);
            root.setProp(firstPrefix + firstNr, firstNr);
            for (int secondIdx = 0; secondIdx < secondAmount; secondIdx++)
            {
                final String secondNr = Integer.toString(secondIdx);
                root.setProp(
                    glue(firstPrefix + firstNr, secondPrefix + secondNr),
                    glueWithDelimiter("_", firstNr, secondNr));
            }
        }
    }

    private Set<String> generateKeys(String firstPrefix, int firstAmount, String secondPrefix, int secondAmount)
    {
        Set<String> keys = new HashSet<>();
        keys.add(""); // root.setProp("/", "");
        for (int firstIdx = 0; firstIdx < firstAmount; firstIdx++)
        {
            final String firstNr = Integer.toString(firstIdx);
            keys.add(firstPrefix + firstNr);
            for (int secondIdx = 0; secondIdx < secondAmount; secondIdx++)
            {
                final String secondNr = Integer.toString(secondIdx);
                keys.add(glue(firstPrefix + firstNr, secondPrefix + secondNr));
            }
        }
        return keys;
    }

    private Collection<String> generateValues(String firstPrefix, int firstAmount, String secondPrefix, int secondAmount)
    {
        Collection<String> values = new ArrayList<>();
        values.add(""); // root.setProp("/", "");
        for (int firstIdx = 0; firstIdx < firstAmount; firstIdx++)
        {
            final String firstNr = Integer.toString(firstIdx);
            values.add(firstNr);
            for (int secondIdx = 0; secondIdx < secondAmount; secondIdx++)
            {
                final String secondNr = Integer.toString(secondIdx);
                values.add(glueWithDelimiter("_", firstNr, secondNr));
            }
        }
        return values;
    }
    
}
