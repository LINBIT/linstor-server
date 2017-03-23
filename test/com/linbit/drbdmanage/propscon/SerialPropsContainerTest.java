package com.linbit.drbdmanage.propscon;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

/**
 * These tests only cover the additional functionality. That means,
 * that only the extended / overridden methods are covered here.
 *
 * The base functionality is tested in {@link PropsContainerTest}
 *
 * @author Gabor Hernadi &lt;gabor.hernadi@linbit.com&gt;
 *
 */
public class SerialPropsContainerTest
{
    private final String serialKey = SerialGenerator.KEY_SERIAL;
    private SerialPropsContainer root;
    private SerialGenerator serialGenerator;

    @Before
    public void setUp() throws Exception
    {
        root = SerialPropsContainer.createRootContainer();
        serialGenerator = root.getSerialGenerator();
    }

    @Test
    public void testInitialSerialNumber() throws InvalidKeyException
    {
        assertSerial(0);
        assertEquals("0", root.getProp(serialKey));
    }

    @Test
    public void testSet() throws InvalidKeyException, InvalidValueException
    {
        final String key = "key";
        final String value = "value";
        final String otherKey = "otherKey";
        final String otherValue = "otherValue";
        assertSerial(0);
        root.setProp(key, value); // insert first entry
        assertSerial(1);
        root.setProp(otherKey, otherValue); // insert second entry
        assertSerial(1); // no closeGeneration called

        root.closeGeneration();
        root.setProp(key, otherValue); // override first entry
        assertSerial(2);

        root.closeGeneration();
        root.setProp(key, otherValue); // basically no change, but still write operation -> serial++
        assertSerial(3);
    }

    @Test
    public void testSetSerial() throws InvalidKeyException, InvalidValueException
    {
        assertSerial(0);
        root.setProp(serialKey, Integer.toString(-20));
        assertSerial(1);
    }

    @Test
    public void testSetAll() throws InvalidKeyException
    {
        final Map<String, String> map = new HashMap<>();
        map.put("a", "a");
        map.put("b", "b");
        map.put("a/a2", "aa2");
        map.put("", "root");

        root.setAllProps(map, null);

        assertSerial(1);
    }

    @Test
    public void testRemove() throws InvalidKeyException, InvalidValueException
    {
        final String key = "key";
        root.setProp(key, "value");
        assertSerial(1);

        root.closeGeneration();

        root.removeProp(key);
        assertSerial(2);

        root.closeGeneration();

        root.removeProp("unknown key");
        assertSerial(2);
    }

    @Test
    public void testRemoveSerial() throws InvalidKeyException
    {
        assertEquals("0", root.getProp(serialKey));
        root.removeProp(serialKey);
        assertEquals("1",root.getProp(serialKey));
    }

    @Test
    public void testClear() throws InvalidKeyException, InvalidValueException
    {
        root.setProp("key", "value");
        assertSerial(1);

        root.closeGeneration();

        root.clear();
        assertSerial(2);
    }

    /*
     * protected methods
     */



    @Test
    public void testRemoveAll() throws InvalidKeyException
    {
        final Map<String, String> map = new HashMap<>();
        final String key1 = "a";
        final String key2 = "a/a2";
        map.put(key1, "a");
        map.put("b", "b");
        map.put(key2, "aa2");
        map.put("", "root");

        root.setAllProps(map, null);

        assertSerial(1);

        final Set<String> toRemove = new HashSet<>();
        toRemove.add(key1);
        toRemove.add(key2);

        root.closeGeneration();

        root.removeAllProps(toRemove, null);

        assertSerial(2);
    }

    @Test
    public void testRetainAll() throws InvalidKeyException
    {
        final Map<String, String> map = new HashMap<>();
        final String key1 = "a";
        final String key2 = "a/a2";
        map.put(key1, "a");
        map.put("b", "b");
        map.put(key2, "aa2");
        map.put("", "root");

        root.setAllProps(map, null);

        assertSerial(1);

        final Set<String> toRemove = new HashSet<>();
        toRemove.add(key1);
        toRemove.add(key2);

        root.closeGeneration();
        root.retainAllProps(toRemove, null);

        assertSerial(1);
    }

    private void assertSerial(int currentSerial) throws InvalidKeyException
    {
        assertEquals(currentSerial, serialGenerator.peekSerial());
        assertEquals(Integer.toString(currentSerial), root.getProp(serialKey));
    }
}
