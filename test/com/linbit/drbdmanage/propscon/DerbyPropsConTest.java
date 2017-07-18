package com.linbit.drbdmanage.propscon;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.junit.Test;

import com.linbit.TransactionMgr;

public class DerbyPropsConTest extends DerbyPropsConBase
{
    @SuppressWarnings("resource")
    @Test
    public void testPersistSimple() throws Throwable
    {
        truncate();
        Connection con = getConnection();
        TransactionMgr transMgr = new TransactionMgr(con);
        PropsContainer container = PropsContainer.getInstance(dbDriver, transMgr);

        String expectedKey = "key";
        String expectedValue = "value";
        container.setProp(expectedKey, expectedValue);
        con.commit();

        ResultSet resultSet = getAllProps();

        assertTrue("No entries found in the database", resultSet.next());
        String instanceName = resultSet.getString(1);
        String key = resultSet.getString(2);
        String value = resultSet.getString(3);

        assertEquals(DEFAULT_INSTANCE_NAME, instanceName);
        assertEquals(expectedKey, key);
        assertEquals(expectedValue, value);

        assertFalse("Unknown entries found in the database", resultSet.next());
        resultSet.close();
    }

    @SuppressWarnings("resource")
    @Test
    public void testPersistNested() throws Throwable
    {
        Connection con = getConnection();
        TransactionMgr transMgr = new TransactionMgr(con);
        PropsContainer container = PropsContainer.getInstance(dbDriver, transMgr);

        Map<String, String> map = new HashMap<>();
        map.put("a", "c");
        map.put("a/b", "d");

        container.setAllProps(map, null);
        con.commit();

        checkIfPresent(map, DEFAULT_INSTANCE_NAME);
    }

    @SuppressWarnings("resource")
    @Test
    public void testPersistUpdate() throws Throwable
    {
        Connection con = getConnection();
        TransactionMgr transMgr = new TransactionMgr(con);
        PropsContainer container = PropsContainer.getInstance(dbDriver, transMgr);

        Map<String, String> map = new HashMap<>();
        map.put("a", "c");

        container.setAllProps(map, null);
        con.commit();

        // we don't have to test the results, as testPersistSimple handles that case

        map.put("a/b", "d");
        container.setAllProps(map, null);
        con.commit();

        checkIfPresent(map, DEFAULT_INSTANCE_NAME);

        map.remove("a/b");
        container.removeProp("a/b");
        con.commit();

        checkIfPresent(map, DEFAULT_INSTANCE_NAME);
    }

    @SuppressWarnings("resource")
    @Test
    public void testPersistMultipleContainer() throws Throwable
    {
        String expectedInstanceName1 = "INSTANCE_1";
        String expectedInstanceName2 = "INSTANCE_2";

        Connection con1 = getConnection();
        Connection con2 = getConnection();
        PropsConDerbyDriver driver1 = new PropsConDerbyDriver(expectedInstanceName1);
        PropsConDerbyDriver driver2 = new PropsConDerbyDriver(expectedInstanceName2);

        PropsContainer container1 = PropsContainer.getInstance(
            driver1,
            new TransactionMgr(con1)
        );
        PropsContainer container2 = PropsContainer.getInstance(
            driver2,
            new TransactionMgr(con2)
        );

        Map<String, String> map1 = new HashMap<>();
        map1.put("a", "b");
        map1.put("a/c", "d");
        map1.put("e", "f");
        Map<String, String> map2 = new HashMap<>();
        map2.put("a", "b");
        map2.put("g", "h");
        map2.put("g/c", "i");
        map2.put("g/j", "k");

        container1.setAllProps(map1, null);
        con1.commit();
        container2.setAllProps(map2, null);
        con2.commit();

        checkIfPresent(map1, expectedInstanceName1);
        checkIfPresent(map2, expectedInstanceName2);

        container1.clear();
        con1.commit();
        map1.clear();

        checkIfPresent(map1, expectedInstanceName1);
        checkIfPresent(map2, expectedInstanceName2);
    }

    @SuppressWarnings("resource")
    @Test
    public void testLoadSimple() throws Throwable
    {
        String instanceName = DEFAULT_INSTANCE_NAME;
        String key = "a";
        String value = "b";
        insert(instanceName, key, value);

        Connection con = getConnection();
        Props props = PropsContainer.getInstance(dbDriver, new TransactionMgr(con));

        Set<Entry<String,String>> entrySet = props.entrySet();
        assertEquals("Unexpected entries in PropsContainer", 1 , entrySet.size());
        assertTrue("PropsContainer missing key [" + key + "]", props.keySet().contains(key));
        assertEquals("Unexpected value", value, props.getProp(key));
    }

    @Test
    public void testLoadNested() throws Throwable
    {
        String instanceName = DEFAULT_INSTANCE_NAME;
        String key1 = "a";
        String value1 = "b";
        String key2 = key1 + "/c";
        String value2 = "d";
        insert(instanceName, key1, value1);
        insert(instanceName, key2, value2);

        Props props = PropsContainer.getInstance(dbDriver, new TransactionMgr(getConnection()));
        assertEquals("Unexpected entries in PropsContainer", 2 , props.size());

        assertTrue("PropsContainer missing key [" + key1 + "]", props.keySet().contains(key1));
        assertTrue("PropsContainer missing key [" + key2 + "]", props.keySet().contains(key2));
        assertEquals("Unexpected value", value1, props.getProp(key1));
        assertEquals("Unexpected value", value2, props.getProp(key2));
    }

    @Test
    public void testLoadUpdate() throws Throwable
    {
        String instanceName = DEFAULT_INSTANCE_NAME;
        String key1 = "a";
        String key2 = key1 + "/" + "c";
        String value1 = "b";
        String value2 = "d";

        Map<String, String> map = new HashMap<>();

        insert(instanceName, key1, value1);
        map.put(key1, value1);

        Props props = PropsContainer.getInstance(dbDriver, new TransactionMgr(getConnection()));
        checkExpectedMap(map, props);

        insert(instanceName, key2, value2);
        map.put(key2, value2);

        props = PropsContainer.getInstance(dbDriver, new TransactionMgr(getConnection()));
        checkExpectedMap(map, props);

        delete(instanceName, key2);
        map.remove(key2);

        props = PropsContainer.getInstance(dbDriver, new TransactionMgr(getConnection()));
        checkExpectedMap(map, props);
    }

    @SuppressWarnings("resource")
    @Test
    public void testLoadMultiple() throws Throwable
    {
        String instanceName1 = "INSTANCE_1";
        String instanceName2 = "INSTANCE_2";

        Connection con = getConnection();
        PropsConDerbyDriver driver1 = new PropsConDerbyDriver(instanceName1);
        PropsConDerbyDriver driver2 = new PropsConDerbyDriver(instanceName2);

        Map<String, String> map1 = new HashMap<>();
        map1.put("a", "b");
        map1.put("a/c", "d");
        map1.put("e", "f");
        Map<String, String> map2 = new HashMap<>();
        map2.put("a", "b");
        map2.put("g", "h");
        map2.put("g/c", "i");
        map2.put("g/j", "k");

        insert(instanceName1, map1);
        insert(instanceName2, map2);

        Props props1 = PropsContainer.getInstance(driver1, new TransactionMgr(con));
        Props props2 = PropsContainer.getInstance(driver2, new TransactionMgr(con));

        checkExpectedMap(map1, props1);
        checkExpectedMap(map2, props2);
    }
}
