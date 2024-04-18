package com.linbit.linstor.transaction;

import com.linbit.linstor.dbdrivers.noop.NoOpMapDatabaseDriver;

import java.util.TreeMap;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TransactionMapTest
{
    private DummyTxMgr dummyTxMgr;
    private NoOpMapDatabaseDriver<Object, String, DummyTxObj> dummyMapDbDriver = new NoOpMapDatabaseDriver<>();

    private TreeMap<String, DummyTxObj> backingMap;
    private TransactionMap<Object, String, DummyTxObj> txMap;

    @Before
    public void setUp()
    {
        dummyTxMgr = new DummyTxMgr();
        backingMap = new TreeMap<>();
        txMap = new TransactionMap<>(new Object(), backingMap, dummyMapDbDriver, () -> dummyTxMgr);
    }

    @Test
    public void simpleCommit()
    {
        String key = "key1";

        txMap.put(key, new DummyTxObj());
        assertTrue(backingMap.containsKey(key));
        assertTrue(txMap.containsKey(key));

        dummyTxMgr.commit();
        assertTrue(backingMap.containsKey(key));
        assertTrue(txMap.containsKey(key));

        dummyTxMgr.rollback();
        assertTrue(backingMap.containsKey(key));
        assertTrue(txMap.containsKey(key));
        assertEquals(1, backingMap.size());
        assertEquals(1, txMap.size());
    }

    @Test
    public void multiOverrideBeforeCommit()
    {
        String key = "key1";
        DummyTxObj value1 = new DummyTxObj();
        DummyTxObj value2 = new DummyTxObj();
        txMap.put(key, value1);
        assertEquals(value1, backingMap.get(key));
        assertEquals(value1, txMap.get(key));

        txMap.put(key, value2);
        assertEquals(value2, backingMap.get(key));
        assertEquals(value2, txMap.get(key));

        dummyTxMgr.commit();
        assertEquals(value2, backingMap.get(key));
        assertEquals(value2, txMap.get(key));

        dummyTxMgr.rollback();
        assertEquals(value2, backingMap.get(key));
        assertEquals(value2, txMap.get(key));
        assertEquals(1, backingMap.size());
        assertEquals(1, txMap.size());
    }

    @Test
    public void simpleCommitPrimitive()
    {
        TreeMap<String, String> primBackMap = new TreeMap<>();
        TransactionMap<Void, String, String> primTxMap = new TransactionMap<>(
            null,
            primBackMap,
            null,
            () -> dummyTxMgr
        );

        String key = "key1";
        String val = "val1";

        primTxMap.put(key, val);
        assertTrue(primBackMap.containsKey(key));
        assertTrue(primTxMap.containsKey(key));
        assertTrue(primBackMap.containsValue(val));
        assertTrue(primTxMap.containsValue(val));

        dummyTxMgr.commit();
        assertTrue(primBackMap.containsKey(key));
        assertTrue(primTxMap.containsKey(key));
        assertTrue(primBackMap.containsValue(val));
        assertTrue(primTxMap.containsValue(val));

        dummyTxMgr.rollback();
        assertTrue(primBackMap.containsKey(key));
        assertTrue(primTxMap.containsKey(key));
        assertTrue(primBackMap.containsValue(val));
        assertTrue(primTxMap.containsValue(val));
        assertEquals(1, primBackMap.size());
        assertEquals(1, primTxMap.size());
    }

    @Test
    public void simpleRollback()
    {
        String key = "key1";

        txMap.put(key, new DummyTxObj());
        assertTrue(backingMap.containsKey(key));
        assertTrue(txMap.containsKey(key));

        dummyTxMgr.rollback();
        assertFalse(backingMap.containsKey(key));
        assertFalse(txMap.containsKey(key));
        assertTrue(backingMap.isEmpty());
        assertTrue(txMap.isEmpty());
    }

    @Test
    public void multiOverrideBeforeRollback()
    {
        String key = "key1";
        DummyTxObj value1 = new DummyTxObj();
        DummyTxObj value2 = new DummyTxObj();
        txMap.put(key, value1);
        assertEquals(value1, backingMap.get(key));
        assertEquals(value1, txMap.get(key));

        txMap.put(key, value2);
        assertEquals(value2, backingMap.get(key));
        assertEquals(value2, txMap.get(key));

        dummyTxMgr.rollback();
        assertNull(backingMap.get(key));
        assertNull(txMap.get(key));
        assertTrue(backingMap.isEmpty());
        assertTrue(txMap.isEmpty());
    }
}
