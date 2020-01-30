package com.linbit.linstor.transaction;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.TreeSet;

import org.junit.Before;
import org.junit.Test;

public class TransactionSetTest
{
    private DummyTxMgr dummyTxMgr;

    private TreeSet<DummyTxObj> backingSet;

    private TransactionSet<Void, DummyTxObj> txSet;

    @Before
    public void setUp()
    {
        dummyTxMgr = new DummyTxMgr();
        backingSet = new TreeSet<>();
        txSet = new TransactionSet<>(null, backingSet, null, () -> dummyTxMgr);
    }

    @Test
    public void simpleCommit()
    {
        DummyTxObj val = new DummyTxObj();
        txSet.add(val);
        assertTrue(backingSet.contains(val));

        dummyTxMgr.commit();
        assertTrue(backingSet.contains(val));

        dummyTxMgr.rollback();
        assertTrue(backingSet.contains(val));
        assertEquals(1, backingSet.size());
    }

    @Test
    public void multiAddBeforeCommit()
    {
        DummyTxObj value1 = new DummyTxObj();
        DummyTxObj value2 = new DummyTxObj();
        txSet.add(value1);
        assertTrue(backingSet.contains(value1));

        txSet.add(value2);
        assertTrue(backingSet.contains(value1));
        assertTrue(backingSet.contains(value2));

        dummyTxMgr.commit();
        assertTrue(backingSet.contains(value1));
        assertTrue(backingSet.contains(value2));
        assertEquals(2, backingSet.size());

        dummyTxMgr.rollback();
        assertTrue(backingSet.contains(value1));
        assertTrue(backingSet.contains(value2));
        assertEquals(2, backingSet.size());
    }

    @Test
    public void simpleRollback()
    {
        DummyTxObj val = new DummyTxObj();
        txSet.add(val);
        assertTrue(backingSet.contains(val));

        dummyTxMgr.rollback();
        assertTrue(backingSet.isEmpty());
    }

    @Test
    public void multiAddBeforeRollback()
    {
        DummyTxObj value1 = new DummyTxObj();
        DummyTxObj value2 = new DummyTxObj();
        txSet.add(value1);
        assertTrue(backingSet.contains(value1));

        txSet.add(value2);
        assertTrue(backingSet.contains(value1));
        assertTrue(backingSet.contains(value2));

        dummyTxMgr.rollback();
        assertTrue(backingSet.isEmpty());
    }
}
