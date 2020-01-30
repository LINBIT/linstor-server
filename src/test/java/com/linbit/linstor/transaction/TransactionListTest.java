package com.linbit.linstor.transaction;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;

import org.junit.Before;
import org.junit.Test;

public class TransactionListTest
{
    private DummyTxMgr dummyTxMgr;

    private ArrayList<DummyTxObj> backingList;

    private TransactionList<Void, DummyTxObj> txSet;

    @Before
    public void setUp()
    {
        dummyTxMgr = new DummyTxMgr();
        backingList = new ArrayList<>();
        txSet = new TransactionList<>(null, backingList, null, () -> dummyTxMgr);
    }

    @Test
    public void simpleCommit()
    {
        DummyTxObj val = new DummyTxObj();
        txSet.add(val);
        assertTrue(backingList.contains(val));

        dummyTxMgr.commit();
        assertTrue(backingList.contains(val));

        dummyTxMgr.rollback();
        assertTrue(backingList.contains(val));
        assertEquals(1, backingList.size());
    }

    @Test
    public void multiAddBeforeCommit()
    {
        DummyTxObj value1 = new DummyTxObj();
        DummyTxObj value2 = new DummyTxObj();
        txSet.add(value1);
        assertTrue(backingList.contains(value1));

        txSet.add(value2);
        assertTrue(backingList.contains(value1));
        assertTrue(backingList.contains(value2));

        dummyTxMgr.commit();
        assertTrue(backingList.contains(value1));
        assertTrue(backingList.contains(value2));
        assertEquals(2, backingList.size());

        dummyTxMgr.rollback();
        assertTrue(backingList.contains(value1));
        assertTrue(backingList.contains(value2));
        assertEquals(2, backingList.size());
    }

    @Test
    public void simpleRollback()
    {
        DummyTxObj val = new DummyTxObj();
        txSet.add(val);
        assertTrue(backingList.contains(val));

        dummyTxMgr.rollback();
        assertTrue(backingList.isEmpty());
    }

    @Test
    public void multiAddBeforeRollback()
    {
        DummyTxObj value1 = new DummyTxObj();
        DummyTxObj value2 = new DummyTxObj();
        txSet.add(value1);
        assertTrue(backingList.contains(value1));

        txSet.add(value2);
        assertTrue(backingList.contains(value1));
        assertTrue(backingList.contains(value2));

        dummyTxMgr.rollback();
        assertTrue(backingList.isEmpty());
    }
}
