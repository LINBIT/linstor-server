package com.linbit.linstor.transaction;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import com.linbit.linstor.dbdrivers.DatabaseException;

import org.junit.Before;
import org.junit.Test;

public class TransactionSimpleObjectTest
{
    private DummyTxMgr dummyTxMgr;

    private TransactionSimpleObject<Void, DummyTxObj> txObj;

    @Before
    public void setUp()
    {
        dummyTxMgr = new DummyTxMgr();
        txObj = new TransactionSimpleObject(null, null, null, () -> dummyTxMgr);
    }

    @Test
    public void simpleCommit() throws DatabaseException
    {
        DummyTxObj val = new DummyTxObj();
        txObj.set(val);
        assertEquals(val, txObj.get());

        dummyTxMgr.commit();
        assertEquals(val, txObj.get());

        dummyTxMgr.rollback();
        assertEquals(val, txObj.get());
    }

    @Test
    public void simpleOverrideCommit() throws DatabaseException
    {
        DummyTxObj value1 = new DummyTxObj();
        DummyTxObj value2 = new DummyTxObj();
        txObj.set(value1);
        dummyTxMgr.commit();

        txObj.set(value2);
        assertEquals(value2, txObj.get());

        dummyTxMgr.commit();
        assertEquals(value2, txObj.get());

        dummyTxMgr.rollback();
        assertEquals(value2, txObj.get());
    }

    @Test
    public void multiSetBeforeCommit() throws DatabaseException
    {
        DummyTxObj value1 = new DummyTxObj();
        DummyTxObj value2 = new DummyTxObj();
        txObj.set(value1);
        assertEquals(value1, txObj.get());

        txObj.set(value2);
        assertEquals(value2, txObj.get());

        dummyTxMgr.commit();
        assertEquals(value2, txObj.get());

        dummyTxMgr.rollback();
        assertEquals(value2, txObj.get());
    }

    @Test
    public void simpleRollback() throws DatabaseException
    {
        DummyTxObj val = new DummyTxObj();
        txObj.set(val);
        assertEquals(val, txObj.get());

        dummyTxMgr.rollback();
        assertNull(txObj.get());
    }

    @Test
    public void simpleOverrideRollback() throws DatabaseException
    {
        DummyTxObj value1 = new DummyTxObj();
        DummyTxObj value2 = new DummyTxObj();
        txObj.set(value1);
        dummyTxMgr.commit();

        txObj.set(value2);
        assertEquals(value2, txObj.get());

        dummyTxMgr.rollback();
        assertEquals(value1, txObj.get());
    }

    @Test
    public void multiAddBeforeRollback() throws DatabaseException
    {
        DummyTxObj value1 = new DummyTxObj();
        DummyTxObj value2 = new DummyTxObj();
        txObj.set(value1);
        assertEquals(value1, txObj.get());

        txObj.set(value2);
        assertEquals(value2, txObj.get());

        dummyTxMgr.rollback();
        assertNull(txObj.get());
    }
}
