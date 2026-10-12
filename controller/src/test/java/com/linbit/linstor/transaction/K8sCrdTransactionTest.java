package com.linbit.linstor.transaction;

import com.linbit.linstor.dbdrivers.DatabaseTable;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdCurrent;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdCurrent.SpaceHistory;

import java.util.Collections;

import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class K8sCrdTransactionTest
{
    private static final DatabaseTable TABLE = GeneratedDatabaseTables.SPACE_HISTORY;
    private static final int CAPACITY_SIZE = 32;

    private K8sCrdTransaction tx;

    @Before
    public void setUp()
    {
        tx = new K8sCrdTransaction(Collections.emptyMap(), null, null, GenCrdCurrent.VERSION);
    }

    @Test
    public void newTransactionHasNoPendingChanges()
    {
        assertThat(tx.hasPendingChanges()).isFalse();
    }

    @Test
    public void createIsPending()
    {
        tx.create(TABLE, entry(1));
        assertThat(tx.hasPendingChanges()).isTrue();
    }

    @Test
    public void replaceIsPending()
    {
        tx.replace(TABLE, entry(1));
        assertThat(tx.hasPendingChanges()).isTrue();
    }

    @Test
    public void deleteIsPending()
    {
        tx.delete(TABLE, entry(1));
        assertThat(tx.hasPendingChanges()).isTrue();
    }

    @Test
    public void createThenDeleteOfSameResourceCancelsOut()
    {
        tx.create(TABLE, entry(1));
        tx.delete(TABLE, entry(1));
        assertThat(tx.hasPendingChanges()).isFalse();
    }

    @Test
    public void deleteThenCreateOfSameResourceIsPendingReplace()
    {
        tx.delete(TABLE, entry(1));
        tx.create(TABLE, entry(1));
        assertThat(tx.hasPendingChanges()).isTrue();
    }

    private static SpaceHistory entry(long entryDate)
    {
        return GenCrdCurrent.createSpaceHistory(entryDate, new byte[CAPACITY_SIZE]);
    }
}
