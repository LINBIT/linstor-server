package com.linbit.linstor.transaction;

import com.linbit.linstor.dbdrivers.interfaces.updater.CollectionDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.updater.MapDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.stateflags.Flags;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.stateflags.StateFlagsBits;
import com.linbit.linstor.stateflags.StateFlagsPersistence;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

@Singleton
public class TransactionObjectFactory
{
    private final Provider<TransactionMgr> transMgrProvider;

    @Inject
    public TransactionObjectFactory(
        Provider<TransactionMgr> transMgrProviderRef
    )
    {
        transMgrProvider = transMgrProviderRef;
    }

    public <PARENT, ELEMENT> TransactionSimpleObject<PARENT, ELEMENT> createTransactionSimpleObject(
        PARENT parent,
        ELEMENT element,
        SingleColumnDatabaseDriver<PARENT, ELEMENT> driver
    )
    {
        return new TransactionSimpleObject<PARENT, ELEMENT>(
            parent,
            element,
            driver,
            transMgrProvider
        );
    }

    public <KEY, VALUE extends TransactionObject> TransactionMap<KEY, VALUE> createTransactionMap(
        Map<KEY, VALUE> mapRef,
        MapDatabaseDriver<KEY, VALUE> driver
    )
    {
        return new TransactionMap<>(mapRef, driver, transMgrProvider);
    }

    public <PARENT, VALUE extends TransactionObject> TransactionSet<PARENT, VALUE> createTransactionSet(
        PARENT parent,
        Set<VALUE> backingSet,
        CollectionDatabaseDriver<PARENT, VALUE> dbDriver
    )
    {
        return new TransactionSet<PARENT, VALUE>(parent, backingSet, dbDriver, transMgrProvider);
    }

    public <PARENT, VALUE extends TransactionObject> TransactionList<PARENT, VALUE> createTransactionList(
        PARENT parent,
        List<VALUE> backingList,
        CollectionDatabaseDriver<PARENT, VALUE> dbDriver
    )
    {
        return new TransactionList<PARENT, VALUE>(parent, backingList, dbDriver, transMgrProvider);
    }

    public <PARENT, VALUE> TransactionList<PARENT, VALUE> createTransactionPrimitiveList(
        PARENT parent,
        List<VALUE> backingList,
        CollectionDatabaseDriver<PARENT, VALUE> dbDriver
    )
    {
        return new TransactionList<PARENT, VALUE>(parent, backingList, dbDriver, transMgrProvider);
    }

    public <PARENT, FLAG extends Enum<FLAG> & Flags> StateFlags<FLAG> createStateFlagsImpl(
        ObjectProtection objProt,
        PARENT parentObj,
        Class<FLAG> enumType,
        StateFlagsPersistence<PARENT> stateFlagPersistence,
        long initFlags
    )
    {
        return createStateFlagsImpl(
            Collections.singletonList(objProt),
            parentObj,
            enumType,
            stateFlagPersistence,
            initFlags
        );
    }

    public <PARENT, FLAG extends Enum<FLAG> & Flags> StateFlags<FLAG> createStateFlagsImpl(
        List<ObjectProtection> objProts,
        PARENT parentObj,
        Class<FLAG> enumType,
        StateFlagsPersistence<PARENT> stateFlagPersistence,
        long initFlags
    )
    {
        return new StateFlagsBits<PARENT, FLAG>(
            objProts,
            parentObj,
            StateFlagsBits.getMask(
                enumType.getEnumConstants()
            ),
            stateFlagPersistence,
            initFlags,
            transMgrProvider
        );
    }
}
