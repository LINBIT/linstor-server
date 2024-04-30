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

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

    public <ELEMENT> TransactionSimpleObject<Void, ELEMENT> createVolatileTransactionSimpleObject(
        ELEMENT element
    )
    {
        return createTransactionSimpleObject(null, element, null);
    }

    public <PARENT, ELEMENT> TransactionSimpleObject<PARENT, ELEMENT> createTransactionSimpleObject(
        PARENT parent,
        ELEMENT element,
        SingleColumnDatabaseDriver<PARENT, ELEMENT> driver
    )
    {
        return new TransactionSimpleObject<>(
            parent,
            element,
            driver,
            transMgrProvider
        );
    }

    public <PARENT, KEY, VALUE extends TransactionObject> TransactionMap<PARENT, KEY, VALUE> createVolatileTransactionMap(
        Map<KEY, VALUE> mapRef
    )
    {
        return createTransactionMap(null, mapRef, null);
    }

    public <PARENT, KEY, VALUE extends TransactionObject> TransactionMap<PARENT, KEY, VALUE> createTransactionMap(
        @Nullable PARENT parent,
        Map<KEY, VALUE> mapRef,
        @Nullable MapDatabaseDriver<PARENT, KEY, VALUE> driver
    )
    {
        return new TransactionMap<>(parent, mapRef, driver, transMgrProvider);
    }

    public <VALUE extends TransactionObject> TransactionSet<Void, VALUE> createVolatileTransactionSet(
        Set<VALUE> backingSet
    )
    {
        return createTransactionSet(null, backingSet, null);
    }

    public <PARENT, KEY, VALUE> TransactionMap<PARENT, KEY, VALUE> createTransactionPrimitiveMap(
        @Nullable PARENT parent,
        Map<KEY, VALUE> mapRef,
        @Nullable MapDatabaseDriver<PARENT, KEY, VALUE> driver
    )
    {
        return new TransactionMap<>(parent, mapRef, driver, transMgrProvider);
    }

    public <PARENT, VALUE extends TransactionObject> TransactionSet<PARENT, VALUE> createTransactionSet(
        PARENT parent,
        Set<VALUE> backingSet,
        CollectionDatabaseDriver<PARENT, VALUE> dbDriver
    )
    {
        return new TransactionSet<>(parent, backingSet, dbDriver, transMgrProvider);
    }

    public <VALUE extends TransactionObject> TransactionList<Void, VALUE> createVolatileTransactionList(
        List<VALUE> backingList
    )
    {
        return createTransactionList(null, backingList, null);
    }

    public <PARENT, VALUE extends TransactionObject> TransactionList<PARENT, VALUE> createTransactionList(
        PARENT parent,
        List<VALUE> backingList,
        CollectionDatabaseDriver<PARENT, VALUE> dbDriver
    )
    {
        return new TransactionList<>(parent, backingList, dbDriver, transMgrProvider);
    }

    public <VALUE> TransactionList<Void, VALUE> createVolatileTransactionPrimitiveList(
        List<VALUE> backingList
    )
    {
        return createTransactionPrimitiveList(null, backingList, null);
    }

    public <PARENT, VALUE> TransactionList<PARENT, VALUE> createTransactionPrimitiveList(
        PARENT parent,
        List<VALUE> backingList,
        CollectionDatabaseDriver<PARENT, VALUE> dbDriver
    )
    {
        return new TransactionList<>(parent, backingList, dbDriver, transMgrProvider);
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
        return new StateFlagsBits<>(
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
