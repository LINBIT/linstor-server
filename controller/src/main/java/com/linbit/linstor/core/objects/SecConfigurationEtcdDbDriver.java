package com.linbit.linstor.core.objects;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.dbdrivers.AbsDatabaseDriver;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DbEngine;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables;
import com.linbit.linstor.dbdrivers.interfaces.SecConfigurationDatabaseDriver;
import com.linbit.linstor.transaction.TransactionMgrETCD;
import com.linbit.utils.Pair;

import javax.inject.Inject;
import javax.inject.Provider;

public class SecConfigurationEtcdDbDriver extends AbsDatabaseDriver<SecConfiguration, Void, Void>
    implements SecConfigurationDatabaseDriver
{
    private final Provider<TransactionMgrETCD> transMgrProvider;

    @Inject
    public SecConfigurationEtcdDbDriver(
        DbEngine dbEngine,
        Provider<TransactionMgrETCD> transMgrProviderRef
    )
    {
        super(GeneratedDatabaseTables.SEC_CONFIGURATION, dbEngine, null);
        transMgrProvider = transMgrProviderRef;
    }

    private String id(SecConfiguration secConfiguration)
    {
        return secConfiguration.getDisplayValue().toUpperCase();
    }

    @Override
    protected Pair<SecConfiguration, Void> load(RawParameters raw)
        throws DatabaseException, InvalidNameException
    {
        // TODO Auto-generated method stub
        throw new ImplementationError("Not implemented yet");
    }

    @Override
    protected String getId(SecConfiguration dataRef)
    {
        // TODO Auto-generated method stub
        throw new ImplementationError("Not implemented yet");
    }
}
