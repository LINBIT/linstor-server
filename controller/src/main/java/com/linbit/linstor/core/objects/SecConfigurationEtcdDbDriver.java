package com.linbit.linstor.core.objects;

import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables;
import com.linbit.linstor.dbdrivers.interfaces.SecConfigurationDatabaseDriver;
import com.linbit.linstor.transaction.TransactionMgrETCD;

import javax.inject.Inject;
import javax.inject.Provider;

public class SecConfigurationEtcdDbDriver extends EtcdDbDriver implements SecConfigurationDatabaseDriver
{
    private static final GeneratedDatabaseTables.SecConfiguration TABLE = GeneratedDatabaseTables.SEC_CONFIGURATION;

    @Inject
    public SecConfigurationEtcdDbDriver(
        Provider<TransactionMgrETCD> transMgrProviderRef
    )
    {
        super(TABLE, transMgrProviderRef);
    }

    private String id(SecConfiguration secConfiguration)
    {
        return secConfiguration.getDisplayValue().toUpperCase();
    }

    @Override
    public void create(SecConfiguration secConfiguration) throws DatabaseException
    {
        transMgrProvider.get().getTransaction()
            .put(putReq(
                tblKey(id(secConfiguration), GeneratedDatabaseTables.SecConfiguration.ENTRY_KEY),
                secConfiguration.getDisplayValue().toUpperCase()))
            .put(putReq(
                tblKey(id(secConfiguration), GeneratedDatabaseTables.SecConfiguration.ENTRY_DSP_KEY),
                secConfiguration.getDisplayValue()
            ))
            .put(putReq(
                tblKey(id(secConfiguration), GeneratedDatabaseTables.SecConfiguration.ENTRY_VALUE),
                secConfiguration.getValue()
            ));
    }

    @Override
    public void delete(SecConfiguration secConfiguration) throws DatabaseException
    {

    }
}
