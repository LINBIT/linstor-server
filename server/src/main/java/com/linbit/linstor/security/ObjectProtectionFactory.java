package com.linbit.linstor.security;

import javax.inject.Inject;
import javax.inject.Provider;

import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import java.sql.SQLException;

public class ObjectProtectionFactory
{
    private final ObjectProtectionDatabaseDriver dbDriver;
    private final Provider<TransactionMgr> transMgrProvider;
    private final TransactionObjectFactory transObjFactory;

    @Inject
    public ObjectProtectionFactory(
        ObjectProtectionDatabaseDriver dbDriverRef,
        Provider<TransactionMgr> transMgrProviderRef,
        TransactionObjectFactory transObjFactoryRef
    )
    {
        dbDriver = dbDriverRef;
        transMgrProvider = transMgrProviderRef;
        transObjFactory = transObjFactoryRef;
    }

    /**
     * Loads an ObjectProtection instance from the database.
     *
     * The {@code accCtx} parameter is only used when no ObjectProtection was found in the
     * database and the {@code createIfNotExists} parameter is set to true
     *
     * @param accCtx
     * @param transMgr
     * @param objPath
     * @param createIfNotExists
     * @return
     * @throws SQLException
     * @throws AccessDeniedException
     */
    public ObjectProtection getInstance(
        AccessContext accCtx,
        String objPath,
        boolean createIfNotExists
    )
        throws DatabaseException, AccessDeniedException
    {
        return ObjectProtection.getInstance(
            accCtx,
            objPath,
            createIfNotExists,
            transMgrProvider,
            transObjFactory,
            dbDriver
        );
    }
}
