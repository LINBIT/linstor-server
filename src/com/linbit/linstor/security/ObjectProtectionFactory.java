package com.linbit.linstor.security;

import com.linbit.TransactionMgr;

import javax.inject.Inject;
import java.sql.SQLException;

public class ObjectProtectionFactory
{
    private final ObjectProtectionDatabaseDriver dbDriver;

    @Inject
    public ObjectProtectionFactory(ObjectProtectionDatabaseDriver dbDriverRef)
    {
        dbDriver = dbDriverRef;
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
        boolean createIfNotExists,
        TransactionMgr transMgr
    )
        throws SQLException, AccessDeniedException
    {
        return ObjectProtection.getInstance(accCtx, objPath, createIfNotExists, transMgr, dbDriver);
    }
}
