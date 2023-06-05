package com.linbit.linstor.security;

import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.CoreModule.ObjProtMap;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.SecObjProtAclDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.SecObjProtDatabaseDriver;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.inject.Inject;
import javax.inject.Provider;

import java.sql.SQLException;

public class ObjectProtectionFactory
{
    private final ObjProtMap objProtMap;
    private final SecObjProtDatabaseDriver objProtDbDriver;
    private final SecObjProtAclDatabaseDriver objProtAclDbDriver;
    private final Provider<TransactionMgr> transMgrProvider;
    private final TransactionObjectFactory transObjFactory;

    @Inject
    public ObjectProtectionFactory(
        CoreModule.ObjProtMap objProtMapRef,
        SecObjProtDatabaseDriver dbDriverRef,
        SecObjProtAclDatabaseDriver objProtAclDbDriverRef,
        Provider<TransactionMgr> transMgrProviderRef,
        TransactionObjectFactory transObjFactoryRef
    )
    {
        objProtMap = objProtMapRef;
        objProtDbDriver = dbDriverRef;
        objProtAclDbDriver = objProtAclDbDriverRef;
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
     */
    public ObjectProtection getInstance(
        AccessContext accCtx,
        String objPath,
        boolean createIfNotExists
    )
        throws DatabaseException
    {
        ObjectProtection ret = objProtMap.get(objPath);
        if (ret == null)
        {
            if (createIfNotExists)
            {
                AccessControlList acl = new AccessControlList(
                    objPath,
                    objProtAclDbDriver,
                    transObjFactory,
                    transMgrProvider
                );
                ret = new ObjectProtection(
                    accCtx,
                    objPath,
                    acl,
                    objProtDbDriver,
                    transObjFactory,
                    transMgrProvider
                );

                objProtDbDriver.create(ret);
                // as we just created a new ObjProt, we have to set the permissions
                // use the acl directly to skip the access checks as there are no rules yet and would cause
                // an exception
                acl.addEntry(accCtx.subjectRole, AccessType.CONTROL);
                objProtMap.put(objPath, ret);
            }
            else
            {
                throw new DatabaseException("ObjProt (" + objPath + ") not found!");
            }
        }

        return ret;
    }
}
