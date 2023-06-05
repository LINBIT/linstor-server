package com.linbit.linstor.security;

import com.linbit.linstor.core.CoreModule.ObjProtMap;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.SecObjProtAclDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.SecObjProtDatabaseDriver;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

@Singleton
public class ObjectProtectionStltFactory extends ObjectProtectionFactory
{
    @Inject
    public ObjectProtectionStltFactory(
        ObjProtMap objProtMapRef,
        SecObjProtDatabaseDriver dbDriverRef,
        SecObjProtAclDatabaseDriver objProtAclDbDriverRef,
        Provider<TransactionMgr> transMgrProviderRef,
        TransactionObjectFactory transObjFactoryRef
    )
    {
        super(objProtMapRef, dbDriverRef, objProtAclDbDriverRef, transMgrProviderRef, transObjFactoryRef);
    }

    @Override
    public ObjectProtection getInstance(AccessContext accCtxRef, String objPathRef, boolean createIfNotExistsRef)
        throws DatabaseException
    {
        return super.getInstance(accCtxRef, objPathRef, true);
    }
}
