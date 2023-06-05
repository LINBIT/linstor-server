package com.linbit.linstor.security;

import com.linbit.linstor.dbdrivers.interfaces.SecObjProtAclDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.SecObjProtDatabaseDriver;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.inject.Inject;
import javax.inject.Provider;

public class SecurityTestUtils
{
    private final SecObjProtDatabaseDriver objProtDriver;
    private final SecObjProtAclDatabaseDriver objProtAclDriver;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;

    @Inject
    public SecurityTestUtils(
        SecObjProtDatabaseDriver objProtDriverRef,
        SecObjProtAclDatabaseDriver objProtAclDriverRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef
    )
    {
        objProtDriver = objProtDriverRef;
        objProtAclDriver = objProtAclDriverRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;
    }

    public ObjectProtection createObjectProtection(
        AccessContext accCtx,
        String objPathRef
    )
    {
        return new ObjectProtection(
            accCtx,
            objPathRef,
            new AccessControlList(objPathRef, objProtAclDriver, transObjFactory, transMgrProvider),
            objProtDriver,
            transObjFactory,
            transMgrProvider
        );
    }

}
