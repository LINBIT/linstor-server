package com.linbit.linstor.security;

import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.inject.Inject;
import javax.inject.Provider;

public class SecurityTestUtils
{
    private ObjectProtectionDatabaseDriver driver;
    private TransactionObjectFactory transObjFactory;
    private Provider<TransactionMgr> transMgrProvider;

    @Inject
    public SecurityTestUtils(
        ObjectProtectionDatabaseDriver driverRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef
    )
    {
        driver = driverRef;
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
            driver,
            transObjFactory,
            transMgrProvider
        );
    }

}
