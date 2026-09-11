package com.linbit.linstor.core.objects;

import com.linbit.linstor.core.ControllerCoreModule;
import com.linbit.linstor.core.identifier.SharedStorPoolName;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import jakarta.inject.Inject;
import jakarta.inject.Provider;

import java.util.Map;

public class FreeSpaceMgrControllerFactory
{
    private final Map<SharedStorPoolName, FreeSpaceMgr> freeSpaceMgrMap;
    private final Provider<TransactionMgr> transMgrProvider;
    private final TransactionObjectFactory transObjFactory;

    @Inject
    public FreeSpaceMgrControllerFactory(
        ControllerCoreModule.FreeSpaceMgrMap freeSpaceMgrMapRef,
        Provider<TransactionMgr> transMgrProviderRef,
        TransactionObjectFactory trasnObjFactoryRef
    )
    {
        freeSpaceMgrMap = freeSpaceMgrMapRef;
        transMgrProvider = transMgrProviderRef;
        transObjFactory = trasnObjFactoryRef;
    }

    public FreeSpaceMgr getInstance(SharedStorPoolName sharedStorPoolName)
        throws DatabaseException
    {
        FreeSpaceMgr ret = freeSpaceMgrMap.get(sharedStorPoolName);
        if (ret == null)
        {
            ret = new FreeSpaceMgr(
                sharedStorPoolName,
                transMgrProvider,
                transObjFactory
            );
            freeSpaceMgrMap.put(sharedStorPoolName, ret);
        }
        return ret;
    }
}
