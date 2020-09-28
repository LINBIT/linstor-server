package com.linbit.linstor.core.objects;

import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.core.ControllerCoreModule;
import com.linbit.linstor.core.identifier.SharedStorPoolName;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.security.ObjectProtectionFactory;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.inject.Inject;
import javax.inject.Provider;
import java.util.Map;

public class FreeSpaceMgrControllerFactory
{
    private final Map<SharedStorPoolName, FreeSpaceMgr> freeSpaceMgrMap;
    private final AccessContext privCtx;
    private final ObjectProtectionFactory objProtFactory;
    private final Provider<TransactionMgr> transMgrProvider;
    private final TransactionObjectFactory transObjFactory;

    @Inject
    public FreeSpaceMgrControllerFactory(
        ControllerCoreModule.FreeSpaceMgrMap freeSpaceMgrMapRef,
        @SystemContext AccessContext privCtxRef,
        ObjectProtectionFactory objProtFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef,
        TransactionObjectFactory trasnObjFactoryRef
    )
    {
        freeSpaceMgrMap = freeSpaceMgrMapRef;
        privCtx = privCtxRef;
        objProtFactory = objProtFactoryRef;
        transMgrProvider = transMgrProviderRef;
        transObjFactory = trasnObjFactoryRef;
    }

    public FreeSpaceMgr getInstance(AccessContext accCtx, SharedStorPoolName sharedStorPoolName)
        throws AccessDeniedException, DatabaseException
    {
        FreeSpaceMgr ret = freeSpaceMgrMap.get(sharedStorPoolName);
        if (ret == null)
        {
            ret = new FreeSpaceMgr(
                privCtx,
                objProtFactory.getInstance(
                    accCtx,
                    ObjectProtection.buildPath(sharedStorPoolName),
                    true
                ),
                sharedStorPoolName,
                transMgrProvider,
                transObjFactory
            );
            freeSpaceMgrMap.put(sharedStorPoolName, ret);
        }
        return ret;
    }
}
