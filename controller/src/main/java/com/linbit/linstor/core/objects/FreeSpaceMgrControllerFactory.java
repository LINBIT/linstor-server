package com.linbit.linstor.core.objects;

import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.core.ControllerCoreModule;
import com.linbit.linstor.core.identifier.FreeSpaceMgrName;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.security.ObjectProtectionFactory;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;

import javax.inject.Inject;
import javax.inject.Provider;
import java.util.Map;

public class FreeSpaceMgrControllerFactory
{
    private final Map<FreeSpaceMgrName, FreeSpaceMgr> freeSpaceMgrMap;
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

    public FreeSpaceMgr getInstance(AccessContext accCtx, FreeSpaceMgrName fsmName)
        throws AccessDeniedException, DatabaseException
    {
        FreeSpaceMgr ret = freeSpaceMgrMap.get(fsmName);
        if (ret == null)
        {
            ret = new FreeSpaceMgr(
                privCtx,
                objProtFactory.getInstance(
                    accCtx,
                    ObjectProtection.buildPath(fsmName),
                    true
                ),
                fsmName,
                transMgrProvider,
                transObjFactory
            );
            freeSpaceMgrMap.put(fsmName, ret);
        }
        return ret;
    }
}
