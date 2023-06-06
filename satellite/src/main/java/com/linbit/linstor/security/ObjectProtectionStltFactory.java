package com.linbit.linstor.security;

import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.LinStorScope;
import com.linbit.linstor.core.CoreModule.ObjProtMap;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.SecObjProtAclDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.SecObjProtDatabaseDriver;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.SatelliteTransactionMgr;
import com.linbit.linstor.transaction.manager.TransactionMgr;
import com.linbit.linstor.transaction.manager.TransactionMgrUtil;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

@Singleton
public class ObjectProtectionStltFactory extends ObjectProtectionFactory
{
    private static final String DUMMY_OBJ_PROT_PATH = "";
    private ObjectProtection objProt;

    @Inject
    public ObjectProtectionStltFactory(
        @SystemContext AccessContext accCtx,
        ObjProtMap objProtMapRef,
        LinStorScope initScope,
        SecObjProtAclDatabaseDriver secObjProtAclDriver,
        SecObjProtDatabaseDriver secObjProtDriver,
        Provider<TransactionMgr> transMgrProviderRef,
        TransactionObjectFactory transObjFactoryRef
    )
        throws DatabaseException
    {
        super(objProtMapRef, secObjProtDriver, secObjProtAclDriver, transMgrProviderRef, transObjFactoryRef);

        // Create a dummy singleton object protection instance which is used everywhere in the satellite
        AccessControlList acl = new AccessControlList(
            DUMMY_OBJ_PROT_PATH,
            secObjProtAclDriver,
            transObjFactoryRef,
            transMgrProviderRef
        );
        objProt = new ObjectProtection(
            accCtx,
            DUMMY_OBJ_PROT_PATH,
            acl,
            secObjProtDriver,
            transObjFactoryRef,
            transMgrProviderRef
        );
        SatelliteTransactionMgr transMgr = new SatelliteTransactionMgr();
        try (LinStorScope.ScopeAutoCloseable close = initScope.enter())
        {
            TransactionMgrUtil.seedTransactionMgr(initScope, transMgr);
            acl.addEntry(accCtx.getRole(), AccessType.CONTROL);
            transMgr.commit();
        }
    }

    @Override
    public ObjectProtection getInstance(AccessContext accCtxRef, String objPathRef, boolean createIfNotExistsRef)
        throws DatabaseException
    {
        return objProt;
    }
}
