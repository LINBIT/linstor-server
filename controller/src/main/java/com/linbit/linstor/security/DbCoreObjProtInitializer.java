package com.linbit.linstor.security;

import com.linbit.linstor.InitializationException;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.LinStorScope;
import com.linbit.linstor.core.objects.FreeSpaceMgrProtectionRepository;
import com.linbit.linstor.core.objects.KeyValueStoreProtectionRepository;
import com.linbit.linstor.core.objects.NodeProtectionRepository;
import com.linbit.linstor.core.objects.ResourceDefinitionProtectionRepository;
import com.linbit.linstor.core.objects.StorPoolDefinitionProtectionRepository;
import com.linbit.linstor.core.objects.SystemConfProtectionRepository;
import com.linbit.linstor.transaction.TransactionException;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionMgrGenerator;

import javax.inject.Inject;

public class DbCoreObjProtInitializer
{
    private final AccessContext initCtx;
    private final ObjectProtectionFactory objectProtectionFactory;
    private final LinStorScope initScope;
    private final NodeProtectionRepository nodesProtectionRepository;
    private final ResourceDefinitionProtectionRepository resourceDefinitionProtectionRepository;
    private final StorPoolDefinitionProtectionRepository storPoolDefinitionProtectionRepository;
    private final FreeSpaceMgrProtectionRepository freeSpaceMgrProtectionRepository;
    private final SystemConfProtectionRepository systemConfProtectionRepository;
    private final KeyValueStoreProtectionRepository keyValueStoreProtectionRepository;
    private final ShutdownProtHolder shutdownProtHolder;
    private final TransactionMgrGenerator transactionMgrGenerator;

    @Inject
    public DbCoreObjProtInitializer(
        @SystemContext AccessContext initCtxRef,
        ObjectProtectionFactory objectProtectionFactoryRef,
        LinStorScope initScopeRef,
        NodeProtectionRepository nodesProtectionRepositoryRef,
        ResourceDefinitionProtectionRepository resourceDefinitionProtectionRepositoryRef,
        StorPoolDefinitionProtectionRepository storPoolDefinitionProtectionRepositoryRef,
        FreeSpaceMgrProtectionRepository freeSpaceMgrProtectionRepositoryRef,
        SystemConfProtectionRepository systemConfProtectionRepositoryRef,
        KeyValueStoreProtectionRepository keyValueStoreProtectionRepositoryRef,
        ShutdownProtHolder shutdownProtHolderRef,
        TransactionMgrGenerator transactionMgrGeneratorRef
    )
    {
        initCtx = initCtxRef;
        objectProtectionFactory = objectProtectionFactoryRef;
        initScope = initScopeRef;
        nodesProtectionRepository = nodesProtectionRepositoryRef;
        resourceDefinitionProtectionRepository = resourceDefinitionProtectionRepositoryRef;
        storPoolDefinitionProtectionRepository = storPoolDefinitionProtectionRepositoryRef;
        freeSpaceMgrProtectionRepository = freeSpaceMgrProtectionRepositoryRef;
        systemConfProtectionRepository = systemConfProtectionRepositoryRef;
        keyValueStoreProtectionRepository = keyValueStoreProtectionRepositoryRef;
        shutdownProtHolder = shutdownProtHolderRef;
        transactionMgrGenerator = transactionMgrGeneratorRef;
    }

    public void initialize()
        throws InitializationException
    {
        TransactionMgr transMgr = null;
        try
        {
            transMgr = transactionMgrGenerator.startTransaction();
            initScope.enter();
            initScope.seed(TransactionMgr.class, transMgr);

            // initializing ObjectProtections for nodeMap, rscDfnMap, storPoolMap and freeSpaceMgrMap
            nodesProtectionRepository.setObjectProtection(objectProtectionFactory.getInstance(
                initCtx,
                ObjectProtection.buildPathController("nodesMap"),
                true
            ));
            resourceDefinitionProtectionRepository.setObjectProtection(objectProtectionFactory.getInstance(
                initCtx,
                ObjectProtection.buildPathController("rscDfnMap"),
                true
            ));
            storPoolDefinitionProtectionRepository.setObjectProtection(objectProtectionFactory.getInstance(
                initCtx,
                ObjectProtection.buildPathController("storPoolMap"),
                true
            ));
            freeSpaceMgrProtectionRepository.setObjectProtection(objectProtectionFactory.getInstance(
                initCtx,
                ObjectProtection.buildPathController("freeSpaceMgrMap"),
                true
            ));
            keyValueStoreProtectionRepository.setObjectProtection(objectProtectionFactory.getInstance(
                initCtx,
                ObjectProtection.buildPathController("keyValueStoreMap"),
                true
            ));

            // initializing controller OP
            systemConfProtectionRepository.setObjectProtection(objectProtectionFactory.getInstance(
                initCtx,
                ObjectProtection.buildPathController("conf"),
                true
            ));

            ObjectProtection shutdownProt = objectProtectionFactory.getInstance(
                initCtx,
                ObjectProtection.buildPathController("shutdown"),
                true
            );

            // Set CONTROL access for the SYSTEM role on shutdown
            shutdownProt.addAclEntry(initCtx, initCtx.getRole(), AccessType.CONTROL);

            shutdownProtHolder.setShutdownProt(shutdownProt);

            transMgr.commit();
        }
        catch (Exception exc)
        {
            if (transMgr != null)
            {
                try
                {
                    transMgr.rollback();
                }
                catch (TransactionException sqlExc)
                {
                    throw new InitializationException(
                        "Rollback after object protection loading failed",
                        sqlExc
                    );
                }
            }
            throw new InitializationException("Failed to load object protection definitions", exc);
        }
        finally
        {
            if (transMgr != null)
            {
                transMgr.returnConnection();
            }
            initScope.exit();
        }
    }

}
