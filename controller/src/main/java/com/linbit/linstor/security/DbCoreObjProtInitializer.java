package com.linbit.linstor.security;

import com.linbit.linstor.InitializationException;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.LinStorScope;
import com.linbit.linstor.core.repository.ExternalFileProtectionRepository;
import com.linbit.linstor.core.repository.FreeSpaceMgrProtectionRepository;
import com.linbit.linstor.core.repository.KeyValueStoreProtectionRepository;
import com.linbit.linstor.core.repository.NodeProtectionRepository;
import com.linbit.linstor.core.repository.RemoteProtectionRepository;
import com.linbit.linstor.core.repository.ResourceDefinitionProtectionRepository;
import com.linbit.linstor.core.repository.ResourceGroupProtectionRepository;
import com.linbit.linstor.core.repository.ScheduleProtectionRepository;
import com.linbit.linstor.core.repository.StorPoolDefinitionProtectionRepository;
import com.linbit.linstor.core.repository.SystemConfProtectionRepository;
import com.linbit.linstor.systemstarter.StartupInitializer;
import com.linbit.linstor.transaction.TransactionException;
import com.linbit.linstor.transaction.manager.TransactionMgr;
import com.linbit.linstor.transaction.manager.TransactionMgrGenerator;
import com.linbit.linstor.transaction.manager.TransactionMgrUtil;

import javax.inject.Inject;

public class DbCoreObjProtInitializer implements StartupInitializer
{
    private final AccessContext initCtx;
    private final ObjectProtectionFactory objectProtectionFactory;
    private final LinStorScope initScope;
    private final NodeProtectionRepository nodesProtectionRepository;
    private final ResourceDefinitionProtectionRepository resourceDefinitionProtectionRepository;
    private final ResourceGroupProtectionRepository resourceGroupProtectionRepository;
    private final StorPoolDefinitionProtectionRepository storPoolDefinitionProtectionRepository;
    private final FreeSpaceMgrProtectionRepository freeSpaceMgrProtectionRepository;
    private final SystemConfProtectionRepository systemConfProtectionRepository;
    private final KeyValueStoreProtectionRepository keyValueStoreProtectionRepository;
    private final ExternalFileProtectionRepository externalFileProtectionRepository;
    private final RemoteProtectionRepository remoteProtectionRepository;
    private final ScheduleProtectionRepository scheduleProtectionRepository;
    private final ShutdownProtHolder shutdownProtHolder;
    private final TransactionMgrGenerator transactionMgrGenerator;

    @Inject
    public DbCoreObjProtInitializer(
        @SystemContext AccessContext initCtxRef,
        ObjectProtectionFactory objectProtectionFactoryRef,
        LinStorScope initScopeRef,
        NodeProtectionRepository nodesProtectionRepositoryRef,
        ResourceDefinitionProtectionRepository resourceDefinitionProtectionRepositoryRef,
        ResourceGroupProtectionRepository resourceGroupProtectionRepositoryRef,
        StorPoolDefinitionProtectionRepository storPoolDefinitionProtectionRepositoryRef,
        FreeSpaceMgrProtectionRepository freeSpaceMgrProtectionRepositoryRef,
        SystemConfProtectionRepository systemConfProtectionRepositoryRef,
        KeyValueStoreProtectionRepository keyValueStoreProtectionRepositoryRef,
        ExternalFileProtectionRepository externalFileProtectionRepositoryRef,
        RemoteProtectionRepository remoteProtectionRepositoryRef,
        ScheduleProtectionRepository scheduleProtectionRepositoryRef,
        ShutdownProtHolder shutdownProtHolderRef,
        TransactionMgrGenerator transactionMgrGeneratorRef
    )
    {
        initCtx = initCtxRef;
        objectProtectionFactory = objectProtectionFactoryRef;
        initScope = initScopeRef;
        nodesProtectionRepository = nodesProtectionRepositoryRef;
        resourceDefinitionProtectionRepository = resourceDefinitionProtectionRepositoryRef;
        resourceGroupProtectionRepository = resourceGroupProtectionRepositoryRef;
        storPoolDefinitionProtectionRepository = storPoolDefinitionProtectionRepositoryRef;
        freeSpaceMgrProtectionRepository = freeSpaceMgrProtectionRepositoryRef;
        systemConfProtectionRepository = systemConfProtectionRepositoryRef;
        keyValueStoreProtectionRepository = keyValueStoreProtectionRepositoryRef;
        externalFileProtectionRepository = externalFileProtectionRepositoryRef;
        remoteProtectionRepository = remoteProtectionRepositoryRef;
        scheduleProtectionRepository = scheduleProtectionRepositoryRef;
        shutdownProtHolder = shutdownProtHolderRef;
        transactionMgrGenerator = transactionMgrGeneratorRef;
    }

    @Override
    public void initialize()
        throws InitializationException
    {
        TransactionMgr transMgr = null;
        try (LinStorScope.ScopeAutoCloseable close = initScope.enter())
        {
            transMgr = transactionMgrGenerator.startTransaction();
            TransactionMgrUtil.seedTransactionMgr(initScope, transMgr);

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
            resourceGroupProtectionRepository.setObjectProtection(objectProtectionFactory.getInstance(
                initCtx,
                ObjectProtection.buildPathController("rscGrpMap"),
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
            externalFileProtectionRepository.setObjectProtection(
                objectProtectionFactory.getInstance(
                    initCtx,
                    ObjectProtection.buildPathController("externalFileMap"),
                    true
                )
            );
            remoteProtectionRepository.setObjectProtection(objectProtectionFactory.getInstance(
                initCtx,
                ObjectProtection.buildPathController("remoteMap"),
                true
            ));
            scheduleProtectionRepository.setObjectProtection(
                objectProtectionFactory.getInstance(
                    initCtx,
                    ObjectProtection.buildPathController("scheduleMap"),
                    true
                )
            );

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
        }
    }

}
