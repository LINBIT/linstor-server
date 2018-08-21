package com.linbit.linstor.security;

import com.linbit.linstor.FreeSpaceMgrProtectionRepository;
import com.linbit.linstor.InitializationException;
import com.linbit.linstor.NodeProtectionRepository;
import com.linbit.linstor.ResourceDefinitionProtectionRepository;
import com.linbit.linstor.StorPoolDefinitionProtectionRepository;
import com.linbit.linstor.SystemConfProtectionRepository;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.LinStorScope;
import com.linbit.linstor.dbcp.DbConnectionPool;
import com.linbit.linstor.transaction.ControllerTransactionMgr;
import com.linbit.linstor.transaction.TransactionMgr;

import javax.inject.Inject;
import java.sql.SQLException;

public class DbCoreObjProtInitializer
{
    private final AccessContext initCtx;
    private final DbConnectionPool dbConnPool;
    private final ObjectProtectionFactory objectProtectionFactory;
    private final LinStorScope initScope;
    private final NodeProtectionRepository nodesProtectionRepository;
    private final ResourceDefinitionProtectionRepository resourceDefinitionProtectionRepository;
    private final StorPoolDefinitionProtectionRepository storPoolDefinitionProtectionRepository;
    private final FreeSpaceMgrProtectionRepository freeSpaceMgrProtectionRepository;
    private final SystemConfProtectionRepository systemConfProtectionRepository;
    private final ShutdownProtHolder shutdownProtHolder;

    @Inject
    public DbCoreObjProtInitializer(
        @SystemContext AccessContext initCtxRef,
        DbConnectionPool dbConnPoolRef,
        ObjectProtectionFactory objectProtectionFactoryRef,
        LinStorScope initScopeRef,
        NodeProtectionRepository nodesProtectionRepositoryRef,
        ResourceDefinitionProtectionRepository resourceDefinitionProtectionRepositoryRef,
        StorPoolDefinitionProtectionRepository storPoolDefinitionProtectionRepositoryRef,
        FreeSpaceMgrProtectionRepository freeSpaceMgrProtectionRepositoryRef,
        SystemConfProtectionRepository systemConfProtectionRepositoryRef,
        ShutdownProtHolder shutdownProtHolderRef
    )
    {
        initCtx = initCtxRef;
        dbConnPool = dbConnPoolRef;
        objectProtectionFactory = objectProtectionFactoryRef;
        initScope = initScopeRef;
        nodesProtectionRepository = nodesProtectionRepositoryRef;
        resourceDefinitionProtectionRepository = resourceDefinitionProtectionRepositoryRef;
        storPoolDefinitionProtectionRepository = storPoolDefinitionProtectionRepositoryRef;
        freeSpaceMgrProtectionRepository = freeSpaceMgrProtectionRepositoryRef;
        systemConfProtectionRepository = systemConfProtectionRepositoryRef;
        shutdownProtHolder = shutdownProtHolderRef;
    }

    public void initialize()
        throws InitializationException
    {
        TransactionMgr transMgr = null;
        try
        {
            transMgr = new ControllerTransactionMgr(dbConnPool);
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
                catch (SQLException sqlExc)
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
