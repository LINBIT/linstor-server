package com.linbit.linstor.dbdrivers;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.linbit.linstor.Node;
import com.linbit.linstor.NodeName;
import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.StorPoolDefinition;
import com.linbit.linstor.StorPoolName;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.DbAccessor;
import com.linbit.linstor.security.DbDerbyPersistence;

import java.util.Map;

public class DbDriversModule extends AbstractModule
{
    private final AccessContext initCtx;

    public DbDriversModule(AccessContext initCtx)
    {
        this.initCtx = initCtx;
    }

    @Override
    protected void configure()
    {

    }

    @Provides
    @Singleton
    public DatabaseDriver persistenceDbDriver(
        ErrorReporter errorLogRef,
        Map<NodeName, Node> nodesMap,
        Map<ResourceName, ResourceDefinition> rscDfnMap,
        Map<StorPoolName, StorPoolDefinition> storPoolDfnMap
    )
    {
        return new DerbyDriver(
            initCtx,
            errorLogRef,
            nodesMap,
            rscDfnMap,
            storPoolDfnMap
        );
    }
}
