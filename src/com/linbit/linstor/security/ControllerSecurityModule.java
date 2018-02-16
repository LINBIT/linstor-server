package com.linbit.linstor.security;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.linbit.ImplementationError;
import com.linbit.TransactionMgr;
import com.linbit.linstor.ControllerDatabase;
import com.linbit.linstor.InitializationException;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.dbcp.DbConnectionPool;
import com.linbit.linstor.logging.ErrorReporter;

import javax.inject.Named;
import javax.inject.Singleton;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;

public class ControllerSecurityModule extends AbstractModule
{
    public static final String NODES_MAP_PROT = "nodesMapProt";
    public static final String RSC_DFN_MAP_PROT = "rscDfnMapProt";
    public static final String STOR_POOL_DFN_MAP_PROT = "storPoolDfnMapProt";
    public static final String CTRL_CONF_PROT = "ctrlConfProt";

    @Override
    protected void configure()
    {
        bind(DbAccessor.class).to(DbDerbyPersistence.class);
    }

    @Provides
    public SecurityLevelSetter securityLevelSetter(
        final DbConnectionPool dbConnectionPool,
        final DbAccessor securityDbDriver
    )
    {
        return new SecurityLevelSetter()
        {
            @Override
            public void setSecurityLevel(AccessContext accCtx, SecurityLevel newLevel)
                throws AccessDeniedException, SQLException
            {
                SecurityLevel.set(
                    accCtx, newLevel, dbConnectionPool, securityDbDriver
                );
            }
        };
    }

    @Provides
    @Singleton
    public Authentication initializeAuthentication(
        @SystemContext AccessContext initCtx,
        ErrorReporter errorLogRef,
        ControllerDatabase dbConnPool,
        DbAccessor securityDbDriver
    )
        throws InitializationException
    {
        errorLogRef.logInfo("Initializing authentication subsystem");

        try
        {
            return new Authentication(initCtx, dbConnPool, securityDbDriver, errorLogRef);
        }
        catch (AccessDeniedException accExc)
        {
            throw new ImplementationError(
                "The initialization security context does not have the necessary " +
                    "privileges to create the authentication subsystem",
                accExc
            );
        }
        catch (NoSuchAlgorithmException algoExc)
        {
            throw new InitializationException(
                "Initialization of the authentication subsystem failed because the " +
                    "required hashing algorithm is not supported on this platform",
                algoExc
            );
        }
    }

    @Provides
    @Singleton
    public Authorization initializeAuthorization(
        @SystemContext AccessContext initCtx,
        ErrorReporter errorLogRef,
        ControllerDatabase dbConnPool,
        DbAccessor securityDbDriver
    )
    {
        errorLogRef.logInfo("Initializing authorization subsystem");

        try
        {
            return new Authorization(initCtx, dbConnPool, securityDbDriver);
        }
        catch (AccessDeniedException accExc)
        {
            throw new ImplementationError(
                "The initialization security context does not have the necessary " +
                    "privileges to create the authorization subsystem",
                accExc
            );
        }
    }

    @Provides
    @Singleton
    @Named(NODES_MAP_PROT)
    public ObjectProtection nodesMapProt(ProtectionBundle protectionBundle)
    {
        return protectionBundle.nodesMapProt;
    }

    @Provides
    @Singleton
    @Named(RSC_DFN_MAP_PROT)
    public ObjectProtection rscDfnMapProt(ProtectionBundle protectionBundle)
    {
        return protectionBundle.rscDfnMapProt;
    }

    @Provides
    @Singleton
    @Named(STOR_POOL_DFN_MAP_PROT)
    public ObjectProtection storPoolDfnMapProt(ProtectionBundle protectionBundle)
    {
        return protectionBundle.storPoolDfnMapProt;
    }

    @Provides
    @Singleton
    @Named(CTRL_CONF_PROT)
    public ObjectProtection ctrlConfProt(ProtectionBundle protectionBundle)
    {
        return protectionBundle.ctrlConfProt;
    }

    @Provides
    @Singleton
    @Named(SecurityModule.SHUTDOWN_PROT)
    public ObjectProtection shutdownProt(ProtectionBundle protectionBundle)
    {
        return protectionBundle.shutdownProt;
    }

    @Provides
    @Singleton
    public ProtectionBundle initializeObjectProtection(
        @SystemContext AccessContext initCtx,
        DbConnectionPool dbConnPool
    )
        throws SQLException, InitializationException
    {
        ProtectionBundle bundle = new ProtectionBundle();

        TransactionMgr transMgr = null;
        try
        {
            transMgr = new TransactionMgr(dbConnPool);

            // initializing ObjectProtections for nodeMap, rscDfnMap and storPoolMap
            bundle.nodesMapProt = ObjectProtection.getInstance(
                initCtx,
                ObjectProtection.buildPathController("nodesMap"),
                true,
                transMgr
            );
            bundle.rscDfnMapProt = ObjectProtection.getInstance(
                initCtx,
                ObjectProtection.buildPathController("rscDfnMap"),
                true,
                transMgr
            );
            bundle.storPoolDfnMapProt = ObjectProtection.getInstance(
                initCtx,
                ObjectProtection.buildPathController("storPoolMap"),
                true,
                transMgr
            );

            // initializing controller OP
            bundle.ctrlConfProt = ObjectProtection.getInstance(
                initCtx,
                ObjectProtection.buildPathController("conf"),
                true,
                transMgr
            );

            bundle.shutdownProt = ObjectProtection.getInstance(
                initCtx,
                ObjectProtection.buildPathController("shutdown"),
                true,
                transMgr
            );

            bundle.shutdownProt.setConnection(transMgr);
            // Set CONTROL access for the SYSTEM role on shutdown
            bundle.shutdownProt.addAclEntry(initCtx, initCtx.getRole(), AccessType.CONTROL);

            transMgr.commit();
        }
        catch (Exception exc)
        {
            if (transMgr != null)
            {
                transMgr.rollback();
            }
            throw new InitializationException("Failed to load object protection definitions", exc);
        }
        finally
        {
            if (transMgr != null)
            {
                dbConnPool.returnConnection(transMgr);
            }
        }

        return bundle;
    }

    // Bundle together so that the objects can be initialized together but provided separately
    private static class ProtectionBundle
    {
        public ObjectProtection nodesMapProt;
        public ObjectProtection rscDfnMapProt;
        public ObjectProtection storPoolDfnMapProt;
        public ObjectProtection ctrlConfProt;
        public ObjectProtection shutdownProt;
    }
}
