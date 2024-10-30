package com.linbit.linstor.dbcp.k8s.crd;

import com.linbit.ImplementationError;
import com.linbit.SystemServiceStartException;
import com.linbit.linstor.ControllerDatabase;
import com.linbit.linstor.InitializationException;
import com.linbit.linstor.core.cfg.CtrlConfig;
import com.linbit.linstor.dbcp.DbInitializer;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.logging.ErrorReporter;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class DbK8sCrdInitializer implements DbInitializer
{
    private static final String K8S_DB_TYPE = "k8s";
    private final ErrorReporter errorLog;
    private final DbK8sCrd dbK8sCrd;
    private final CtrlConfig ctrlCfg;

    private boolean enableMigrationOnInit = true;

    @Inject
    public DbK8sCrdInitializer(
        ErrorReporter errorLogRef,
        ControllerDatabase dbK8sCrdRef,
        CtrlConfig ctrlCfgRef
    )
    {
        errorLog = errorLogRef;
        dbK8sCrd = (DbK8sCrd) dbK8sCrdRef;
        ctrlCfg = ctrlCfgRef;
    }

    @Override
    public void setEnableMigrationOnInit(boolean enableRef)
    {
        enableMigrationOnInit = enableRef;
    }

    @Override
    public void initialize() throws InitializationException, SystemServiceStartException
    {
        errorLog.logInfo("Initializing the k8s crd database connector");

        try
        {
            final String crdConnectionUrl = ctrlCfg.getDbConnectionUrl();
            errorLog.logInfo("Kubernetes-CRD connection URL is \"%s\"", crdConnectionUrl);
            dbK8sCrd.initializeDataSource(crdConnectionUrl);

            if (enableMigrationOnInit)
            {
                dbK8sCrd.migrate(K8S_DB_TYPE);
            }
        }
        catch (Exception | ImplementationError exc)
        {
            throw new SystemServiceStartException("Database initialization error", exc, true);
        }
    }

    @Override
    public boolean needsMigration() throws DatabaseException
    {
        final String crdConnectionUrl = ctrlCfg.getDbConnectionUrl();
        errorLog.logInfo("Kubernetes-CRD connection URL is \"%s\"", crdConnectionUrl);
        dbK8sCrd.initializeDataSource(crdConnectionUrl);
        return dbK8sCrd.needsMigration(K8S_DB_TYPE);
    }

    @Override
    public void migrateTo(Object versionRef) throws DatabaseException, InitializationException
    {
        final String crdConnectionUrl = ctrlCfg.getDbConnectionUrl();

        dbK8sCrd.initializeDataSource(crdConnectionUrl);
        dbK8sCrd.preImportMigrateToVersion(K8S_DB_TYPE, versionRef);
    }
}
