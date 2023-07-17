package com.linbit.linstor.dbcp.etcd;

import com.linbit.SystemServiceStartException;
import com.linbit.linstor.ControllerDatabase;
import com.linbit.linstor.InitializationException;
import com.linbit.linstor.core.cfg.CtrlConfig;
import com.linbit.linstor.dbcp.DbInitializer;
import com.linbit.linstor.logging.ErrorReporter;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class DbEtcdInitializer implements DbInitializer
{
    private final ErrorReporter errorLog;
    private final DbEtcd dbEtcd;
    private final CtrlConfig ctrlCfg;

    private boolean enableMigrationOnInit = true;

    @Inject
    public DbEtcdInitializer(
        ErrorReporter errorLogRef,
        ControllerDatabase dbEtcdRef,
        CtrlConfig ctrlCfgRef
    )
    {
        errorLog = errorLogRef;
        dbEtcd = (DbEtcd) dbEtcdRef;
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
        errorLog.logInfo("Initializing the etcd database");

        try
        {
            final String etcdConnectionUrl = ctrlCfg.getDbConnectionUrl();
            errorLog.logInfo("etcd connection URL is \"%s\"", etcdConnectionUrl);
            dbEtcd.initializeDataSource(etcdConnectionUrl);

            if (enableMigrationOnInit)
            {
                dbEtcd.migrate("etcd");
            }
        }
        catch (Exception exc)
        {
            throw new SystemServiceStartException("Database initialization error", exc, true);
        }
    }

    @Override
    public boolean needsMigration() throws InitializationException
    {
        final String etcdConnectionUrl = ctrlCfg.getDbConnectionUrl();
        errorLog.logInfo("etcd connection URL is \"%s\"", etcdConnectionUrl);
        dbEtcd.initializeDataSource(etcdConnectionUrl);
        return dbEtcd.needsMigration("etcd");
    }
}
