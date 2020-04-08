package com.linbit.linstor.dbcp.etcd;

import com.linbit.linstor.ControllerDatabase;
import com.linbit.linstor.InitializationException;
import com.linbit.linstor.InternalApiConsts;
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
    public void initialize() throws InitializationException
    {
        errorLog.logInfo("Initializing the etcd database");

        try
        {
            dbEtcd.initializeDataSource(ctrlCfg.getDbConnectionUrl());

            dbEtcd.migrate("etcd");
        }
        catch (Exception exc)
        {
            errorLog.logError("Database initialization error: " + exc.getMessage());
            errorLog.reportError(exc);
            System.exit(InternalApiConsts.EXIT_CODE_IMPL_ERROR);
        }
    }
}
