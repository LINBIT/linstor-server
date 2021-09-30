package com.linbit.linstor.dbcp.k8s.crd;

import com.linbit.SystemServiceStartException;
import com.linbit.linstor.ControllerDatabase;
import com.linbit.linstor.InitializationException;
import com.linbit.linstor.core.cfg.CtrlConfig;
import com.linbit.linstor.dbcp.DbInitializer;
import com.linbit.linstor.logging.ErrorReporter;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class DbK8sCrdInitializer implements DbInitializer
{
    private final ErrorReporter errorLog;
    private final DbK8sCrd dbK8sCrd;
    private final CtrlConfig ctrlCfg;

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
    public void initialize() throws InitializationException, SystemServiceStartException
    {
        errorLog.logInfo("Initializing the k8s crd database connector");

        try
        {
            dbK8sCrd.initializeDataSource(ctrlCfg.getDbConnectionUrl());

            dbK8sCrd.migrate("k8s");
        }
        catch (Exception exc)
        {
            throw new SystemServiceStartException("Database initialization error", exc, true);
        }
    }
}
