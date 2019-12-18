package com.linbit.linstor.dbcp.etcd;

import com.linbit.linstor.ControllerDatabase;
import com.linbit.linstor.InitializationException;
import com.linbit.linstor.core.ControllerCmdlArguments;
import com.linbit.linstor.core.LinstorConfigToml;
import com.linbit.linstor.dbcp.DbInitializer;
import com.linbit.linstor.logging.ErrorReporter;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class DbEtcdInitializer implements DbInitializer
{
    private final ErrorReporter errorLog;
    private final ControllerCmdlArguments args;
    private final DbEtcd dbEtcd;
    private final LinstorConfigToml linstorConfig;

    @Inject
    public DbEtcdInitializer(
        ErrorReporter errorLogRef,
        ControllerCmdlArguments argsRef,
        ControllerDatabase dbEtcdRef,
        LinstorConfigToml linstorConfigRef
    )
    {
        errorLog = errorLogRef;
        args = argsRef;
        dbEtcd = (DbEtcd) dbEtcdRef;
        linstorConfig = linstorConfigRef;
    }

    @Override
    public void initialize(boolean withStartupVer) throws InitializationException
    {
        errorLog.logInfo("Initializing the etcd database");

        dbEtcd.initializeDataSource(linstorConfig.getDB().getConnectionUrl());

        dbEtcd.migrate("etcd");
    }
}
