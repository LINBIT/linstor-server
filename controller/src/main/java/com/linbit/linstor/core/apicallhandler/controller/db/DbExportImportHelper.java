package com.linbit.linstor.core.apicallhandler.controller.db;

import com.linbit.ImplementationError;
import com.linbit.linstor.InitializationException;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.core.apicallhandler.controller.db.DbExportPojo.Table;
import com.linbit.linstor.core.apicallhandler.response.ApiDatabaseException;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.cfg.CtrlConfig;
import com.linbit.linstor.dbcp.DbConnectionPool;
import com.linbit.linstor.dbcp.DbConnectionPoolInitializer;
import com.linbit.linstor.dbcp.etcd.DbEtcd;
import com.linbit.linstor.dbcp.k8s.crd.DbK8sCrd;
import com.linbit.linstor.dbdrivers.AbsDatabaseDriver;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DatabaseTable;
import com.linbit.linstor.dbdrivers.DbEngine;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables;
import com.linbit.linstor.dbdrivers.k8s.crd.LinstorSpec;
import com.linbit.linstor.logging.ErrorReporter;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.io.File;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;

@Singleton
public class DbExportImportHelper
{
    private final ErrorReporter errorReporter;

    private final DbConnectionPool dbSql;
    private final DbEngine currentDbEngine;
    private final DbEtcd dbEtcd;
    private final DbK8sCrd dbK8s;
    private final CtrlConfig ctrlCfg;
    private final Map<DatabaseTable, AbsDatabaseDriver<?, ?, ?>> allDrivers;

    @Inject
    public DbExportImportHelper(
        ErrorReporter errorReporterRef,
        Map<DatabaseTable, AbsDatabaseDriver<?, ?, ?>> allDriversRef,
        DbEngine dbEngineRef,
        DbConnectionPool dbSqlRef,
        DbEtcd dbEtcdRef,
        DbK8sCrd dbK8sRef,
        CtrlConfig ctrlCfgRef
    )
    {

        errorReporter = errorReporterRef;
        allDrivers = allDriversRef;
        currentDbEngine = dbEngineRef;
        dbSql = dbSqlRef;
        dbEtcd = dbEtcdRef;
        dbK8s = dbK8sRef;
        ctrlCfg = ctrlCfgRef;
    }

    public void export(String fileNameRef)
    {
        Map<String, Table> tables = new LinkedHashMap<>();

        String dbConnectionUrl = ctrlCfg.getDbConnectionUrl();
        String exportedBy;
        switch (currentDbEngine.getType())
        {
            case ETCD:
                exportedBy = "etcd";
                break;
            case K8S_CRD:
                exportedBy = "k8s";
                break;
            case SQL:
                try
                {
                    exportedBy = "sql/" + DbConnectionPoolInitializer.getDbType(dbConnectionUrl);
                }
                catch (InitializationException exc)
                {
                    throw new ImplementationError(exc);
                }
                break;
            default:
                throw new ImplementationError("Unknown database type: " + currentDbEngine.getType());
        }
        DbExportPojo pojo = new DbExportPojo(
            LinStor.VERSION_INFO_PROVIDER.getVersion(),
            System.currentTimeMillis(),
            exportedBy,
            dbSql.getCurrentVersion(dbConnectionUrl),
            dbEtcd.getCurrentVersion(),
            dbK8s.getCurrentVersion(),
            tables
        );

        try
        {
            for (DatabaseTable table : GeneratedDatabaseTables.ALL_TABLES)
            {
                AbsDatabaseDriver<?, ?, ?> dbDriver = allDrivers.get(table);

                // some dbDrivers do not have a corresponding table (that means they do not need to be exported)
                // this is the case for example for storage-resources. three is no storage-resource table
                // but since layers are loaded based of resources, the abstract class architecture still requires
                // a storage resource db driver which still needs to link other objects (like storage volumes, which
                // do have database tables and drivers)

                // however, the same is also valid in the other direction. Some existing database tables might not have
                // database drivers. Please see the comment and code in ControllerDbModules#getAllDbDrivers for more
                // information
                if (dbDriver != null)
                {
                    List<LinstorSpec> pojoList = dbDriver.export();
                    tables.put(table.getName(), new DbExportPojo.Table(pojoList));
                }
            }
        }
        catch (DatabaseException dbExc)
        {
            throw new ApiDatabaseException(dbExc);
        }
        catch (Exception exc)
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(ApiConsts.MASK_ERROR, "Unknown exception occured"),
                exc
            );
        }

        ObjectMapper om = new ObjectMapper();
        try
        {
            om.writeValue(new File(fileNameRef), pojo);
            errorReporter.logTrace("written db export to: %s", fileNameRef);

        }
        catch (IOException exc)
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_INVLD_DB_EXPORT_FILE,
                    "Failed to write database backup. File: " + fileNameRef
                ),
                exc
            );
        }
    }
}
