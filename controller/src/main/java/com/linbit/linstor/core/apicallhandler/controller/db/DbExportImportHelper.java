package com.linbit.linstor.core.apicallhandler.controller.db;

import com.linbit.ImplementationError;
import com.linbit.linstor.InitializationException;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.LinStorScope;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.core.apicallhandler.response.ApiDatabaseException;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.cfg.CtrlConfig;
import com.linbit.linstor.dbcp.DbConnectionPool;
import com.linbit.linstor.dbcp.DbConnectionPoolInitializer;
import com.linbit.linstor.dbcp.k8s.crd.DbK8sCrd;
import com.linbit.linstor.dbdrivers.AbsDatabaseDriver;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DatabaseTable;
import com.linbit.linstor.dbdrivers.DbEngine;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdCurrent;
import com.linbit.linstor.dbdrivers.k8s.crd.LinstorSpec;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.storage.kinds.ExtToolsInfo.Version;
import com.linbit.linstor.transaction.manager.TransactionMgr;
import com.linbit.linstor.transaction.manager.TransactionMgrGenerator;
import com.linbit.linstor.transaction.manager.TransactionMgrUtil;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;

@Singleton
public class DbExportImportHelper
{
    private static final String LIN_VER_PGRP_MAJOR = "major";
    private static final String LIN_VER_PGRP_MINOR = "minor";
    private static final String LIN_VER_PGRP_PATCH = "patch";
    private static final String LIN_VER_PGRP_GIT_COMMIT_COUNT = "gitCommitCnt";
    /*
     * this pattern should match the output of "git describe --tags". Examples:
     * "1.23.1"
     * "1.42.2-30-g1464609" // although the last "-g1464609" part will be ignored.
     */
    private static final Pattern LINSTOR_VERSION_PATTERN = Pattern.compile(
        "(?<" + LIN_VER_PGRP_MAJOR + ">[0-9]+)" +
            "\\.(?<" + LIN_VER_PGRP_MINOR + ">[0-9]+)" +
            "\\.(?<" + LIN_VER_PGRP_PATCH + ">[0-9]+)" +
            "(?:-(?<" + LIN_VER_PGRP_GIT_COMMIT_COUNT + ">[0-9]+))?"
    );

    private final ErrorReporter errorReporter;

    private final Map<DatabaseTable, AbsDatabaseDriver<?, ?, ?>> allDrivers;
    private final DbConnectionPool dbSql;
    private final DbEngine currentDbEngine;
    private final DbK8sCrd dbK8s;
    private final Provider<TransactionMgrGenerator> txMgrGenerator;
    private final Provider<TransactionMgr> txMgrProvider;
    private final CtrlConfig ctrlCfg;
    private final LinStorScope linstorScope;

    @Inject
    public DbExportImportHelper(
        ErrorReporter errorReporterRef,
        Map<DatabaseTable, AbsDatabaseDriver<?, ?, ?>> allDriversRef,
        DbEngine currentDbEngineRef,
        DbConnectionPool dbSqlRef,
        DbK8sCrd dbK8sRef,
        Provider<TransactionMgrGenerator> txMgrGeneratorRef,
        Provider<TransactionMgr> txMgrProviderRef,
        LinStorScope linstorScopeRef,
        CtrlConfig ctrlCfgRef
    )
    {

        errorReporter = errorReporterRef;
        allDrivers = allDriversRef;
        currentDbEngine = currentDbEngineRef;
        dbSql = dbSqlRef;
        dbK8s = dbK8sRef;
        txMgrGenerator = txMgrGeneratorRef;
        txMgrProvider = txMgrProviderRef;
        linstorScope = linstorScopeRef;
        ctrlCfg = ctrlCfgRef;
    }

    public void export(String fileNameRef)
    {
        List<DbExportPojoData.Table> tables = new ArrayList<>();

        TransactionMgr txMgr = txMgrGenerator.get().startTransaction();
        TransactionMgrUtil.seedTransactionMgr(linstorScope, txMgr);

        String dbConnectionUrl = ctrlCfg.getDbConnectionUrl();
        String exportedBy;
        switch (currentDbEngine.getType())
        {
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
        DbExportPojoData pojo = new DbExportPojoData(
            LinStor.VERSION_INFO_PROVIDER.getVersion(),
            System.currentTimeMillis(),
            exportedBy,
            GenCrdCurrent.VERSION,
            dbSql.getCurrentVersion(),
            dbK8s.getCurrentVersion(),
            tables
        );

        try
        {
            for (DatabaseTable table : GeneratedDatabaseTables.ALL_TABLES)
            {
                List<DbExportPojoData.Column> clmDescrList = new ArrayList<>();
                for (DatabaseTable.Column column : table.values())
                {
                    clmDescrList.add(
                        new DbExportPojoData.Column(
                            column.getName(),
                            column.getSqlType(),
                            column.isPk(),
                            column.isNullable()
                        )
                    );
                }

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
                    List<LinstorSpec<?, ?>> pojoList = dbDriver.export();
                    tables.add(
                        new DbExportPojoData.Table(
                            table.getName(),
                            clmDescrList,
                            pojoList,
                            null // we do not need this information when serializing
                        )
                    );
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

    public void importDb(String fileNameRef)
    {
        ObjectMapper om = new ObjectMapper();
        DbExportPojoMeta exportPojoMeta;
        try
        {
            // first, we only parse the meta-part of the export, thus ignoring the "table" field which is only defined
            // in DbExportPojoData
            om.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            exportPojoMeta = om.readValue(new File(fileNameRef), DbExportPojoMeta.class);
        }
        catch (IOException exc)
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_INVLD_DB_EXPORT_FILE,
                    "Failed to read database backup. File: " + fileNameRef
                ),
                exc
            );
        }

        try
        {
            Version exportPojoMetaVersion = parseVersion(exportPojoMeta.linstorVersion);
            Version currentLinstorVersion = parseVersion(LinStor.VERSION_INFO_PROVIDER.getVersion());
            if (!currentLinstorVersion.greaterOrEqual(exportPojoMetaVersion, true))
            {
                throw new DatabaseException(
                    "The given database export was created with a newer version of Linstor than currently installed! " +
                        "Current version: " + LinStor.VERSION_INFO_PROVIDER.getVersion() +
                        " version from DB export: " + exportPojoMeta.linstorVersion
                );
            }

            DbExportPojoData exportPojoData = loadExportWithData(fileNameRef, exportPojoMeta);

            String dbConnectionUrl = ctrlCfg.getDbConnectionUrl();
            switch (currentDbEngine.getType())
            {
                case K8S_CRD:
                    dbK8s.preImportMigrateToVersion(dbConnectionUrl, exportPojoData.k8sVersion);
                    break;
                case SQL:
                    dbSql.preImportMigrateToVersion(
                        DbConnectionPoolInitializer.getDbType(dbConnectionUrl),
                        exportPojoData.sqlVersion
                    );
                    break;
                default:
                    throw new ImplementationError("Unknown database type: " + currentDbEngine.getType());
            }

            TransactionMgr txMgr = txMgrGenerator.get().startTransaction();
            TransactionMgrUtil.seedTransactionMgr(linstorScope, txMgr);

            // since we are importing ALL data (including the default entries that were created by migrations), we first
            // need to truncate the table (in reverse order due to referential integration)
            errorReporter.logTrace("Deleting default data created by migration for all tables");
            List<DbExportPojoData.Table> invertedTables = new ArrayList<>(exportPojoData.tables);
            Collections.reverse(invertedTables);
            currentDbEngine.truncateAllData(invertedTables);

            for (DbExportPojoData.Table tbl : exportPojoData.tables)
            {
                errorReporter.logTrace("Importing %d entries for table %s", tbl.data.size(), tbl.name);
                currentDbEngine.importData(tbl);
            }
            txMgrProvider.get().commit();
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
    }

    private Version parseVersion(String linstorVersionRef) throws DatabaseException
    {
        Matcher matcher = LINSTOR_VERSION_PATTERN.matcher(linstorVersionRef);
        if (!matcher.find())
        {
            throw new DatabaseException("Could not parse linstor version: " + linstorVersionRef);
        }
        return new Version(
            Integer.parseInt(matcher.group(LIN_VER_PGRP_MAJOR)),
            Integer.parseInt(matcher.group(LIN_VER_PGRP_MINOR)),
            Integer.parseInt(matcher.group(LIN_VER_PGRP_PATCH)),
            matcher.group(LIN_VER_PGRP_GIT_COMMIT_COUNT)
        );
    }

    private DbExportPojoData loadExportWithData(String fileNameRef, DbExportPojoMeta pojoMetaRef)
        throws DatabaseException
    {
        /*
         * The main idea is to create 2 deserializers (encapsulated in DbExportPojoDataDeserializationHelper), one for
         * DbExportPojoData.Table and the other for the LinstorSpec interface.
         *
         * The LinstorSpec interface needs to be chosen based on 2 criteria:
         * 1) the correct GenCrd* class
         * 2) within the correct GenCrd* class, the correct table (Nodes, Resources, etc...)
         *
         * 1) can be found relatively easy since the given DbExportPojoMeta already contains the required version, and
         * current (and new) GenCrd* classes will contain an annotation for easier access of that version.
         *
         * 2) is more tricky, since we want to avoid an unnecessary "@type" field within the actual data object, since
         * it would be quite redundant within the same table.
         * That is why we also use a deserializer for the DbExportPojoData.Table, tracking the "currentTable", which is
         * later used to determine the actual class required for step 2).
         */
        ObjectMapper om = new ObjectMapper();
        SimpleModule module = new SimpleModule();

        DbExportPojoDataDeserializationHelper deserialzerHelper = new DbExportPojoDataDeserializationHelper(
            errorReporter,
            pojoMetaRef.genCrdVersion
        );
        module.addDeserializer(DbExportPojoData.Table.class, deserialzerHelper.getDbExportTableDeserializer());
        module.addDeserializer(LinstorSpec.class, deserialzerHelper.getLinstorSpecDeserializer());
        om.registerModule(module);

        try
        {
            return om.readValue(new File(fileNameRef), DbExportPojoData.class);
        }
        catch (IOException exc)
        {
            throw new DatabaseException("Failed to parse exported DB file", exc);
        }
    }
}
