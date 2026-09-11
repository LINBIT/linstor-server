package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.db.DbExportImportHelper;
import com.linbit.linstor.core.apicallhandler.controller.db.DbExportPojoData;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.locks.LockGuardFactory;
import com.linbit.locks.LockGuardFactory.LockObj;
import com.linbit.locks.LockGuardFactory.LockType;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;

import reactor.core.publisher.Flux;

@Singleton
public class CtrlExportDbApiCallHandler
{
    private final ScopeRunner scopeRunner;
    private final LockGuardFactory lockGuardFactory;
    private final DbExportImportHelper dbExportImportHelper;

    @Inject
    public CtrlExportDbApiCallHandler(
        ScopeRunner scopeRunnerRef,
        LockGuardFactory lockGuardFactoryRef,
        DbExportImportHelper dbExportImportHelperRef
    )
    {
        scopeRunner = scopeRunnerRef;
        lockGuardFactory = lockGuardFactoryRef;
        dbExportImportHelper = dbExportImportHelperRef;
    }

    /**
     * Exports the database to the given path string.
     *
     * @see #exportDatabase(Path)
     */
    public Flux<ApiCallRc> exportDatabase(String targetPathStr)
    {
        try
        {
            return exportDatabase(Paths.get(targetPathStr));
        }
        catch (InvalidPathException ipe)
        {
            throw new ApiRcException(
                ApiCallRcImpl.entryBuilder(
                    ApiConsts.FAIL_INVLD_DB_EXPORT_FILE,
                    "The parameter '" + targetPathStr + "' is not a valid path"
                )
                    .setSkipErrorReport(true)
                    .build(),
                ipe
            );
        }
    }

    /**
     * Exporting the database is internally split into two separate Fluxes:
     * <ol>
     *  <li>Collect the database dump as {@link DbExportPojoData}</li>
     *  <li>Write the {@link DbExportPojoData} into <code>targetPath</code>
     * </ol>
     * The reason for this split is simply to be able to give up the reconfiguration-write lock asap.
     *
     * @param targetPath The path where the exported JSON should be written to.
     * @return Flux that performs the export. Flux completes after the file is written.
     */
    public Flux<ApiCallRc> exportDatabase(Path targetPath)
    {
        return scopeRunner.fluxInTransactionalScope(
            "Exporting database",
            lockGuardFactory.buildDeferred(LockType.WRITE, LockObj.RECONFIGURATION),
            () -> exportDatabaseInTx()
        )
            // no locks required
            .concatMap(pojo -> writeToFile(targetPath, pojo));
    }

    private Flux<DbExportPojoData> exportDatabaseInTx()
    {
        return Flux.just(dbExportImportHelper.exportDb());
    }

    private Flux<ApiCallRc> writeToFile(Path targetPathRef, DbExportPojoData pojoRef)
    {
        dbExportImportHelper.writeTo(pojoRef, targetPathRef);
        return Flux.<ApiCallRc>just(
            ApiCallRcImpl.singleApiCallRc(ApiConsts.MASK_SUCCESS, "Exported database to " + targetPathRef)
        );
    }
}
