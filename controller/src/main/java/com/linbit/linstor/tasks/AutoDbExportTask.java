package com.linbit.linstor.tasks;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.LinStorScope;
import com.linbit.linstor.api.LinStorScope.ScopeAutoCloseable;
import com.linbit.linstor.core.apicallhandler.controller.CtrlPropsHelper;
import com.linbit.linstor.core.apicallhandler.controller.db.DbExportImportHelper;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.ReadOnlyProps;
import com.linbit.linstor.tasks.TaskScheduleService.Task;
import com.linbit.linstor.tasks.utils.CronUtils;
import com.linbit.utils.StringUtils;
import com.linbit.utils.TimeUtils;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.TemporalAccessor;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import com.cronutils.model.time.ExecutionTime;
import com.google.common.base.Objects;

@Singleton
public class AutoDbExportTask implements TaskScheduleService.Task
{
    public final static String FULL_KEY_CRON = ApiConsts.NAMESPC_AUTO_DB_EXPORT + "/" +
        ApiConsts.KEY_AUTO_DB_EXPORT_CRON;
    public final static String FULL_KEY_KEEP = ApiConsts.NAMESPC_AUTO_DB_EXPORT + "/" +
        ApiConsts.KEY_AUTO_DB_EXPORT_KEEP;
    public final static String FULL_KEY_PATH = ApiConsts.NAMESPC_AUTO_DB_EXPORT + "/" +
        ApiConsts.KEY_AUTO_DB_EXPORT_PATH;
    public final static String FULL_KEY_COMPRESS = ApiConsts.NAMESPC_AUTO_DB_EXPORT + "/" +
        ApiConsts.KEY_AUTO_DB_EXPORT_COMPRESS;

    private static final Path DFLT_BASE_PATH = Paths.get("/var/lib/linstor/");
    /** Every day at 04:00 am */
    private final static String DFLT_DB_EXPORT_CRON = "0 4 * * *";
    private final static int DFLT_KEEP_OLD_EXPORTS = 7;
    private final static Pattern DFLT_NAME_PATTERN = Pattern.compile(
        // "auto_db_export_yyyy-MM-dd_HH-mm-ss.json.gz"
        "auto_db_export_([0-9]{4}-[0-9]{2}-[0-9]{2}_[0-9]{2}-[0-9]{2}-[0-9]{2})\\.json(?:\\.gz)?"
    );

    private final ErrorReporter errorReporter;
    private final CtrlPropsHelper ctrlPropsHelper;
    private final TaskScheduleService taskScheduleService;
    private final LinStorScope linstorScope;
    private final DbExportImportHelper dbExportImportHelper;

    private volatile Path basePath = DFLT_BASE_PATH;
    private volatile @Nullable ExecutionTime cron = null; // null iff deactivated / off
    // ExecutionTime does not have a proper equals implementation so we also store the last string version to check
    // if the cron has changed or not.
    private volatile @Nullable String cronStr = null;
    private volatile int keepOld = DFLT_KEEP_OLD_EXPORTS;

    @Inject
    public AutoDbExportTask(
        ErrorReporter errorRepotertRef,
        CtrlPropsHelper ctrlPropsHelperRef,
        TaskScheduleService taskScheduleServiceRef,
        LinStorScope linstorScopeRef,
        DbExportImportHelper dbExportImportHelperRef
    )
    {
        errorReporter = errorRepotertRef;
        ctrlPropsHelper = ctrlPropsHelperRef;
        taskScheduleService = taskScheduleServiceRef;
        linstorScope = linstorScopeRef;
        dbExportImportHelper = dbExportImportHelperRef;
    }

    public void updateProps(@Nullable ApiCallRc apiCallRcRef)
    {
        ReadOnlyProps ctrlProps = ctrlPropsHelper.getCtrlPropsForView();
        String newCronStr = ctrlProps.getPropWithDefault(
            ApiConsts.KEY_AUTO_DB_EXPORT_CRON,
            ApiConsts.NAMESPC_AUTO_DB_EXPORT,
            DFLT_DB_EXPORT_CRON
        );
        @Nullable String keepStr = ctrlProps.getProp(
            ApiConsts.KEY_AUTO_DB_EXPORT_KEEP,
            ApiConsts.NAMESPC_AUTO_DB_EXPORT
        );
        @Nullable String basePathStr = ctrlProps.getProp(
            ApiConsts.KEY_AUTO_DB_EXPORT_PATH,
            ApiConsts.NAMESPC_AUTO_DB_EXPORT
        );

        if (ApiConsts.VAL_AUTO_DB_EXPORT_CRON_OFF.equalsIgnoreCase(newCronStr))
        {
            if (apiCallRcRef != null)
            {
                apiCallRcRef.add(
                    ApiCallRcImpl.simpleEntry(
                        ApiConsts.MODIFIED,
                        "Auto database exports deactivated"
                    )
                );
            }
            cron = null;
            cronStr = null;
            taskScheduleService.cancel(this);
        }
        else
        {
            boolean changed = false;

            // apply path since it is the only call that can "easily" throw (dir not existing, not a dir, etc..)
            changed |= applyPath(basePathStr, apiCallRcRef);
            changed |= applyCron(newCronStr, apiCallRcRef);
            changed |= applyKeep(keepStr, apiCallRcRef);

            // we are checking apiCallRc to prevent "reschedule" triggered by initialize - that would lead to
            // double-execution on startup.
            if (apiCallRcRef != null && changed)
            {
                taskScheduleService.rescheduleAt(this, getNextExec());
            }
        }
    }

    private boolean applyPath(@Nullable String basePathStrRef, @Nullable ApiCallRc apiCallRcRef)
    {
        boolean changed = false;
        Path newBasePath = basePathStrRef == null ? DFLT_BASE_PATH : Paths.get(basePathStrRef);
        if (!Objects.equal(basePath, newBasePath))
        {
            boolean exists = Files.exists(newBasePath);
            boolean isDirectory = Files.isDirectory(newBasePath);
            if (exists && isDirectory)
            {
                if (apiCallRcRef != null)
                {
                    apiCallRcRef.add(
                        ApiCallRcImpl.simpleEntry(
                            ApiConsts.MODIFIED,
                            "Updated path for database exports to '" + basePathStrRef + "'."
                        )
                    );
                }
                basePath = newBasePath;
                changed = true;
            }
            else
            {
                String errMsg = "is invalid";
                if (!exists)
                {
                    errMsg = "does not exist. Please create it first";
                }
                if (!isDirectory)
                {
                    errMsg = "is not a directory.";
                }
                throw new ApiRcException(
                    ApiCallRcImpl.simpleEntry(
                        ApiConsts.FAIL_INVLD_DB_EXPORT_FILE,
                        "The given path for database exports '" + basePathStrRef + "' " + errMsg
                    )
                        .setSkipErrorReport(true)
                );
            }
        }
        return changed;
    }

    private boolean applyCron(String newCronStrRef, @Nullable ApiCallRc apiCallRcRef)
    {
        boolean changed = false;
        ExecutionTime newCron = CronUtils.asCron(newCronStrRef);
        if (!Objects.equal(cronStr, newCronStrRef))
        {
            if (apiCallRcRef != null)
            {
                apiCallRcRef.add(
                    ApiCallRcImpl.simpleEntry(
                        ApiConsts.MODIFIED,
                        "Updated cron to '" + newCronStrRef + "'. Next execution: " + CronUtils.nextExec(newCron)
                    )
                        .setSkipErrorReport(true)
                );
            }
            cronStr = newCronStrRef;
            cron = newCron;
            changed = true;
        }
        return changed;
    }

    private boolean applyKeep(@Nullable String keepStrRef, @Nullable ApiCallRc apiCallRcRef)
    {
        boolean changed = false;
        int newKeepOld = keepStrRef == null ? DFLT_KEEP_OLD_EXPORTS : Integer.parseInt(keepStrRef);
        if (keepOld != newKeepOld)
        {
            if (apiCallRcRef != null)
            {
                apiCallRcRef.add(
                    ApiCallRcImpl.simpleEntry(
                        ApiConsts.MODIFIED,
                        "Updated keep to '" + newKeepOld + "'."
                    )
                        .setSkipErrorReport(true)
                );
            }
            keepOld = newKeepOld;
            changed = true;
        }
        return changed;
    }

    @Override
    public long firstRunAt()
    {
        final long ret;
        @Nullable ExecutionTime localCron = cron;
        if (localCron == null)
        {
            ret = END_TASK;
        }
        else
        {
            ZonedDateTime lastExec = CronUtils.lastExec(localCron, ZonedDateTime.now(ZoneId.systemDefault()));
            TreeMap<LocalDateTime, Path> exportsByAge = getExportsByAge();
            @Nullable LocalDateTime ceilingKey = exportsByAge.ceilingKey(lastExec.toLocalDateTime());
            if (ceilingKey == null)
            { // we missed to create a export.
                ret = Task.RUN_ASAP;
            }
            else
            {
                ret = getNextExec();
            }
        }
        return ret;
    }

    @Override
    public void initialize()
    {
        try
        {
            updateProps(null);
        }
        catch (ApiRcException exc)
        {
            errorReporter.logError("Caught %s. Disabling AutoDbExportTask.", exc.getClass().getSimpleName());
            errorReporter.reportError(exc);
            cronStr = ApiConsts.VAL_AUTO_DB_EXPORT_CRON_OFF;
        }
    }

    @Override
    public long run(long scheduledAtRef)
    {
        exportDb();
        return getNextExec();
    }

    private long getNextExec()
    {
        long ret;
        @Nullable ExecutionTime localCron = cron;
        if (localCron == null)
        {
            ret = Task.END_TASK;
        }
        else
        {
            ZonedDateTime nextExec = CronUtils.nextExec(localCron);
            ret = nextExec.toEpochSecond() * 1000;
        }
        return ret;
    }

    private void exportDb()
    {
        try (ScopeAutoCloseable scope = linstorScope.enter())
        {
            dbExportImportHelper.exportTo(getExportPath(TimeUtils.now()));
            cleanupOldExports();
        }
    }

    private Path getExportPath(LocalDateTime nowRef)
    {
        return basePath.resolve(getExportName(nowRef));
    }

    private String getExportName(TemporalAccessor tempAccessorRef)
    {
        String compressPropValue = ctrlPropsHelper.getCtrlPropsForView()
            .getPropWithDefault(
                ApiConsts.KEY_AUTO_DB_EXPORT_COMPRESS,
                ApiConsts.NAMESPC_AUTO_DB_EXPORT,
                ApiConsts.VAL_TRUE
            );
        boolean compress = StringUtils.propTrueOrYes(compressPropValue);
        return String.format(
            // the .gz suffix makes DbExportImportHelper compress the export
            "auto_db_export_%s.json%s",
            TimeUtils.DTF_NO_SPACE.format(tempAccessorRef),
            compress ? ".gz" : ""
        );
    }

    private void cleanupOldExports()
    {
        // cleanup

        TreeMap<LocalDateTime, Path> exportsByAge = getExportsByAge();
        try
        {
            while (exportsByAge.size() > keepOld)
            {
                Path path = exportsByAge.pollFirstEntry().getValue();
                errorReporter.logDebug("Deleting old database export %s", path.toString());
                Files.delete(path);
            }
        }
        catch (IOException exc)
        {
            errorReporter.reportError(exc, null, "Failed to delete old auto-export");
        }
    }

    private TreeMap<LocalDateTime, Path> getExportsByAge()
    {
        TreeMap<LocalDateTime, Path> exportsByAge = new TreeMap<>();
        try (Stream<Path> stream = Files.list(basePath))
        {
            for (Path path : stream.toList())
            {
                Matcher matcher = DFLT_NAME_PATTERN.matcher(path.toString());
                if (matcher.find())
                {
                    exportsByAge.put(
                        LocalDateTime.parse(
                            matcher.group(1),
                            TimeUtils.DTF_NO_SPACE
                        ),
                        path
                    );
                }
            }
        }
        catch (IOException exc)
        {
            errorReporter.reportError(exc, null, "Failed to list old auto-exports");
        }
        return exportsByAge;
    }

}
