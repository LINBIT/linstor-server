package com.linbit.linstor.layer.drbd.helper;

import com.linbit.extproc.ExtCmd.OutputData;
import com.linbit.extproc.ExtCmdFailedException;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.layer.drbd.resfiles.DrbdResourceFileUtils;
import com.linbit.linstor.layer.drbd.utils.DrbdAdm;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdRscData;
import com.linbit.timer.Delay;

import java.nio.charset.StandardCharsets;

/**
 * <p>Helper class that practically just wraps a "drbdadm adjust $rsc" call. Since we also need a retry logic for
 * certain scenarios as well as a (not always active) restore old resource file on failure, we extracted these
 * error-handlers into a separate class so that we can easily add additional handlers in the future.</p>
 */
public class DrbdadmAdjust
{
    private static final long[] RETRY_BACKOFF_RETRY_ON_RESIZE_NOT_ALLOWED_DURING_RESYNC = {
        500,
        1000,
        2000
    };

    private final ErrorReporter errorReporter;
    private final DrbdResourceFileUtils drbdResFileUtils;
    private final DrbdAdm utils;
    private final DrbdRscData<Resource> drbdRscData;

    private boolean restoreResFileOnFailure = false;

    private boolean skipDisk = false;
    private boolean skipNet = false;
    private boolean discard = false;

    private boolean retryOnResizeNotAllowedDuringResync = false;


    public DrbdadmAdjust(
        ErrorReporter errorReporterRef,
        DrbdAdm utilsRef,
        DrbdResourceFileUtils drbdResFileUtilsRef,
        DrbdRscData<Resource> drbdRscDataRef
    )
    {
        errorReporter = errorReporterRef;
        utils = utilsRef;
        drbdResFileUtils = drbdResFileUtilsRef;
        drbdRscData = drbdRscDataRef;
    }

    public void adjust() throws ExtCmdFailedException, StorageException
    {
        boolean retry;
        int attempt = 0;
        do
        {
            retry = false;
            try
            {
                utils.adjust(drbdRscData, skipNet, skipDisk, discard);
            }
            catch (ExtCmdFailedException exc)
            {
                @Nullable OutputData outputData = exc.getOutputData();
                retry = checkRetryOnResizeNotAllowedDuringResync(outputData, attempt);
                if (retry)
                {
                    attempt++;
                }
                else
                {
                    if (restoreResFileOnFailure)
                    {
                        attachRejectedResFile(exc);
                        drbdResFileUtils.restoreBackupResFile(drbdRscData);
                    }
                    throw exc;
                }
            }
        }
        while (retry);
    }

    /**
     * Adds the content of a freshly regenerated resource file that {@code drbdadm adjust} just failed on to the given
     * exception, so that it is part of the ErrorReport. Has to run before the backup is restored, since that
     * overwrites the file. Nothing is added if the file equals its backup, i.e. was not regenerated for this adjust.
     */
    private void attachRejectedResFile(ExtCmdFailedException excRef)
    {
        @Nullable String content = drbdResFileUtils.readChangedResFileContent(drbdRscData);
        if (content != null)
        {
            String rscName = drbdRscData.getSuffixedResourceName();
            excRef.addSuppressed(
                new StorageException(
                    "drbdadm adjust failed with a regenerated resource file of resource '" + rscName + "'",
                    "The DRBD resource file of resource '" + rscName + "' had been regenerated before the failed " +
                        "drbdadm adjust and was replaced by its last known good version afterwards",
                    null,
                    null,
                    "Content of " + drbdResFileUtils.getResFilePath(drbdRscData) + " at the time of the failure:\n" +
                        content
                )
            );
        }
    }

    private boolean checkRetryOnResizeNotAllowedDuringResync(@Nullable OutputData outputDataRef, int attemptRef)
    {
        boolean ret = false;
        if (outputDataRef != null &&
            new String(outputDataRef.stderrData, StandardCharsets.UTF_8).contains("Resize not allowed during resync") &&
            retryOnResizeNotAllowedDuringResync)
        {
            if (attemptRef < RETRY_BACKOFF_RETRY_ON_RESIZE_NOT_ALLOWED_DURING_RESYNC.length)
            {
                long sleep = RETRY_BACKOFF_RETRY_ON_RESIZE_NOT_ALLOWED_DURING_RESYNC[attemptRef];
                errorReporter.logWarning(
                    "'Resize not allowed during resync' triggered for resource %s. Sleeping for %dms (retry %d/%d).",
                    drbdRscData.getSuffixedResourceName(),
                    sleep,
                    attemptRef + 1,
                    RETRY_BACKOFF_RETRY_ON_RESIZE_NOT_ALLOWED_DURING_RESYNC.length
                );
                Delay.sleep(sleep);
                ret = true;
            }
        }
        return ret;
    }

    public DrbdadmAdjust withSkipNet(boolean skipNetRef)
    {
        skipNet = skipNetRef;
        return this;
    }

    public DrbdadmAdjust withSkipDisk(boolean skipDiskRef)
    {
        skipDisk = skipDiskRef;
        return this;
    }

    public DrbdadmAdjust withDiscard(boolean discardRef)
    {
        discard = discardRef;
        return this;
    }

    /**
     * If set to {@code true}, the std-err of a failed {@code drbdadm adjust} is scanned if it contains
     * {@code "Resize not allowed during resync"}. If so, the very same adjust call is retried after an increasing delay
     * time (see {@link DrbdadmAdjust#RETRY_BACKOFF_RETRY_ON_RESIZE_NOT_ALLOWED_DURING_RESYNC}).
     */
    public DrbdadmAdjust withRetryOnResizeNotAllowedDuringResync(boolean retryRef)
    {
        retryOnResizeNotAllowedDuringResync = retryRef;
        return this;
    }

    /**
     * If set to {@code true}, {@link DrbdResourceFileUtils#restoreBackupResFile} is
     * called if {@code drbadm adjust} fails and no other handler wants to keep retrying.
     */
    public DrbdadmAdjust withRestoreResFileOnFailure(boolean restoreRef)
    {
        restoreResFileOnFailure = restoreRef;
        return this;
    }
}
