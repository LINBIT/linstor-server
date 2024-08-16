package com.linbit.linstor.api.prop;

import com.linbit.ChildProcessTimeoutException;
import com.linbit.drbd.DrbdVersion;
import com.linbit.extproc.ExtCmd;
import com.linbit.extproc.ExtCmd.OutputData;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.timer.CoreTimer;

import javax.inject.Inject;
import javax.inject.Named;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.concurrent.locks.ReadWriteLock;

public class WhitelistPropsReconfigurator
{
    private final ErrorReporter errLog;
    private final CoreTimer timer;
    private final ReadWriteLock reconfigurationLock;

    private final WhitelistProps whitelistProps;
    private final DrbdVersion drbdVersion;

    @Inject
    public WhitelistPropsReconfigurator(
        CoreTimer timerRef,
        ErrorReporter errLogRef,
        WhitelistProps whitelistPropsRef,
        @Named(CoreModule.RECONFIGURATION_LOCK) ReadWriteLock reconfigurationLockRef,
        DrbdVersion drbdVersionRef
    )
    {
        timer = timerRef;
        errLog = errLogRef;
        whitelistProps = whitelistPropsRef;
        reconfigurationLock = reconfigurationLockRef;
        drbdVersion = drbdVersionRef;
    }

    public void reconfigure()
    {
        try
        {
            reconfigurationLock.writeLock().lock();

            whitelistProps.reconfigure(LinStorObject.CTRL);

            if (drbdVersion.hasDrbd9())
            {
                reloadDrbdOptions();
                whitelistProps.overrideDrbdProperties();
            }
        }
        catch (ChildProcessTimeoutException | IOException exc)
        {
            errLog.reportError(
                exc,
                null,
                null,
                "An exception occurred while reconfiguring drbd-whitelist properties"
            );
        }
        finally
        {
            reconfigurationLock.writeLock().unlock();
        }
    }

    private void reloadDrbdOptions() throws IOException, ChildProcessTimeoutException
    {
        ExtCmd resourceOptsExcCmd = new ExtCmd(timer, errLog);
        OutputData rscOptsData = resourceOptsExcCmd.exec("drbdsetup", "xml-help", "resource-options");
        OutputData peerDevOptsData = resourceOptsExcCmd.exec("drbdsetup", "xml-help", "peer-device-options");
        OutputData netOptsData = resourceOptsExcCmd.exec("drbdsetup", "xml-help", "new-peer");
        OutputData diskOptsData = resourceOptsExcCmd.exec("drbdsetup", "xml-help", "disk-options");
        OutputData newMinorOptsData = resourceOptsExcCmd.exec("drbdsetup", "xml-help", "new-minor");

        whitelistProps.clearDynamicProps();
        whitelistProps.appendRules(
            false, // override existing property
            new ByteArrayInputStream(rscOptsData.stdoutData),
            ApiConsts.NAMESPC_DRBD_RESOURCE_OPTIONS,
            true,
            LinStorObject.CTRL
        );
        whitelistProps.appendRules(
            false, // override existing property
            new ByteArrayInputStream(peerDevOptsData.stdoutData),
            ApiConsts.NAMESPC_DRBD_PEER_DEVICE_OPTIONS,
            true,
            LinStorObject.CTRL
        );
        whitelistProps.appendRules(
            false, // override existing property
            new ByteArrayInputStream(netOptsData.stdoutData),
            ApiConsts.NAMESPC_DRBD_NET_OPTIONS,
            true,
            LinStorObject.CTRL
        );
        whitelistProps.appendRules(
            false, // override existing property
            new ByteArrayInputStream(diskOptsData.stdoutData),
            ApiConsts.NAMESPC_DRBD_DISK_OPTIONS,
            true,
            LinStorObject.CTRL
        );
        whitelistProps.appendRules(
            false, // override existing property
            new ByteArrayInputStream(newMinorOptsData.stdoutData),
            ApiConsts.NAMESPC_DRBD_DISK_OPTIONS,
            true,
            LinStorObject.CTRL
        );
    }
}
