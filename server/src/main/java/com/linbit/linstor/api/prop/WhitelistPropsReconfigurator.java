package com.linbit.linstor.api.prop;

import com.linbit.ChildProcessTimeoutException;
import com.linbit.extproc.ExtCmd;
import com.linbit.extproc.ExtCmd.OutputData;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.timer.CoreTimer;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.concurrent.locks.ReadWriteLock;

import javax.inject.Inject;
import javax.inject.Named;

public class WhitelistPropsReconfigurator
{
    private ErrorReporter errLog;
    private CoreTimer timer;
    private ReadWriteLock reconfigurationLock;

    private WhitelistProps whitelistProps;

    @Inject
    public WhitelistPropsReconfigurator(
        CoreTimer timerRef,
        ErrorReporter errLogRef,
        WhitelistProps whitelistPropsRef,
        @Named(CoreModule.RECONFIGURATION_LOCK) ReadWriteLock reconfigurationLockRef
    )
    {
        timer = timerRef;
        errLog = errLogRef;
        whitelistProps = whitelistPropsRef;
        reconfigurationLock = reconfigurationLockRef;
    }

    public void reconfigure()
    {
        try
        {
            reconfigurationLock.writeLock().lock();

            whitelistProps.reconfigure(LinStorObject.CONTROLLER);

            ExtCmd resourceOptsExcCmd = new ExtCmd(timer, errLog);
            OutputData rscOptsData = resourceOptsExcCmd.exec("drbdsetup", "xml-help", "resource-options");
            OutputData peerDevOptsData = resourceOptsExcCmd.exec("drbdsetup", "xml-help", "peer-device-options");
            OutputData netOptsData = resourceOptsExcCmd.exec("drbdsetup", "xml-help", "new-peer");
            OutputData diskOptsData = resourceOptsExcCmd.exec("drbdsetup", "xml-help", "disk-options");

            whitelistProps.appendRules(
                false, // override existing property
                new ByteArrayInputStream(rscOptsData.stdoutData),
                ApiConsts.NAMESPC_DRBD_RESOURCE_OPTIONS,
                LinStorObject.CONTROLLER
            );
            whitelistProps.appendRules(
                false, // override existing property
                new ByteArrayInputStream(peerDevOptsData.stdoutData),
                ApiConsts.NAMESPC_DRBD_PEER_DEVICE_OPTIONS,
                LinStorObject.CONTROLLER
            );
            whitelistProps.appendRules(
                false, // override existing property
                new ByteArrayInputStream(netOptsData.stdoutData),
                ApiConsts.NAMESPC_DRBD_NET_OPTIONS,
                LinStorObject.CONTROLLER
            );
            whitelistProps.appendRules(
                false, // override existing property
                new ByteArrayInputStream(diskOptsData.stdoutData),
                ApiConsts.NAMESPC_DRBD_DISK_OPTIONS,
                LinStorObject.CONTROLLER
            );
        }
        catch (ChildProcessTimeoutException | IOException exc)
        {
            errLog.reportError(
                exc,
                null,
                null,
                "An exception occured while reconfiguring drbd-whitelist properties"
            );
        }
        finally
        {
            reconfigurationLock.writeLock().unlock();
        }
    }
}
