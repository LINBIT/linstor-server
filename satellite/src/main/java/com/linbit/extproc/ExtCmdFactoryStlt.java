package com.linbit.extproc;

import com.linbit.extproc.ExtCmd.ExtCmdCondition;
import com.linbit.linstor.core.DeviceManager;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.timer.CoreTimer;

import javax.inject.Inject;
import javax.inject.Provider;

import java.util.HashSet;
import java.util.Set;

public class ExtCmdFactoryStlt extends ExtCmdFactory
{
    private final Provider<DeviceManager> devMgrProvider;
    private final Set<ExtCmd> extCmdWithSharedLocksSet;
    private final ExtCmdEndedListener extCmdEndedListener;

    private ExtCmdCondition condition;
    private boolean sharedLocks = false;

    @Inject
    public ExtCmdFactoryStlt(
        CoreTimer timerRef,
        ErrorReporter errorReporterRef,
        Provider<DeviceManager> devMgrProviderRef
    )
    {
        super(timerRef, errorReporterRef);
        devMgrProvider = devMgrProviderRef;

        extCmdWithSharedLocksSet = new HashSet<>();
        extCmdEndedListener = new ExtCmdEndedListener();
        condition = ignored -> true;
    }

    public void setUsedWithSharedLock()
    {
        sharedLocks = true;
        DeviceManager devMgr = devMgrProvider.get();
        devMgr.registerSharedExtCmdFactory(this);
        condition = extCmd -> extCmd.isSaveWithoutSharedLocks() || devMgr.hasAllSharedLocksGranted();
    }

    @Override
    public ExtCmd create()
    {
        ExtCmd extCmd = new ExtCmd(timer, errlog);
        if (sharedLocks)
        {
            extCmd.addCondition(
                condition,
                "Required shared locks are not granted"
            );
            extCmd.addExtCmdEndedListener(extCmdEndedListener);
        }
        else
        {
            extCmd.setSaveWithoutSharedLocks(true);
        }
        return extCmd;
    }

    public void killAllExtCmds()
    {
        synchronized (extCmdWithSharedLocksSet)
        {
            for (ExtCmd extCmd : extCmdWithSharedLocksSet)
            {
                extCmd.kill();
            }
        }
    }

    private class ExtCmdEndedListener implements ExtCmd.ExtCmdEndedListener
    {
        @Override
        public void extCmdEnded(ExtCmd extCmdRef)
        {
            synchronized (extCmdWithSharedLocksSet)
            {
                extCmdWithSharedLocksSet.remove(extCmdRef);
            }
        }

        @Override
        public void extCmdEnded(ExtCmd extCmdRef, Exception excRef)
        {
            synchronized (extCmdWithSharedLocksSet)
            {
                extCmdWithSharedLocksSet.remove(extCmdRef);
            }
        }
    }
}
