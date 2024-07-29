package com.linbit.linstor.core;

import com.linbit.extproc.ExtCmd.OutputData;
import com.linbit.extproc.ExtCmdFactory;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.core.apicallhandler.StltExtToolsChecker;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.kinds.ExtTools;
import com.linbit.linstor.storage.kinds.ExtToolsInfo;
import com.linbit.linstor.storage.utils.Commands;
import com.linbit.linstor.storage.utils.MkfsUtils;

import javax.inject.Inject;

import java.util.TreeSet;

public class UdevHandler
{
    private final ErrorReporter errorReporter;
    private final ExtCmdFactory extCmdFactory;
    private final StltExtToolsChecker extTools;

    @Inject
    public UdevHandler(
        ErrorReporter errorReporterRef,
        ExtCmdFactory extCmdFactoryRef,
        StltExtToolsChecker extToolsRef
    )
    {
        errorReporter = errorReporterRef;
        extCmdFactory = extCmdFactoryRef;
        extTools = extToolsRef;
    }

    public @Nullable TreeSet<String> getSymlinks(String devicePath) throws StorageException
    {
        TreeSet<String> ret = null;
        ExtToolsInfo udevadmInfo = extTools.getExternalTools(false).get(ExtTools.UDEVADM);

        if (devicePath != null && udevadmInfo != null && udevadmInfo.isSupported())
        {
            OutputData outputData = Commands.genericExecutor(
                extCmdFactory.create(),
                new String[] {
                    "udevadm",
                    "info",
                    "-q", "symlink", // --query=symlink
                    devicePath
                },
                "Failed to query symlinks of device " + devicePath,
                "Failed to query symlinks of device " + devicePath
            );
            String symLinksRaw = new String(outputData.stdoutData).trim();
            ret = new TreeSet<>(MkfsUtils.shellSplit(symLinksRaw));
        }

        return ret;
    }
}
