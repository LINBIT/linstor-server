package com.linbit;

import com.linbit.extproc.ExtCmd.OutputData;
import com.linbit.extproc.ExtCmdFactoryStlt;
import com.linbit.linstor.layer.storage.utils.WmiHelper;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.storage.StorageException;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class PlatformStlt extends Platform
{
    private static String winDRBDRoot = null;
    private static String winDRBDRootWinPath = null;

    private final ExtCmdFactoryStlt extCmdFactoryStlt;
    private final ErrorReporter errorReporter;

    @Inject
    public PlatformStlt(ExtCmdFactoryStlt extCmdFactoryStltRef,
                        ErrorReporter errorReporterRef)
    {
        extCmdFactoryStlt = extCmdFactoryStltRef;
        errorReporter = errorReporterRef;
    }

    public String sysRoot()
    {
        String path = null;
        if (isLinux())
        {
            path = "";
        }
        else if (isWindows())
        {
            if (winDRBDRootWinPath == null)
            {
                try
                {
                    OutputData res;

                    res = WmiHelper.run(extCmdFactoryStlt, new String[] {
                        "registry",
                        "read-string-value",
                        "HKEY_LOCAL_MACHINE\\SYSTEM\\CurrentControlSet\\Services\\WinDRBD",
                        "WinDRBDRootWinPath"
                    });

                    winDRBDRootWinPath = new String(res.stdoutData).trim();
                }
                catch (StorageException exc)
                {
                    errorReporter.logWarning("Couldn't find registry value" +
                        "HKEY_LOCAL_MACHINE\\SYSTEM\\CurrentControlSet\\Services\\WinDRBD\\WinDRBDRootWinPath" +
                        ", is WinDRBD installed on this node?\n" + exc);
                }
            }
            path = winDRBDRootWinPath;
        }
        else
        {
            throw new ImplementationError("Platform is neither Linux nor Windows," +
                " please add support for it to LINSTOR");
        }

        return path;
    }

    public String sysRootCygwin()
    {
        String path = null;
        if (isLinux())
        {
            path = "";
        }
        else if (isWindows())
        {
            if (winDRBDRoot == null)
            {
                try
                {
                    OutputData res;

                    res = WmiHelper.run(extCmdFactoryStlt, new String[] {
                        "registry",
                        "read-string-value",
                        "HKEY_LOCAL_MACHINE\\SYSTEM\\CurrentControlSet\\Services\\WinDRBD",
                        "WinDRBDRoot"
                    });
                    winDRBDRoot = new String(res.stdoutData).trim();
                }
                catch (StorageException exc)
                {
                    errorReporter.logWarning("Couldn't find registry value " +
                        "HKEY_LOCAL_MACHINE\\SYSTEM\\CurrentControlSet\\Services\\WinDRBD\\WinDRBDRoot, " +
                        "is WinDRBD installed on this node?\n" + exc);
                }
            }
            path = winDRBDRoot;
        }
        else
        {
            throw new ImplementationError("Platform is neither Linux nor Windows," +
                " please add support for it to LINSTOR");
        }

        return path;
    }
}
