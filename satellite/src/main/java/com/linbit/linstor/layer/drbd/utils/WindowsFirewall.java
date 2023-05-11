package com.linbit.linstor.layer.drbd.utils;

import com.linbit.ChildProcessTimeoutException;
import com.linbit.extproc.ExtCmd.OutputData;
import com.linbit.extproc.ExtCmdFactory;
import com.linbit.linstor.logging.ErrorReporter;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.io.IOException;
import java.util.Arrays;

/**
 * This helper class opens and closes specific TCP ports
 * on a Windows system. It uses the netsh utility that
 * comes with Windows.
 *
 * @author Johannes Thoma &lt;johannes@johannethoma.com&gt;
 */

@Singleton
public class WindowsFirewall
{
    private final ExtCmdFactory extCmdFactory;
    private final ErrorReporter errorReporter;

    @Inject
    public WindowsFirewall(ExtCmdFactory extCmdFactoryRef,
                           ErrorReporter errorReporterRef)
    {
        extCmdFactory = extCmdFactoryRef;
        errorReporter = errorReporterRef;
    }

    public static String[] concatStringArrays(String[] first, String[] second)
    {
        String[] result = Arrays.copyOf(first, first.length + second.length);
        System.arraycopy(second, 0, result, first.length, second.length);
        return result;
    }

    private String portToRuleName(int port)
    {
        return String.format("LINSTOR Port %d", port);
    }

    private int runNetsh(String[] commands)
    {
        String[] cmd = concatStringArrays(new String[] { "netsh", "advfirewall", "firewall" }, commands);
        int ret = -1;

        try
        {
            OutputData res = extCmdFactory.create().exec(cmd);
            ret = res.exitCode;
        }
        catch (ChildProcessTimeoutException | IOException exc)
        {
            errorReporter.logWarning("Exception when calling netsh for setting Windows Firewall rules.");
        }

        return ret;
    }


    private boolean checkIfRuleIsThere(int port)
    {
        return runNetsh(new String[] {
            "show",
            "rule",
            String.format("name=%s", portToRuleName(port))
        }) == 0;
    }

    public void openPort(int port)
    {
        if (!checkIfRuleIsThere(port))
        {
            runNetsh(new String[] {
                "add",
                "rule",
                String.format("name=%s", portToRuleName(port)),
                "protocol=tcp",
                "dir=in",
                String.format("localport=%d", port), "action=allow"
            });
        }
    }

    public void closePort(int port)
    {
        if (checkIfRuleIsThere(port))
        {
           runNetsh(new String[] {
                "delete",
                "rule",
                String.format("name=%s", portToRuleName(port))
            });
        }
    }
}

