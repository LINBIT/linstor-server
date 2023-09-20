package com.linbit.linstor.layer.storage.utils;

import com.linbit.extproc.ChildProcessHandler.TimeoutType;
import com.linbit.extproc.ExtCmd;
import com.linbit.extproc.ExtCmd.OutputData;
import com.linbit.extproc.ExtCmdFactoryStlt;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.utils.Commands;

import java.util.Arrays;

/**
 * This helper class is an interface to the linstor-wmi-helper utility.
 * (a .NET application written in C# for managing storage spaces)
 *
 * @author Johannes Thoma &lt;johannes@johannethoma.com&gt;
 */

public class WmiHelper
{
    public static String[] concatStringArrays(String[] first, String[] second)
    {
        String[] result = Arrays.copyOf(first, first.length + second.length);
        System.arraycopy(second, 0, result, first.length, second.length);
        return result;
    }


    public static OutputData run(ExtCmdFactoryStlt extCmdFactory, String[] args)
        throws StorageException
    {
        final String[] cmds = concatStringArrays(new String[] { "linstor-wmi-helper" }, args);
        ExtCmd command = extCmdFactory.create();
        command.setTimeout(TimeoutType.WAIT, 5*60*1000);

        /* Wait up to 5 minutes. Listing one backing device takes 300ms
         * on my virtual machine, so we can list up to 1000 volumes within
         * 5 minutes.
         */
        OutputData outputData = Commands.genericExecutor(
            command,
            cmds,
            "Failed to run linstor-wmi-helper utility, is it in the path?",
            "Failed to run linstor-wmi-helper utility, is it in the path?"
        );
        return outputData;
    }
}
