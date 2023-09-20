package com.linbit.linstor.layer.storage.utils;

import com.linbit.extproc.ExtCmd.OutputData;
import com.linbit.extproc.ExtCmdFactory;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.utils.Commands;

public class SdparmUtils
{
    /**
     * Calls
     *
     * <pre>
     * sdparm -i -H ${devPath}
     * </pre>
     *
     * and returns the MAC address in the format of
     *
     * <pre>
     * 00 c0 ff 29 a5 f5
     * </pre>
     *
     * Note the blanks instead of delimiter like ":"
     *
     * @param extCmdFactory
     * @param devPath
     *
     * @return
     *
     * @throws StorageException
     */
    public static String getMac(ExtCmdFactory extCmdFactory, String devPath) throws StorageException
    {
        OutputData outputData = Commands.genericExecutor(
            extCmdFactory.create().setSaveWithoutSharedLocks(true),
            new String[] {"sdparm", "-i", "-H", devPath},
            "Failed to gather inquiry information for " + devPath + " while looking for the MAC address",
            "Failed to gather inquiry information for " + devPath + " while looking for the MAC address"
        );
        String out = new String(outputData.stdoutData).trim();

        return getHexLine(1, out).substring(132, 149);
    }

    /**
     * From an example input like
     *
     * <pre>
 Device identification VPD page:
  00     00 83 00 60 01 03 00 10  60 0c 0f f0 00 29 a5 f5    ...`....`....)..
  10     6e 7b fe 5f 01 00 00 00  01 14 00 04 00 00 00 01    n{._............
  20     01 00 00 20 11 33 62 65  65 64 34 00 00 c0 ff 29    ... .3beed4....)
  30     a5 f5 00 00 ac 10 10 0c  00 c0 ff 3b 7c 78 00 00    ...........;|x..
  40     ac 10 10 0d 01 15 00 04  00 00 00 00 61 93 00 08    ............a...
  50     50 0c 0f f3 be ed 40 00  61 a3 00 08 50 0c 0f f3    P.....@.a...P...
  60     be ed 40 00                                         ..@.
     * </pre>
     *
     * This method returns one long line containing only the human-readable hex-bytes:
     * <code>00 83 00 60 01 03 00 10 60 0c 0f f0 00 29 a5 f5 6e 7b fe 5f ...</code>
     */
    private static String getHexLine(int skipFirstLines, String out)
    {
        StringBuilder ret = new StringBuilder();

        String[] lines = out.split("\n");
        // ignore the first line
        for (int idx = skipFirstLines; idx < lines.length; idx++)
        {
            ret.append(lines[idx].substring(8, 56)).append(" ");
        }

        return ret.toString().replaceAll("  ", " ");
    }
}
