package com.linbit.linstor.layer.storage.utils;

import com.linbit.extproc.ExtCmd.OutputData;
import com.linbit.extproc.ExtCmdFactoryStlt;
import com.linbit.linstor.storage.StorageException;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

public class MultipathUtils
{
    public static class MultipathRow
    {
        public final String hcil;
        public final String dev;
        public final String multipathDev;
        public final String dmStatus;

        public MultipathRow(String hcilRef, String devRef, String multipathDevRef, String dmStatusRef)
        {
            hcil = hcilRef;
            dev = devRef;
            multipathDev = multipathDevRef;
            dmStatus = dmStatusRef;
        }
    }

    public static List<MultipathRow> getRowsByHCIL(ExtCmdFactoryStlt extCmdFactoryRef, ArrayList<String> hctlListRef)
        throws StorageException
    {
        List<MultipathRow> ret = new ArrayList<>();

        Predicate<String> includeToRet;
        if (hctlListRef == null || hctlListRef.isEmpty())
        {
            includeToRet = ignored -> true;
        }
        else
        {
            includeToRet = hctlListRef::contains;
        }

        OutputData outputData = Commands.genericExecutor(
            extCmdFactoryRef.create().setSaveWithoutSharedLocks(true),
            new String[] {
                "multipathd",
                "show",
                "paths",
                "format",
                /*
                 * %i == hcil / hctl
                 * %d == single device (sdb, sdc, ...)
                 * %m == multipath device (i.e. 3600c0ff0003b7c784c7f206001000000)
                 * %t == dm status (failed, undef, active ,..)
                 */
                "%i %d %m %t"
            },
            "Failed to query multipathd",
            "Failed to query multipathd"
        );

        String out = new String(outputData.stdoutData);
        String[] rows = out.split("\n");
        // skip first row (headers)
        for (int i = 1; i < rows.length; i++)
        {
            String line = rows[i];
            String[] parts = line.trim().split("\\s+");

            String hcil = parts[0];
            String dev = parts[1];
            String multipathDev = parts[2];
            String dmStatus = parts[3];

            if (includeToRet.test(hcil))
            {
                ret.add(new MultipathRow(hcil, dev, multipathDev, dmStatus));
            }
        }
        return ret;
    }

}
