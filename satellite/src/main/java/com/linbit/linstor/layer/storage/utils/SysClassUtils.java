package com.linbit.linstor.layer.storage.utils;

import com.linbit.extproc.ExtCmd.OutputData;
import com.linbit.extproc.ExtCmdFactory;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.utils.Commands;

import java.util.HashSet;
import java.util.Set;

public class SysClassUtils
{
    public static final String[] CMD_CAT_SAS_PHY = new String[]{
        "/bin/bash", "-c", "cat /sys/class/sas_phy/*/sas_address"
    };
    public static final String[] CMD_CAT_SAS_DEVICE = new String[] {
        "/bin/bash", "-c", "cat /sys/class/sas_device/end_device-*/sas_address"
    };

    public static Set<String> getScsiInitiatorIds(ExtCmdFactory extCmdFactoryRef)
        throws StorageException
    {
        Set<String> ret = new HashSet<>(); // takes care about deduplication of lines

        OutputData outputData = Commands.genericExecutor(
            extCmdFactoryRef.create().setSaveWithoutSharedLocks(true),
            CMD_CAT_SAS_PHY,
            "Failed to query local sas_address",
            "Failed to query local sas_address"
        );

        String out = new String(outputData.stdoutData);
        for (String line : out.split("\n"))
        {
            ret.add(line.substring(2)); // cut the leading "0x"
        }
        return ret;
    }

    public static Set<String> getScsiTargetIds(ExtCmdFactory extCmdFactoryRef)
        throws StorageException
    {
        Set<String> ret = new HashSet<>(); // takes care about deduplication of lines

        OutputData outputData = Commands.genericExecutor(
            extCmdFactoryRef.create().setSaveWithoutSharedLocks(true),
            CMD_CAT_SAS_DEVICE,
            "Failed to query target sas_address",
            "Failed to query target sas_address"
        );

        String out = new String(outputData.stdoutData);

        for (String line : out.split("\n"))
        {
            ret.add(line.substring(2)); // cut the leading "0x"
        }
        return ret;
    }
}
