package com.linbit.linstor.layer.storage.utils;

import com.linbit.extproc.ExtCmd.OutputData;
import com.linbit.extproc.ExtCmdFactoryStlt;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.utils.Commands;

public class UdevAdmUtils
{
    public static String getScsiIdentSerial(ExtCmdFactoryStlt extCmdFactoryRef, String devPathRef)
        throws StorageException
    {
        OutputData outputData = Commands.genericExecutor(
            extCmdFactoryRef.create().setSaveWithoutSharedLocks(true),
            new String[]
            {
                "/bin/bash", "-c", "udevadm info -n " + devPathRef.substring("/dev/".length()) +
                " | grep SCSI_IDENT_SERIAL"
            },
            "Failed to grep SCSI_IDENT_SERIAL from udevadm info",
            "Failed to grep SCSI_IDENT_SERIAL from udevadm info"
        );
        String out = new String(outputData.stdoutData).trim();

        return out.split("=")[1];
    }

}
