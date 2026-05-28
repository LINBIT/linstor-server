package com.linbit.linstor.layer.drbd.utils;

import com.linbit.extproc.ExtCmd;

import java.nio.charset.StandardCharsets;

import org.junit.Assert;
import org.junit.Test;

public class DrbdAdmTest
{
    @Test
    public void extractsMinorForBitmapLeakAttachFailure()
    {
        ExtCmd.OutputData outputData = new ExtCmd.OutputData(
            new String[] {"drbdadm", "-vvv", "adjust", "rsc"},
            """
            scheduling attach(rsc) as [0x123]
            """.getBytes(StandardCharsets.UTF_8),
            """
             [ne] minor 1111 (vol:0) disk: r=none c=/dev/zvol/data/rsc_00000
            1111: Failure: (162) Invalid configuration request
            additional info from kernel:
            already has a bitmap, this should not happen
            """.getBytes(StandardCharsets.UTF_8),
            10
        );

        Assert.assertTrue(DrbdAdm.isBitmapLeakOnAttach(outputData));
        Assert.assertEquals(Integer.valueOf(1111), DrbdAdm.extractBitmapLeakMinor(outputData));
    }

    @Test
    public void ignoresOtherAdjustFailures()
    {
        ExtCmd.OutputData outputData = new ExtCmd.OutputData(
            new String[] {"drbdadm", "-vvv", "adjust", "rsc"},
            new byte[0],
            """
            1111: Failure: (161) Device has no disk
            """.getBytes(StandardCharsets.UTF_8),
            10
        );

        Assert.assertFalse(DrbdAdm.isBitmapLeakOnAttach(outputData));
        Assert.assertNull(DrbdAdm.extractBitmapLeakMinor(outputData));
    }
}
