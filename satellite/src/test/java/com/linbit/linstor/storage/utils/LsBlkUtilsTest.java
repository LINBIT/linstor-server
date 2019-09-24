package com.linbit.linstor.storage.utils;

import com.linbit.linstor.storage.LsBlkEntry;

import java.util.List;

import org.junit.Assert;
import org.junit.Test;

public class LsBlkUtilsTest
{
    private static final String TEST_DATA =
        "NAME=\"sr0\" PKNAME=\"\" SIZE=\"1073741312\" KNAME=\"sr0\" ROTA=\"1\" FSTYPE=\"\" MAJ:MIN=\"11:0\"\n" +
        "NAME=\"vda\" PKNAME=\"\" SIZE=\"10737418240\" KNAME=\"vda\" ROTA=\"1\" FSTYPE=\"\" MAJ:MIN=\"252:0\"\n" +
        "NAME=\"vda1\" PKNAME=\"vda\" SIZE=\"1073741824\" KNAME=\"vda1\" ROTA=\"1\" FSTYPE=\"xfs\" MAJ:MIN=\"252:1\"\n" +
        "NAME=\"vda2\" PKNAME=\"vda\" SIZE=\"9662627840\" KNAME=\"vda2\" ROTA=\"1\" FSTYPE=\"LVM2_member\" MAJ:MIN=\"252:2\"\n" +
        "NAME=\"centos_rpcentos1-root\" PKNAME=\"vda2\" SIZE=\"8585740288\" KNAME=\"dm-0\" ROTA=\"1\" FSTYPE=\"xfs\" MAJ:MIN=\"253:0\"\n" +
        "NAME=\"centos_rpcentos1-swap\" PKNAME=\"vda2\" SIZE=\"1073741824\" KNAME=\"dm-1\" ROTA=\"1\" FSTYPE=\"swap\" MAJ:MIN=\"253:1\"\n" +
        "NAME=\"vdb\" PKNAME=\"\" SIZE=\"10737418240\" KNAME=\"vdb\" ROTA=\"1\" FSTYPE=\"LVM2_member\" MAJ:MIN=\"252:16\"\n" +
        "NAME=\"mylvmpool-thinpool_tmeta\" PKNAME=\"vdb\" SIZE=\"4194304\" KNAME=\"dm-2\" ROTA=\"1\" FSTYPE=\"\" MAJ:MIN=\"254:1\"\n" +
        "NAME=\"mylvmpool-thinpool\" PKNAME=\"dm-2\" SIZE=\"524288000\" KNAME=\"dm-4\" ROTA=\"1\" FSTYPE=\"\" MAJ:MIN=\"254:2\"\n" +
        "NAME=\"mylvmpool-thinpool_tdata\" PKNAME=\"vdb\" SIZE=\"524288000\" KNAME=\"dm-3\" ROTA=\"1\" FSTYPE=\"\" MAJ:MIN=\"254:3\"\n" +
        "NAME=\"mylvmpool-thinpool\" PKNAME=\"dm-3\" SIZE=\"524288000\" KNAME=\"dm-4\" ROTA=\"1\" FSTYPE=\"\" MAJ:MIN=\"254:4\"\n" +
        "NAME=\"sda\" PKNAME=\"\" SIZE=\"536870912000\" KNAME=\"sda\" ROTA=\"0\" FSTYPE=\"\"\n" +
        "NAME=\"drbd1000\" PKNAME=\"\" SIZE=\"10737418240\" KNAME=\"drbd1000\" ROTA=\"1\" FSTYPE=\"\" MAJ:MIN=\"147:1000\"\n" +
        "NAME=\"drbd1001\" PKNAME=\"\" SIZE=\"138342400\" KNAME=\"drbd1001\" ROTA=\"1\" FSTYPE=\"\" MAJ:MIN=\"147:1001\"";

    private static final String FAIL_INPUT_MIS_SPACE =
        "NAME=\"sr0\" PKNAME=\"\" SIZE=\"1073741312\" KNAME=\"sr0\" ROTA=\"1\"FSTYPE=\"\"\n" +
            "NAME=\"vda\" PKNAME=\"\" SIZE=\"10737418240\" KNAME=\"vda\" ROTA=\"1\" FSTYPE=\"\"\n";
    @Test
    public void testLsBlkParse()
    {
        List<LsBlkEntry> lsBlkEntries = LsBlkUtils.parseLsblkOutput(TEST_DATA);
        Assert.assertEquals(14, lsBlkEntries.size());
        Assert.assertEquals("sr0", lsBlkEntries.get(0).getName());
        Assert.assertEquals("drbd1001", lsBlkEntries.get(lsBlkEntries.size() - 1).getName());
    }

    @Test(expected = RuntimeException.class)
    public void testLsBlkParseMissingSpace()
    {
        LsBlkUtils.parseLsblkOutput(FAIL_INPUT_MIS_SPACE);
    }

    @Test
    public void testFilterLsBlkEntries()
    {
        List<LsBlkEntry> lsBlkEntries = LsBlkUtils.parseLsblkOutput(TEST_DATA);
        List<LsBlkEntry> filtered = LsBlkUtils.filterDeviceCandidates(lsBlkEntries);
        Assert.assertEquals(1, filtered.size());
        Assert.assertEquals("sda", filtered.get(0).getName());
        Assert.assertFalse(filtered.get(0).isRotational());
    }
}
