package com.linbit.linstor.layer.storage.utils;

import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

public class SEDUtilsTest
{

    @Test
    public void testDrivePasswordMap()
    {
        {
            Map<String, String> testmap = new HashMap<>();
            testmap.put("StorDriver/StorPoolName", "seedpool");
            testmap.put("SED/dev/nvme0n1", "rXBn05s6bIaT3vhU1bnpjNV72O");

            Map<String, String> sedMap = SEDUtils.drivePasswordMap(testmap);
            Assert.assertEquals(1, sedMap.size());
            Assert.assertEquals("rXBn05s6bIaT3vhU1bnpjNV72O", sedMap.get("/dev/nvme0n1"));
        }

        {
            Map<String, String> testmap = new HashMap<>();
            testmap.put("StorDriver/StorPoolName", "seedpool");
            testmap.put("SEDicus/whatisthis", "xxx");
            testmap.put("SED/dev/nvme0n1", "rXBn05s6bIxT3vhU1bn");
            testmap.put("SED/dev/nvme1n1", "akP5eFbs5plm4jKMk4E");

            Map<String, String> sedMap = SEDUtils.drivePasswordMap(testmap);
            Assert.assertEquals(2, sedMap.size());
            Assert.assertEquals("rXBn05s6bIxT3vhU1bn", sedMap.get("/dev/nvme0n1"));
            Assert.assertEquals("akP5eFbs5plm4jKMk4E", sedMap.get("/dev/nvme1n1"));
        }
    }
}
