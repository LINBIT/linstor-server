package com.linbit.linstor.storage.utils;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.linbit.extproc.ExtCmd;
import com.linbit.extproc.utils.TestExtCmd;
import com.linbit.extproc.utils.TestExtCmd.Command;
import com.linbit.extproc.utils.TestExtCmd.TestOutputData;
import com.linbit.linstor.layer.storage.utils.PmemUtils;
import com.linbit.linstor.storage.StorageException;

import java.util.HashSet;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest(
    {
        PmemUtils.class,
        ExtCmd.class
    }
)
public class PmenUtilsTest
{
    private TestExtCmd ec;

    public PmenUtilsTest() throws Exception
    {
        ec = new TestExtCmd();
        PowerMockito
            .whenNew(ExtCmd.class)
            .withAnyArguments()
            .thenReturn(ec);
    }

    @Before
    public void setUp() throws Exception
    {
        ec.clearBehaviors();
    }

    @After
    public void tearDown() throws Exception
    {
        StringBuilder sb = new StringBuilder();
        HashSet<Command> uncalledCommands = ec.getUncalledCommands();
        if (!uncalledCommands.isEmpty())
        {
            for (Command cmd : uncalledCommands)
            {
                sb.append(cmd).append("\n");
            }
            sb.setLength(sb.length() - 1);
            fail("Not all expected commands were called: \n" + sb.toString());
        }
    }

    private void setExpectedOutput(String out)
    {
        Command cmd = new Command("ndctl", "list");
        ec.setExpectedBehavior(cmd, new TestOutputData(cmd.getRawCommand(), out, "", 0));
    }

    @Test
    public void findTwoDevices() throws StorageException
    {
        setExpectedOutput(
            "[\n" +
                "  {\n" +
                "    \"dev\":\"namespace1.0\",\n" +
                "    \"mode\":\"fsdax\",\n" +
                "    \"map\":\"dev\",\n" +
                "    \"size\":8453619712,\n" +
                "    \"uuid\":\"e77b5119-c182-4f17-8866-bb79784dbedc\",\n" +
                "    \"sector_size\":512,\n" +
                "    \"blockdev\":\"pmem1\",\n" +
                "    \"numa_node\":1\n" +
                "  },\n" +
                "  {\n" +
                "    \"dev\":\"namespace0.0\",\n" +
                "    \"mode\":\"fsdax\",\n" +
                "    \"map\":\"dev\",\n" +
                "    \"size\":8453619712,\n" +
                "    \"uuid\":\"68ec17a0-7815-45c3-8a80-38c83bd06df5\",\n" +
                "    \"sector_size\":512,\n" +
                "    \"blockdev\":\"pmem0\",\n" +
                "    \"numa_node\":0\n" +
                "  }\n" +
                "]"
        );

        Assert.assertTrue(PmemUtils.supportsDax(ec, "/dev/pmem0"));
        Assert.assertTrue(PmemUtils.supportsDax(ec, "/dev/pmem1"));
        Assert.assertTrue(PmemUtils.supportsDax(ec, "pmem0"));
        Assert.assertTrue(PmemUtils.supportsDax(ec, "pmem1"));
    }

    @Test
    public void findSingleDevice() throws StorageException
    {
        // ndctl does not include array [] around a single result
        setExpectedOutput(
            "  {\n" +
                "    \"dev\":\"namespace0.0\",\n" +
                "    \"mode\":\"fsdax\",\n" +
                "    \"map\":\"dev\",\n" +
                "    \"size\":8453619712,\n" +
                "    \"uuid\":\"68ec17a0-7815-45c3-8a80-38c83bd06df5\",\n" +
                "    \"sector_size\":512,\n" +
                "    \"blockdev\":\"pmem0\",\n" +
                "    \"numa_node\":0\n" +
                "  }"
        );
        Assert.assertTrue(PmemUtils.supportsDax(ec, "/dev/pmem0"));
    }

    @Test
    public void emptyResult() throws StorageException
    {
        setExpectedOutput("");
        Assert.assertFalse(PmemUtils.supportsDax(ec, "/dev/pmem0"));

        ec.clearBehaviors();
        setExpectedOutput("{}");
        Assert.assertFalse(PmemUtils.supportsDax(ec, "/dev/pmem0"));

        ec.clearBehaviors();
        setExpectedOutput("[{}]");
        Assert.assertFalse(PmemUtils.supportsDax(ec, "/dev/pmem0"));

        ec.clearBehaviors();
        setExpectedOutput("[]");
        Assert.assertFalse(PmemUtils.supportsDax(ec, "/dev/pmem0"));

        ec.clearBehaviors();
        setExpectedOutput("[{},{}]");
        Assert.assertFalse(PmemUtils.supportsDax(ec, "/dev/pmem0"));

        ec.clearBehaviors();
        setExpectedOutput("[{\"droids we are looking for\":\"no\"}]");
        Assert.assertFalse(PmemUtils.supportsDax(ec, "/dev/pmem0"));
    }

    @Test
    public void excludeNonFsdaxDevices() throws StorageException
    {
        setExpectedOutput(
            "[\n" +
                "  {\n" +
                "    \"dev\":\"namespace1.0\",\n" +
                "    \"mode\":\"dax\",\n" +
                "    \"map\":\"dev\",\n" +
                "    \"size\":8453619712,\n" +
                "    \"uuid\":\"e77b5119-c182-4f17-8866-bb79784dbedc\",\n" +
                "    \"sector_size\":512,\n" +
                "    \"blockdev\":\"pmem1\",\n" +
                "    \"numa_node\":1\n" +
                "  },\n" +
                "  {\n" +
                "    \"dev\":\"namespace0.0\",\n" +
                "    \"mode\":\"fsdax\",\n" +
                "    \"map\":\"dev\",\n" +
                "    \"size\":8453619712,\n" +
                "    \"uuid\":\"68ec17a0-7815-45c3-8a80-38c83bd06df5\",\n" +
                "    \"sector_size\":512,\n" +
                "    \"blockdev\":\"pmem0\",\n" +
                "    \"numa_node\":0\n" +
                "  }\n" +
                "]"
        );
        assertTrue(PmemUtils.supportsDax(ec, "/dev/pmem0"));
        assertTrue(PmemUtils.supportsDax(ec, "pmem0"));

        assertFalse(PmemUtils.supportsDax(ec, "pmem1"));
    }
}
