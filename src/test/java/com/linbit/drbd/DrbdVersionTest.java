package com.linbit.drbd;

import com.linbit.extproc.ExtCmd;
import com.linbit.extproc.utils.TestExtCmd;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.logging.StdErrorReporter;
import com.linbit.linstor.storage.kinds.ExtToolsInfo.Version;
import com.linbit.linstor.testutils.EmptyErrorReporter;
import com.linbit.linstor.timer.CoreTimerImpl;

import static com.linbit.drbd.DrbdVersion.UNDETERMINED_VERSION_INT;
import static com.linbit.drbd.DrbdVersion.VSN_QUERY_COMMAND;

import java.nio.file.Paths;
import java.util.HashSet;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(PowerMockRunner.class)
@PrepareForTest({
        DrbdVersion.class,
        ExtCmd.class
})
public class DrbdVersionTest
{
    public static final int DRBD8_MAJOR_VSN = 8;
    private static final int PROVOKE_NUMBER_FORMAT_EXCEPTION = 0;

    private static StdErrorReporter errorReporter;

    private TestExtCmd testExtCmd;
    private DrbdVersion drbdVersion;

    public DrbdVersionTest() throws Exception
    {
        testExtCmd = new TestExtCmd(errorReporter);
        PowerMockito
                .whenNew(ExtCmd.class)
                .withAnyArguments()
                .thenReturn(testExtCmd);
    }

    @BeforeClass
    public static void setUpClass()
    {
        errorReporter = new StdErrorReporter(
            "LINSTOR-UNITTESTS",
            Paths.get("build/test-logs"),
            true,
            "",
            null,
            null,
            () -> null
        );
    }

    @Before
    public void setUp() throws Exception
    {
        testExtCmd.clearBehaviors();
        drbdVersion = new DrbdVersion(new CoreTimerImpl(), new EmptyErrorReporter());
    }

    @After
    public void tearDown()
    {
        StringBuilder stringBuilder = new StringBuilder();
        HashSet<TestExtCmd.Command> uncalledCommands = testExtCmd.getUncalledCommands();
        if (!uncalledCommands.isEmpty())
        {
            for (TestExtCmd.Command cmd : uncalledCommands)
            {
                stringBuilder.append(cmd).append("\n");
            }
            stringBuilder.setLength(stringBuilder.length() - 1);
            fail("Not all expected commands were called: \n" + stringBuilder.toString());
        }
    }

    @AfterClass
    public static void tearDownClass() throws DatabaseException
    {
        errorReporter.shutdown();
    }

    private void setExpectedBehaviorVersion(Version kernelVsn, Version utilsVsn)
    {
        String hexFormat = "%02X%02X%02X";
        String kernelHex = String.format(hexFormat, kernelVsn.getMajor(), kernelVsn.getMinor(), kernelVsn.getPatch());
        String utilsHex = String.format(hexFormat, utilsVsn.getMajor(), utilsVsn.getMinor(), utilsVsn.getPatch());
        String stdOut = "DRBDADM_BUILDTAG=not-needed\n" +
            "DRBDADM_API_VERSION=not-needed\n" +
            "DRBD_KERNEL_VERSION_CODE=0x" + kernelHex + "\n" +
            "DRBD_KERNEL_VERSION=" + kernelVsn + "\n" +
            "DRBDADM_VERSION_CODE=0x" + utilsHex + "\n" +
            "DRBDADM_VERSION=" + utilsVsn;
        setExpectedBehavior(stdOut, "", 0);
    }

    private void setExpectedBehavior(String stdOut, String stdErr, int rc)
    {
        final TestExtCmd.Command command = new TestExtCmd.Command(VSN_QUERY_COMMAND);
        final ExtCmd.OutputData outputData = new TestExtCmd.TestOutputData(command.getRawCommand(), stdOut, stdErr, rc);
        testExtCmd.setExpectedBehavior(command, outputData);
    }

    /**
     * Runs checkVersion() without DRBD installed.
     */
    @Test
    public void checkVersionHasNoVersion()
    {
        setExpectedBehavior("", "bash: drbdadm: command not found", 127);
        drbdVersion.checkVersions();
        assertEquals(UNDETERMINED_VERSION_INT, drbdVersion.getKModMajorVsn());
        assertEquals(UNDETERMINED_VERSION_INT, drbdVersion.getKModMinorVsn());
        assertEquals(UNDETERMINED_VERSION_INT, drbdVersion.getKModPatchLvl());
    }

    /**
     * Runs checkVersion() with DRBD 9 installed.
     */
    @Test
    public void checkVersionHasVersion9()
    {
        Version expectedKernel = new Version(9, 0, 16);
        Version expectedUtils = new Version(9, 7, 0);
        setExpectedBehaviorVersion(expectedKernel, expectedUtils);
        drbdVersion.checkVersions();
        assertTrue(drbdVersion.hasDrbd9());
        assertTrue(drbdVersion.hasUtils());
        assertEquals(expectedKernel, drbdVersion.getKModVsn());
        assertEquals(expectedUtils, drbdVersion.getUtilsVsn());
    }

    @Test
    public void checkVersionHasBadKernel()
    {
        Version expectedKernel = new Version(8, 0, 16);
        Version expectedUtils = new Version(9, 7, 0);
        setExpectedBehaviorVersion(expectedKernel, expectedUtils);
        drbdVersion.checkVersions();
        assertFalse(drbdVersion.hasDrbd9());
        assertTrue(drbdVersion.hasUtils());
        assertEquals(expectedKernel, drbdVersion.getKModVsn());
        assertEquals(expectedUtils, drbdVersion.getUtilsVsn());
    }

    @Test
    public void checkVersionHasBadUtils()
    {
        Version expectedKernel = new Version(9, 0, 16);
        Version expectedUtils = new Version(8, 7, 0);
        setExpectedBehaviorVersion(expectedKernel, expectedUtils);
        drbdVersion.checkVersions();
        assertTrue(drbdVersion.hasDrbd9());
        assertFalse(drbdVersion.hasUtils());
        assertEquals(expectedKernel, drbdVersion.getKModVsn());
        assertEquals(expectedUtils, drbdVersion.getUtilsVsn());
    }

    /**
     * Runs checkVersion() with an invalid DRBD_KERNEL_VERSION_CODE from the cmd.
     */
    @Test
    public void checkVersionThrowsNumberFormatException()
    {
        setExpectedBehavior("DRBD_KERNEL_VERSION_CODE=0xthisIsNotParsable\n", "", 0);
        drbdVersion.checkVersions();
        assertEquals(UNDETERMINED_VERSION_INT, drbdVersion.getKModMajorVsn());
        assertEquals(UNDETERMINED_VERSION_INT, drbdVersion.getKModMinorVsn());
        assertEquals(UNDETERMINED_VERSION_INT, drbdVersion.getKModPatchLvl());
    }
}
