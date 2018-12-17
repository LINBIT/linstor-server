package com.linbit.drbd;

import com.linbit.ChildProcessTimeoutException;
import com.linbit.extproc.ExtCmd;
import com.linbit.extproc.utils.TestExtCmd;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.storage.StorageTestUtils;
import com.linbit.linstor.testutils.EmptyErrorReporter;
import com.linbit.linstor.timer.CoreTimer;
import com.linbit.linstor.timer.CoreTimerImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static com.linbit.drbd.DrbdVersion.DRBD9_MAJOR_VSN;
import static com.linbit.drbd.DrbdVersion.UNDETERMINED_VERSION;
import static com.linbit.drbd.DrbdVersion.VSN_QUERY_COMMAND;
import static org.junit.Assert.*;
import static org.powermock.api.mockito.PowerMockito.doThrow;
import static org.powermock.api.mockito.PowerMockito.when;
import static org.powermock.api.support.membermodification.MemberMatcher.method;

import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;

@RunWith(PowerMockRunner.class)
@PrepareForTest({
        DrbdVersion.class,
        ExtCmd.class
})
public class DrbdVersionTest
{
    public static final int DRBD8_MAJOR_VSN = 8;
    private static final int PROVOKE_NUMBER_FORMAT_EXCEPTION = 0;
    private static final int PROVOKE_IO_EXCEPTION = 1;
    private static final int PROVOKE_CHILD_PROCESS_TIMEOUT_EXCEPTION = 2;

    private TestExtCmd testExtCmd;
    private DrbdVersion drbdVersion;

    public DrbdVersionTest() throws Exception
    {
        testExtCmd = new TestExtCmd();
        PowerMockito
                .whenNew(ExtCmd.class)
                .withAnyArguments()
                .thenReturn(testExtCmd);
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

    /**
     *
     * @param version
     */
    private void expectReturnVersionBehavior(final int version)
    {
        final TestExtCmd.Command command = new TestExtCmd.Command(VSN_QUERY_COMMAND);
        final ExtCmd.OutputData outputData;

        if (version == UNDETERMINED_VERSION)
        {
            outputData = new TestExtCmd.TestOutputData(
                    command.getRawCommand(),
                    "-bash: /usr/sbin/drbdadm: No such file or directory",
                    "",
                    0
            );
        }
        else if (version >= DRBD9_MAJOR_VSN)
        {
            outputData = new TestExtCmd.TestOutputData(
                    command.getRawCommand(),
                    "DRBDADM_BUILDTAG=GIT-hash:\\ db0782dac005c9c9899670feea28c249bcddbfa1\\ build\\ by\\ buildd@lcy01-amd64-008\\,\\ 2018-12-05\\ 10:50:48\n" +
                            "DRBDADM_API_VERSION=2\n" +
                            "DRBD_KERNEL_VERSION_CODE=0x090010\n" +
                            "DRBD_KERNEL_VERSION=9.0.16\n" +
                            "DRBDADM_VERSION_CODE=0x090700\n" +
                            "DRBDADM_VERSION=9.7.0",
                    "",
                    0);
        }
        else if (version == DRBD8_MAJOR_VSN)
        {
            outputData = new TestExtCmd.TestOutputData(
                    command.getRawCommand(),
                    "DRBDADM_BUILDTAG=GIT-hash:\\ db0782dac005c9c9899670feea28c249bcddbfa1\\ build\\ by\\ buildd@lcy01-amd64-008\\,\\ 2018-12-05\\ 10:50:48\n" +
                            "DRBDADM_API_VERSION=2\n" +
                            "DRBD_KERNEL_VERSION_CODE=0x08040B\n" +
                            "DRBD_KERNEL_VERSION=8.4.11\n" +
                            "DRBDADM_VERSION_CODE=0x090700\n" +
                            "DRBDADM_VERSION=9.7.0",
                    "",
                    0);
        }
        else
        {
            outputData = new TestExtCmd.TestOutputData(
                    command.getRawCommand(),
                    "",
                    "",
                    -1);
        }

        testExtCmd.setExpectedBehavior(command, outputData);
    }

    private void expectCheckVersionExceptionBehavior(final int exception)
    {
        final TestExtCmd.Command command = new TestExtCmd.Command(VSN_QUERY_COMMAND);
        ExtCmd.OutputData outputData;

        outputData = new TestExtCmd.TestOutputData(
                command.getRawCommand(),
                "DRBD_KERNEL_VERSION_CODE=0x090010\n",
                "",
                0);

        if (exception == PROVOKE_NUMBER_FORMAT_EXCEPTION)
        {
            outputData = new TestExtCmd.TestOutputData(
                    command.getRawCommand(),
                    "DRBD_KERNEL_VERSION_CODE=0xthisIsNotParsable\n",
                    "",
                    0);
        }
        else if (exception == PROVOKE_IO_EXCEPTION)
        {
            when(method(DrbdVersion.class, "checkVersion")).thenThrow(new IOException());
        }
        else if (exception == PROVOKE_CHILD_PROCESS_TIMEOUT_EXCEPTION)
        {
            when(method(DrbdVersion.class, "checkVersion")).thenThrow(new ChildProcessTimeoutException());
        }

        testExtCmd.setExpectedBehavior(command, outputData);
    }

    /**
     *
     */
    @Test
    public void checkVersionHasNoVersion()
    {
        expectReturnVersionBehavior(UNDETERMINED_VERSION);
        drbdVersion.checkVersion();
        assertEquals(UNDETERMINED_VERSION, drbdVersion.getMajorVsn());
        assertEquals(UNDETERMINED_VERSION, drbdVersion.getMinorVsn());
        assertEquals(UNDETERMINED_VERSION, drbdVersion.getPatchLvl());
    }

    /**
     *
     */
    @Test
    public void checkVersionHasVersion9()
    {
        expectReturnVersionBehavior(DRBD9_MAJOR_VSN);
        drbdVersion.checkVersion();
        assertTrue(drbdVersion.hasDrbd9());
    }

    /**
     *
     */
    @Test(expected = NumberFormatException.class)
    public void checkVersionThrowsNumberFormatException() throws NumberFormatException
    {
        expectReturnVersionBehavior(PROVOKE_NUMBER_FORMAT_EXCEPTION);
        drbdVersion.checkVersion();
        assertEquals(UNDETERMINED_VERSION, drbdVersion.getMajorVsn());
        assertEquals(UNDETERMINED_VERSION, drbdVersion.getMinorVsn());
        assertEquals(UNDETERMINED_VERSION, drbdVersion.getPatchLvl());
    }

    /**
     *
     */
    @Test(expected = IOException.class)
    public void checkVersionThrowsIOException() throws IOException
    {
        expectReturnVersionBehavior(PROVOKE_IO_EXCEPTION);
        drbdVersion.checkVersion();
        assertEquals(UNDETERMINED_VERSION, drbdVersion.getMajorVsn());
        assertEquals(UNDETERMINED_VERSION, drbdVersion.getMinorVsn());
        assertEquals(UNDETERMINED_VERSION, drbdVersion.getPatchLvl());
    }

    /**
     *
     */
    @Test(expected = ChildProcessTimeoutException.class)
    public void checkVersionThrowsChildProcessTimeoutException() throws ChildProcessTimeoutException
    {
        expectReturnVersionBehavior(PROVOKE_CHILD_PROCESS_TIMEOUT_EXCEPTION);
        drbdVersion.checkVersion();

        assertEquals(UNDETERMINED_VERSION, drbdVersion.getMajorVsn());
        assertEquals(UNDETERMINED_VERSION, drbdVersion.getMinorVsn());
        assertEquals(UNDETERMINED_VERSION, drbdVersion.getPatchLvl());
    }
}