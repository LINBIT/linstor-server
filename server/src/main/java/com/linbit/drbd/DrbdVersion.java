package com.linbit.drbd;

import com.linbit.ChildProcessTimeoutException;
import com.linbit.extproc.ExtCmd;
import com.linbit.extproc.ExtCmd.OutputData;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.storage.kinds.ExtToolsInfo.Version;
import com.linbit.linstor.timer.CoreTimer;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.slf4j.event.Level;

@Singleton
public class DrbdVersion
{
    public static final String DRBD_UTILS_CMD = "drbdadm";
    public static final String[] VSN_QUERY_COMMAND = {"drbdadm", "--version"};

    public static final String LOG_TXT_CHECK_FAILED = "DRBD kernel module version check failed";
    public static final String ERR_DSC_CHECK_FAILED =
        "The application could not determine the version of the DRBD kernel module";
    public static final String ERR_CORR_TXT =
        "- Check whether the DRBD kernel module and drbd-utils package are installed\n" +
        "  and accessible by " + LinStor.PROGRAM + "\n" +
        "- Make sure the system is running a version of DRBD that is supported by " +
        LinStor.PROGRAM;

    public static final String KEY_DRBD_VSN_CODE = "DRBD_KERNEL_VERSION_CODE";
    public static final String KEY_UTILS_VSN_CODE = "DRBDADM_VERSION_CODE";
    public static final String KEY_VSN_CODE = "DRBD_KERNEL_VERSION_CODE";
    public static final String KEY_WINDRBD_VSN = "WINDRBD_VERSION";

    private static final Object SYNC_OBJ = new Object();

    private static final int HEXADECIMAL = 16;
    public static final int SHIFT_MAJOR_VSN = 16;
    public static final int SHIFT_MINOR_VSN = 8;
    public static final int MASK_VSN_ELEM = 0xFF;

    public static final Version DRBD9_VSN = new Version(9);
    public static final Version DRBD_UTILS_VSN = new Version(8, 9, 10);

    public static final int UNDETERMINED_VERSION_INT = -1;
    public static final Version UNDETERMINED_VERSION = new Version(
        UNDETERMINED_VERSION_INT,
        UNDETERMINED_VERSION_INT,
        UNDETERMINED_VERSION_INT
    );

    private Version drbdVsn = UNDETERMINED_VERSION;
    private Version utilsVsn = UNDETERMINED_VERSION;
    private String windrbdVsn = null;

    private final CoreTimer timerRef;
    private final ErrorReporter errorLogRef;
    private List<String> drbdNotSupportedReasons;
    private List<String> utilsNotSupportedReasons;

    @Inject
    public DrbdVersion(CoreTimer coreTimer, ErrorReporter errorReporter)
    {
        this.timerRef = coreTimer;
        this.errorLogRef = errorReporter;
        drbdNotSupportedReasons = new ArrayList<>();
        utilsNotSupportedReasons = new ArrayList<>();
    }

    /**
     * Initializes the DRBD kernel and utils version variables
     * <br/>
     * If the instance was unable to determine the DRBD versions, an error will be raised,
     * but only if DRBD is installed.
     */
    public void checkVersions()
    {
        synchronized (SYNC_OBJ)
        {
            try
            {
                restoreDefaults();
                ExtCmd cmd = new ExtCmd(timerRef, errorLogRef);
                OutputData cmdData = cmd.exec(VSN_QUERY_COMMAND);

                utilsVsn = getVersion(cmdData, KEY_UTILS_VSN_CODE);
                drbdVsn = getVersion(cmdData, KEY_DRBD_VSN_CODE);
                windrbdVsn = getVersionAsString(cmdData, KEY_WINDRBD_VSN);

                if (!hasDrbd9())
                {
                    drbdNotSupportedReasons.add(
                        "DRBD version has to be >= " + DRBD9_VSN + ". Current DRBD version: " + drbdVsn
                    );
                }
                else if (!hasUtils())
                {
                    utilsNotSupportedReasons.add(
                        "DRBD utils version has to be >= " + DRBD_UTILS_VSN + ". Current utils version: " +
                            utilsVsn
                    );
                }
                else
                {
                    SYNC_OBJ.notifyAll();
                }
            }
            catch (IOException | ChildProcessTimeoutException exc)
            {
                errorLogRef.logWarning("Unable to check drbdadm version. '" + exc.getMessage() + "'");
                String errMsg = exc.getClass().getSimpleName() + " occurred when checking the 'drbdadm --version'";
                drbdNotSupportedReasons.add(errMsg);
                utilsNotSupportedReasons.add(errMsg);
            }
        }
    }

    private Version getVersion(OutputData cmdDataRef, String key) throws IOException
    {
        Version ret = UNDETERMINED_VERSION;
        String value = null;
        try (BufferedReader vsnReader = new BufferedReader(new InputStreamReader(cmdDataRef.getStdoutStream())))
        {
            String longKey = key + "=";
            for (String vsnLine = vsnReader.readLine(); vsnLine != null; vsnLine = vsnReader.readLine())
            {
                if (vsnLine.startsWith(longKey))
                {
                    value = vsnLine.substring(longKey.length());
                    if (value.startsWith("0x"))
                    {
                        value = value.substring(2);
                    }

                    int vsnCode = Integer.parseInt(value, HEXADECIMAL);

                    ret = new Version(
                        ((vsnCode >>> SHIFT_MAJOR_VSN) & MASK_VSN_ELEM),
                        ((vsnCode >>> SHIFT_MINOR_VSN) & MASK_VSN_ELEM),
                        (vsnCode & MASK_VSN_ELEM)
                    );
                    break;
                }
            }
        }
        catch (NumberFormatException nfExc)
        {
            errorLogRef.reportError(
                Level.ERROR,
                new LinStorException(
                    LOG_TXT_CHECK_FAILED,
                    ERR_DSC_CHECK_FAILED,
                    "The value of the " + key + " field in the output of the " + DRBD_UTILS_CMD +
                        "utility is unparsable",
                    ERR_CORR_TXT,
                    "The value of the " + key + " field is:\n" + value,
                    nfExc
                )
            );
        }
        return ret;
    }

    private String getVersionAsString(OutputData cmdDataRef, String key) throws IOException
    {
        String ret = null;

        try (BufferedReader vsnReader = new BufferedReader(new InputStreamReader(cmdDataRef.getStdoutStream())))
        {
            String longKey = key + "=";
            for (String vsnLine = vsnReader.readLine(); vsnLine != null; vsnLine = vsnReader.readLine())
            {
                if (vsnLine.startsWith(longKey))
                {
                    ret = vsnLine.substring(longKey.length());

                    break;
                }
            }
        }
        return ret;
    }
    /**
     * Returns the DRBD major version
     * <br/>
     * E.g., for DRBD 9.0.15, the method will yield the value 9
     * <br/>
     * If the instance was unable to determine the DRBD version,
     * the method returns {@code UNDETERMINED_VERSION (-1)}.
     *
     * @return DRBD major version (0 - 255), if successfully determined; UNDETERMINED_VERSION (-1) otherwise.
     */
    public int getKModMajorVsn()
    {
        synchronized (SYNC_OBJ)
        {
            return drbdVsn.getMajor();
        }
    }

    /**
     * Returns the DRBD utils major version
     * <br/>
     * E.g., for DRBD utils 9.0.15, the method will yield the value 9
     * <br/>
     * If the instance was unable to determine the DRBD utils version,
     * the method returns {@code UNDETERMINED_VERSION (-1)}.
     *
     * @return DRBD utils major version (0 - 255), if successfully determined; UNDETERMINED_VERSION (-1) otherwise.
     */
    public int getUtilsMajorVsn()
    {
        synchronized (SYNC_OBJ)
        {
            return utilsVsn.getMajor();
        }
    }

    /**
     * Returns the DRBD minor version
     * <br/>
     * E.g., for DRBD 9.0.15, the method will yield the value 0
     * <br/>
     * If the instance was unable to determine the DRBD version,
     * the method returns {@code UNDETERMINED_VERSION (-1)}.
     *
     * @return DRBD minor version (0 - 255), if successfully determined; UNDETERMINED_VERSION (-1) otherwise.
     */
    public int getKModMinorVsn()
    {
        synchronized (SYNC_OBJ)
        {
            return drbdVsn.getMinor();
        }
    }

    /**
     * Returns the DRBD utils minor version
     * <br/>
     * E.g., for DRBD utils 9.0.15, the method will yield the value 0
     * <br/>
     * If the instance was unable to determine the DRBD utils version,
     * the method returns {@code UNDETERMINED_VERSION (-1)}.
     *
     * @return DRBD utils minor version (0 - 255), if successfully determined; UNDETERMINED_VERSION (-1) otherwise.
     */
    public int getUtilsMinorVsn()
    {
        synchronized (SYNC_OBJ)
        {
            return utilsVsn.getMinor();
        }
    }

    /**
     * Returns the DRBD patch level
     * <br/>
     * E.g., for DRBD 9.0.15, the method will yield the value 15
     * <br/>
     * If the instance was unable to determine the DRBD version,
     * the method returns {@code UNDETERMINED_VERSION (-1)}.
     *
     * @return DRBD patch level (0 - 255), if successfully determined; UNDETERMINED_VERSION (-1) otherwise.
     */
    public int getKModPatchLvl()
    {
        synchronized (SYNC_OBJ)
        {
            return drbdVsn.getPatch();
        }
    }

    /**
     * Returns the DRBD utils patch level
     * <br/>
     * E.g., for DRBD utils 9.0.15, the method will yield the value 15
     * <br/>
     * If the instance was unable to determine the DRBD utils version,
     * the method returns {@code UNDETERMINED_VERSION (-1)}.
     *
     * @return DRBD utils patch level (0 - 255), if successfully determined; UNDETERMINED_VERSION (-1) otherwise.
     */
    public int getUtilsPatchLvl()
    {
        synchronized (SYNC_OBJ)
        {
            return utilsVsn.getPatch();
        }
    }

    /**
     * Returns the DRBD version as a {@link Version}
     * <br/>
     * If the instance was unable to determine the DRBD version, all elements of the Version will
     * have the value null.
     *
     * @return full DRBD version
     */
    public Version getKModVsn()
    {
        synchronized (SYNC_OBJ)
        {
            return drbdVsn;
        }
    }

    /**
     * Returns the DRBD utils version as a {@link Version}
     * <br/>
     * If the instance was unable to determine the DRBD utils version, all elements of the Version will
     * have the value null.
     *
     * @return full DRBD utils version
     */
    public Version getUtilsVsn()
    {
        synchronized (SYNC_OBJ)
        {
            return utilsVsn;
        }
    }

    /**
     * Returns the WinDRBD version string
     *
     * If the instance was unable to determine the WinDRBD version,
     * the method returns null.
     *
     * @return WinDRBD version as reported by drbdadm --version or
     * null if driver not loaded or not a WinDRBD machine.
     */
    public String getWindrbdVsn()
    {
        synchronized (SYNC_OBJ)
        {
            return windrbdVsn;
        }
    }

    /**
     * Indicates whether the DRBD version that was detected is at least DRBD 9
     * <br/>
     * If the instance was unable to determine the DRBD version, the method returns {@code false}.
     *
     * @return true if the DRBD major version is equal to or greater than 9, false otherwise
     */
    public boolean hasDrbd9()
    {
        synchronized (SYNC_OBJ)
        {
            return drbdVsn.greaterOrEqual(DRBD9_VSN);
        }
    }

    /**
     * Indicates whether the DRBD utils version that was detected is at least DRBD-utils 9
     * <br/>
     * If the instance was unable to determine the DRBD utils version, the method returns {@code false}.
     *
     * @return true if the DRBD utils major version is equal to or greater than 9, false otherwise
     */
    public boolean hasUtils()
    {
        synchronized (SYNC_OBJ)
        {
            return utilsVsn.greaterOrEqual(DRBD_UTILS_VSN);
        }
    }

    /**
     * Resets the given version and notSupportedReasons
     */
    private void restoreDefaults()
    {
        utilsVsn = UNDETERMINED_VERSION;
        drbdVsn = UNDETERMINED_VERSION;
        windrbdVsn = null;
        drbdNotSupportedReasons.clear();
        utilsNotSupportedReasons.clear();
    }

    /**
     * Waits (blocks) until {@link #hasDrbd9()} returns true
     *
     * @throws InterruptedException
     */
    public void waitUntilDrbd9IsAvailable() throws InterruptedException
    {
        synchronized (SYNC_OBJ)
        {
            while (!hasDrbd9())
            {
                SYNC_OBJ.wait();
            }
        }
    }

    public List<String> getKernelNotSupportedReasons()
    {
        synchronized (SYNC_OBJ)
        {
            return Collections.unmodifiableList(drbdNotSupportedReasons);
        }
    }

    public List<String> getUtilsNotSupportedReasons()
    {
        synchronized (SYNC_OBJ)
        {
            return Collections.unmodifiableList(utilsNotSupportedReasons);
        }
    }
}
