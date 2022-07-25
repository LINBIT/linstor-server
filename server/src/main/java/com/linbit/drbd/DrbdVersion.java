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

    private static final Object SYNC_OBJ = new Object();

    private static final int HEXADECIMAL = 16;
    public static final int SHIFT_MAJOR_VSN = 16;
    public static final int SHIFT_MINOR_VSN = 8;
    public static final int MASK_VSN_ELEM = 0xFF;

    public static final Version DRBD9_VSN = new Version(9);
    public static final Version DRBD_UTILS_VSN = new Version(8, 9, 10);

    public static final int UNDETERMINED_VERSION = -1;

    private Version drbdVsn = new Version();
    private Version utilsVsn = new Version();

    private CoreTimer timerRef;
    private ErrorReporter errorLogRef;
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
    public void checkDrbdVersions()
    {
        checkKernelVersion();
        checkUtilsVersion();
    }

    /**
     * Initializes the DRBD version variable
     * <br/>
     * If the instance was unable to determine the DRBD version, an error will be raised,
     * but only if DRBD is installed.
     */
    public void checkKernelVersion()
    {
        checkVersion(false);
    }

    /**
     * Initializes the DRBD utils version variable
     * <br/>
     * If the instance was unable to determine the DRBD utils version, an error will be raised,
     * but only if DRBD is installed.
     */
    public void checkUtilsVersion()
    {
        checkVersion(true);
    }

    private void checkVersion(boolean utils)
    {
        synchronized (SYNC_OBJ)
        {
            ExtCmd cmd = new ExtCmd(timerRef, errorLogRef);
            String value = null;
            String key;
            String errorText;
            List<String> notSupportedReasons;
            if (utils)
            {
                key = KEY_UTILS_VSN_CODE;
                errorText = "DRBD utils";
                notSupportedReasons = utilsNotSupportedReasons;
            }
            else
            {
                key = KEY_DRBD_VSN_CODE;
                errorText = "DRBD kernel";
                notSupportedReasons = drbdNotSupportedReasons;
            }
            try
            {
                restoreDefaults(utils);
                OutputData cmdData = cmd.exec(VSN_QUERY_COMMAND);
                try (BufferedReader vsnReader = new BufferedReader(new InputStreamReader(cmdData.getStdoutStream())))
                {
                    String longKey = key + "=";
                    for (String vsnLine = vsnReader.readLine(); vsnLine != null; vsnLine = vsnReader.readLine())
                    {
                        if (vsnLine.startsWith(longKey))
                        {
                            value = vsnLine.substring(longKey.length());
                            break;
                        }
                    }

                    if (value != null)
                    {
                        if (value.startsWith("0x"))
                        {
                            value = value.substring(2);
                        }
                        int vsnCode = Integer.parseInt(value, HEXADECIMAL);
                        if (utils)
                        {
                            utilsVsn = new Version(
                                ((vsnCode >>> SHIFT_MAJOR_VSN) & MASK_VSN_ELEM),
                                ((vsnCode >>> SHIFT_MINOR_VSN) & MASK_VSN_ELEM),
                                (vsnCode & MASK_VSN_ELEM)
                            );
                        }
                        else
                        {
                            drbdVsn = new Version(
                                ((vsnCode >>> SHIFT_MAJOR_VSN) & MASK_VSN_ELEM),
                                ((vsnCode >>> SHIFT_MINOR_VSN) & MASK_VSN_ELEM),
                                (vsnCode & MASK_VSN_ELEM)
                            );
                        }

                        if (!hasDrbd9())
                        {
                            if (!utils)
                            {
                                drbdNotSupportedReasons.add(
                                    "DRBD version has to be >= " + DRBD9_VSN + ". Current DRBD version: " + drbdVsn
                                );
                            }
                        }
                        else if (!hasUtils())
                        {
                            if (utils)
                            {
                                utilsNotSupportedReasons.add(
                                    "DRBD utils version has to be >= " + DRBD_UTILS_VSN + ". Current utils version: " +
                                        utilsVsn
                                );
                            }
                        }
                        else
                        {
                            SYNC_OBJ.notifyAll();
                        }
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
            catch (IOException | ChildProcessTimeoutException exc)
            {
                errorLogRef.logWarning("Unable to check drbdadm version. '" + exc.getMessage() + "'");
                notSupportedReasons
                    .add(exc.getClass().getSimpleName() + " occurred when checking the " + errorText + " version");
            }
        }
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
    public int getDrbdMajorVsn()
    {
        return drbdVsn.getMajor() == null ? UNDETERMINED_VERSION : drbdVsn.getMajor();
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
        return utilsVsn.getMajor() == null ? UNDETERMINED_VERSION : utilsVsn.getMajor();
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
    public int getDrbdMinorVsn()
    {
        return drbdVsn.getMinor() == null ? UNDETERMINED_VERSION : drbdVsn.getMinor();
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
        return utilsVsn.getMinor() == null ? UNDETERMINED_VERSION : utilsVsn.getMinor();
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
    public int getDrbdPatchLvl()
    {
        return drbdVsn.getPatch() == null ? UNDETERMINED_VERSION : drbdVsn.getPatch();
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
        return utilsVsn.getPatch() == null ? UNDETERMINED_VERSION : utilsVsn.getPatch();
    }

    /**
     * Returns the DRBD version as a {@link Version}
     * <br/>
     * If the instance was unable to determine the DRBD version, all elements of the Version will
     * have the value null.
     *
     * @return full DRBD version
     */
    public Version getKernelVsn()
    {
        return drbdVsn;
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
        return utilsVsn;
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
        return drbdVsn.greaterOrEqual(DRBD9_VSN);
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
        return utilsVsn.greaterOrEqual(DRBD_UTILS_VSN);
    }

    /**
     * Resets the given version and notSupportedReasons
     */
    private void restoreDefaults(boolean utils)
    {
        if (utils)
        {
            utilsVsn = new Version();
            drbdNotSupportedReasons.clear();
        }
        else
        {
            drbdVsn = new Version();
            utilsNotSupportedReasons.clear();
        }
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
            if (!hasDrbd9())
            {
                SYNC_OBJ.wait();
            }
        }
    }

    public List<String> getKernelNotSupportedReasons()
    {
        return Collections.unmodifiableList(drbdNotSupportedReasons);
    }

    public List<String> getUtilsNotSupportedReasons()
    {
        return Collections.unmodifiableList(utilsNotSupportedReasons);
    }
}
