package com.linbit.drbd;

import com.linbit.ChildProcessTimeoutException;
import com.linbit.extproc.ExtCmd;
import com.linbit.extproc.ExtCmd.OutputData;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.logging.ErrorReporter;
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

    public static final String KEY_VSN_CODE = "DRBD_KERNEL_VERSION_CODE";

    private static final int HEXADECIMAL = 16;
    public static final int SHIFT_MAJOR_VSN = 16;
    public static final int SHIFT_MINOR_VSN = 8;
    public static final int MASK_VSN_ELEM = 0xFF;

    public static final short DRBD9_MAJOR_VSN = 9;

    public static final short UNDETERMINED_VERSION = -1;

    private short majorVsn = UNDETERMINED_VERSION;
    private short minorVsn = UNDETERMINED_VERSION;
    private short patchLvl = UNDETERMINED_VERSION;

    private CoreTimer timerRef;
    private ErrorReporter errorLogRef;
    private List<String> notSupportedReasons;

    @Inject
    public DrbdVersion(CoreTimer coreTimer, ErrorReporter errorReporter)
    {
        this.timerRef = coreTimer;
        this.errorLogRef = errorReporter;
        notSupportedReasons = new ArrayList<>();
    }

    /**
     * Initializes the version variables
     *  majorVsn
     *  majorVsn
     *  patchLvl
     *
     * If the instance was unable to determine the DRBD version, an error will be raised,
     * but only if DRBD is installed.
     */
    public void checkVersion()
    {
        ExtCmd cmd = new ExtCmd(timerRef, errorLogRef);
        String value = null;
        try
        {
            restoreDefaults();
            OutputData cmdData = cmd.exec(VSN_QUERY_COMMAND);
            try (BufferedReader vsnReader = new BufferedReader(new InputStreamReader(cmdData.getStdoutStream())))
            {
                String key = KEY_VSN_CODE + "=";
                for (String vsnLine = vsnReader.readLine(); vsnLine != null; vsnLine = vsnReader.readLine())
                {
                    if (vsnLine.startsWith(key))
                    {
                        value = vsnLine.substring(key.length());
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
                    majorVsn = (short) ((vsnCode >>> SHIFT_MAJOR_VSN) & MASK_VSN_ELEM);
                    minorVsn = (short) ((vsnCode >>> SHIFT_MINOR_VSN) & MASK_VSN_ELEM);
                    patchLvl = (short) (vsnCode & MASK_VSN_ELEM);

                    if (!hasDrbd9())
                    {
                        notSupportedReasons.add(
                            "DRBD version has to be >= 9. Current DRBD version: " +
                                majorVsn + "." + minorVsn + "." + patchLvl
                        );
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
                    "The value of the " + KEY_VSN_CODE + " field in the output of the " + DRBD_UTILS_CMD +
                            "utility is unparsable",
                    ERR_CORR_TXT,
                    "The value of the " + KEY_VSN_CODE + " field is:\n" + value,
                    nfExc));
        }
        catch (IOException | ChildProcessTimeoutException exc)
        {
            errorLogRef.logWarning("Unable to check drbdadm version. '" + exc.getMessage() + "'");
            notSupportedReasons.add(exc.getClass().getSimpleName() + " occurred when checking DRBD version");
        }
    }

    /**
     * Returns the DRBD major version
     *
     * E.g., for DRBD 9.0.15, the method will yield the value 9
     *
     * If the instance was unable to determine the DRBD version,
     * the method returns {@code UNDETERMINED_VERSION (-1)}.
     *
     * @return DRBD major version (0 - 255), if successfully determined; UNDETERMINED_VERSION (-1) otherwise.
     */
    public short getMajorVsn()
    {
        return majorVsn;
    }

    /**
     * Returns the DRBD minor version
     *
     * E.g., for DRBD 9.0.15, the method will yield the value 0
     *
     * If the instance was unable to determine the DRBD version,
     * the method returns {@code UNDETERMINED_VERSION (-1)}.
     *
     * @return DRBD minor version (0 - 255), if successfully determined; UNDETERMINED_VERSION (-1) otherwise.
     */
    public short getMinorVsn()
    {
        return minorVsn;
    }

    /**
     * Returns the DRBD patch level
     *
     * E.g., for DRBD 9.0.15, the method will yield the value 15
     *
     * If the instance was unable to determine the DRBD version,
     * the method returns {@code UNDETERMINED_VERSION (-1)}.
     *
     * @return DRBD patch level (0 - 255), if successfully determined; UNDETERMINED_VERSION (-1) otherwise.
     */
    public short getPatchLvl()
    {
        return patchLvl;
    }

    /**
     * Returns an array 3 of element type short, containing the DRBD version
     *
     * Array indexes of the DRBD version information parts:
     *   0 DRBD major version
     *   1 DRBD minor version
     *   2 DRBD patch level
     *
     * E.g., for DRBD 9.0.15, a call of the form
     *   {@code short[] drbdVsn = instance.getVsn();}
     * will yield the following values:
     *   drbdVsn[0] == 9
     *   drbdVsn[1] == 0
     *   drbdVsn[2] == 15
     *
     * If the instance was unable to determine the DRBD version, all elements of the array will
     * have the value {@code UNDETERMINED_VERSION (-1)}.
     *
     * @return Array 3 of element type short, containing the DRBD version parts
     */
    public short[] getVsn()
    {
        return new short[] {majorVsn, minorVsn, patchLvl};
    }

    /**
     * Indicates whether the DRBD version that was detected is at least DRBD 9
     *
     * If the instance was unable to determine the DRBD version, the method returns {@code false}.
     *
     * @return true if the DRBD major version is equal to or greater than 9, false otherwise
     */
    public boolean hasDrbd9()
    {
        return majorVsn >= DRBD9_MAJOR_VSN;
    }

    /**
     * Assigns the default value UNDETERMINED_VERSION to the version variables
     *  majorVsn
     *  majorVsn
     *  patchLvl
     */
    private void restoreDefaults()
    {
        majorVsn = UNDETERMINED_VERSION;
        minorVsn = UNDETERMINED_VERSION;
        patchLvl = UNDETERMINED_VERSION;
        notSupportedReasons.clear();
    }

    public List<String> getNotSupportedReasons()
    {
        return Collections.unmodifiableList(notSupportedReasons);
    }
}
