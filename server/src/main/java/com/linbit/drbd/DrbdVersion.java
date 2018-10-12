package com.linbit.drbd;

import com.linbit.ChildProcessTimeoutException;
import com.linbit.extproc.ExtCmd;
import com.linbit.extproc.ExtCmd.OutputData;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.timer.CoreTimer;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import javax.inject.Inject;

public class DrbdVersion
{
    public static final String[] VSN_QUERY_COMMAND = { "drbdadm", "--version" };

    public static final String KEY_VSN_CODE = "DRBD_KERNEL_VERSION_CODE";

    public static final int SHIFT_MAJOR_VSN = 16;
    public static final int SHIFT_MINOR_VSN = 8;
    public static final int MASK_VSN_ELEM = 0xFF;

    public static final short DRBD9_MAJOR_VSN = 9;

    public static final short UNDETERMINED_VERSION = -1;

    private short majorVsn = UNDETERMINED_VERSION;
    private short minorVsn = UNDETERMINED_VERSION;
    private short patchLvl = UNDETERMINED_VERSION;

    @Inject
    public DrbdVersion(CoreTimer timerRef, ErrorReporter errorLogRef)
    {
        ExtCmd cmd = new ExtCmd(timerRef, errorLogRef);
        try
        {
            OutputData cmdData = cmd.exec(VSN_QUERY_COMMAND);
            try
            (
                BufferedReader vsnReader = new BufferedReader(new InputStreamReader(cmdData.getStdoutStream()))
            )
            {
                String key = KEY_VSN_CODE + "=";
                String value = null;
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
                    int vsnCode = Integer.parseInt(value, 16);
                    majorVsn = (short) ((vsnCode >>> SHIFT_MAJOR_VSN) & MASK_VSN_ELEM);
                    minorVsn = (short) ((vsnCode >>> SHIFT_MINOR_VSN) & MASK_VSN_ELEM);
                    patchLvl = (short) (vsnCode & MASK_VSN_ELEM);
                }
                else
                {
                    throw new IOException(
                        "Cannot determine DRBD kernel module version: Constant not found in external command output"
                    );
                }
            }
        }
        catch (IOException | NumberFormatException | ChildProcessTimeoutException exc)
        {
            // TODO: Logging may be useful if the external command fails
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
        return new short[] { majorVsn, minorVsn, patchLvl };
    }

    /**
     * Indicates whether the DRBD version that was detected is at least DRBD 9
     *
     * If the instance was unable to determine the DRBD version, the method returns {@code false}.
     *
     * @return true if the DRBD major version is equal to or greater than 9, false otherwise
     */
    public boolean isDrbd9()
    {
        return majorVsn >= DRBD9_MAJOR_VSN;
    }
}
