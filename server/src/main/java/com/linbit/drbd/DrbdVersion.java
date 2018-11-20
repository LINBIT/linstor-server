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

    private short majorVsn;
    private short minorVsn;
    private short patchLvl;

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

    public short getMajorVsn()
    {
        return majorVsn;
    }

    public short getMinorVsn()
    {
        return minorVsn;
    }

    public short getPatchLvl()
    {
        return patchLvl;
    }

    public short[] getVsn()
    {
        return new short[] { majorVsn, minorVsn, patchLvl };
    }

    public boolean isDrbd9()
    {
        return majorVsn >= DRBD9_MAJOR_VSN;
    }
}
