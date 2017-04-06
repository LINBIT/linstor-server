package com.linbit.drbdmanage.storage;

import java.io.IOException;
import java.util.HashMap;

import com.linbit.ChildProcessTimeoutException;
import com.linbit.extproc.ExtCmd;
import com.linbit.extproc.ExtCmd.OutputData;

/**
 *
 * @author Gabor Hernadi &lt;gabor.hernadi@linbit.com&gt;
 */
public class LvsInfo
{
    public long size;
    public String identifier;
    public String path;

    private static final String DELIMITER = ",";

    public LvsInfo(final long size, final String identifier, final String path)
    {
        super();
        this.size = size;
        this.identifier = identifier;
        this.path = path;
    }

    public static String[] getCommand(
        final String lvmLvsCommand,
        final String volumeGroup
    )
    {
        return new String[]
        {
            LvmThinDriver.LVM_LVS_DEFAULT,
            "-o", "lv_name,lv_path,lv_size",
            "--separator", DELIMITER,
            "--noheadings",
            "--units", "k",
            volumeGroup
        };
    }

    public static HashMap<String, LvsInfo> getAllInfo(
        final ExtCmd ec,
        final String lvmLvsCommand,
        final String volumeGroup
    ) throws ChildProcessTimeoutException, IOException
    {
        final OutputData output = ec.exec(
            LvsInfo.getCommand(lvmLvsCommand, volumeGroup)
        );

        final String stdOut = new String(output.stdoutData);

        final HashMap<String, LvsInfo> infoByIdentifier = new HashMap<>();

        final String[] lines = stdOut.trim().split("\n");
        for (final String line : lines)
        {
            final String[] data = line.split(DELIMITER);

            final String identifier = data[0];
            final String path = data[1];
            final String rawSize = data[2];

            final String rawSizeLong = rawSize.substring(0, rawSize.indexOf("."));
            final long size = Long.parseLong(rawSizeLong);

            final LvsInfo info = new LvsInfo(size, identifier, path);
            infoByIdentifier.put(info.identifier, info);
        }

        return infoByIdentifier;
    }
}
