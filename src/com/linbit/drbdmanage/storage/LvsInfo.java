package com.linbit.drbdmanage.storage;

import java.io.IOException;
import java.util.HashMap;

import com.linbit.ChildProcessTimeoutException;
import com.linbit.extproc.ExtCmd;
import com.linbit.extproc.ExtCmd.OutputData;

/**
 * @author Gabor Hernadi &lt;gabor.hernadi@linbit.com&gt;
 */
public class LvsInfo extends VolumeInfo
{
    // DO NOT USE "," or "." AS DELIMITER due to localization issues
    private static final String DELIMITER = ";";

    public LvsInfo(final long size, final String identifier, final String path)
    {
        super(size, identifier, path);
    }

    public static String[] getCommand(
        final String lvmLvsCommand,
        final String volumeGroup
    )
    {
        return new String[]
        {
            lvmLvsCommand,
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

        final String[] lines = stdOut.split("\n");
        for (final String line : lines)
        {
            final String[] data = line.trim().split(DELIMITER);

            final String identifier = data[0];
            final String path = data[1];
            final String rawSize = data[2];

            int indexOf = rawSize.indexOf(".");
            if (indexOf == -1)
            {
                indexOf = rawSize.indexOf(","); // localization
            }
            final String rawSizeLong = rawSize.substring(0, indexOf);
            final long size = Long.parseLong(rawSizeLong);

            final LvsInfo info = new LvsInfo(size, identifier, path);
            infoByIdentifier.put(info.getIdentifier(), info);
        }

        return infoByIdentifier;
    }
}
