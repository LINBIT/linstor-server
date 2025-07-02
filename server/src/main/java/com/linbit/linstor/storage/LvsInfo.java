package com.linbit.linstor.storage;

import java.io.IOException;
import java.util.HashMap;

import com.linbit.ChildProcessTimeoutException;
import com.linbit.extproc.ExtCmd;
import com.linbit.extproc.ExtCmd.OutputData;
import com.linbit.utils.ShellUtils;

/**
 * @author Gabor Hernadi &lt;gabor.hernadi@linbit.com&gt;
 */
public class LvsInfo extends VolumeInfo
{
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
            "--separator", StorageUtils.DELIMITER,
            "--noheadings",
            "--units", "k",
            "--nosuffix",
            volumeGroup
        };
    }

    public static HashMap<String, LvsInfo> getAllInfo(
        final ExtCmd ec,
        final String lvmLvsCommand,
        final String volumeGroup
    )
        throws ChildProcessTimeoutException, IOException, StorageException
    {
        final OutputData output = ec.exec(
            LvsInfo.getCommand(lvmLvsCommand, volumeGroup)
        );

        final String stdOut = new String(output.stdoutData);

        final HashMap<String, LvsInfo> infoByIdentifier = new HashMap<>();

        final String[] lines = stdOut.split("\n");
        for (final String line : lines)
        {
            final String[] data = line.trim().split(StorageUtils.DELIMITER);
            final int expectedColCount = 3;
            if (data.length >= expectedColCount)
            {
                final String identifier = data[0];
                final String path = data[1];
                final String rawSize = data[2];

                long size;
                try
                {
                    size = StorageUtils.parseDecimalAsLong(rawSize.trim());
                }
                catch (NumberFormatException nfExc)
                {
                    throw new StorageException(
                        "Unable to parse logical volume size",
                        "Size to parse: '" + rawSize + "'",
                        null,
                        null,
                        "External command used to query logical volume info: " + ShellUtils.joinShellQuote(lvmLvsCommand),
                        nfExc
                    );
                }

                final LvsInfo info = new LvsInfo(size, identifier, path);
                infoByIdentifier.put(info.getIdentifier(), info);
            }
        }

        return infoByIdentifier;
    }
}
