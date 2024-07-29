package com.linbit.linstor.layer.storage.utils;

import com.linbit.extproc.ExtCmd.OutputData;
import com.linbit.extproc.ExtCmdFactory;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.utils.Commands;

import static com.linbit.linstor.storage.utils.Commands.genericExecutor;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class LsscsiUtils
{
    private static final Object SYNC_OBJ = new Object();
    private static final String RESCAN_PATH_FORMAT = "/sys/class/scsi_host/host%s/scan";

    private static @Nullable List<LsscsiRow> cachedLsscsiRows;

    public static class LsscsiRow
    {
        public final String hctl;
        public final String type;
        public final String devPath;

        public LsscsiRow(String hctlRef, String typeRef, String devPathRef)
        {
            hctl = hctlRef;
            type = typeRef;
            devPath = devPathRef;
        }
    }

    public static void clearCache()
    {
        synchronized (SYNC_OBJ)
        {
            cachedLsscsiRows = null;
        }
    }

    public static List<LsscsiRow> getLsscsiRowByLun(ExtCmdFactory extCmdFactory, int lun) throws StorageException
    {
        recacheIfNeeded(extCmdFactory);

        String lunStr = ":" + lun;

        List<LsscsiRow> ret = new ArrayList<>();
        for (LsscsiRow row : cachedLsscsiRows)
        {
            if (row.hctl.endsWith(lunStr))
            {
                ret.add(row);
            }
        }
        return ret;
    }

    /**
     * Basically does
     *
     * <pre>
     * echo "- - -" > /sys/class/scsi_host/host${host}/scan
     * </pre>
     *
     * for every host of the given set
     *
     * @param extCmdFactory
     * @param errReporter
     * @param hostLunMap
     *
     * @throws StorageException
     */
    public static void rescan(ExtCmdFactory extCmdFactory, ErrorReporter errReporter, Set<String> hostSet)
        throws StorageException
    {
        synchronized (SYNC_OBJ)
        {
            String filter = "- - -";
            String rescanPath = null;
            try
            {
                for (String host : hostSet)
                {
                    rescanPath = String.format(RESCAN_PATH_FORMAT, host);
                    errReporter.logTrace("rescanning %s using filter: %s", rescanPath, filter);
                    Files.write(Paths.get(rescanPath), filter.getBytes());
                }
            }
            catch (IOException exc)
            {
                throw new StorageException("Failed to rescan " + rescanPath, exc);
            }
            cachedLsscsiRows = null;
            recacheIfNeeded(extCmdFactory);
        }
    }

    public static List<LsscsiRow> getAll(ExtCmdFactory extCmdFactory) throws StorageException
    {
        recacheIfNeeded(extCmdFactory);
        return new ArrayList<>(cachedLsscsiRows);
    }

    private static void recacheIfNeeded(ExtCmdFactory extCmdFactory) throws StorageException
    {
        synchronized (SYNC_OBJ)
        {
            if (cachedLsscsiRows == null)
            {
                OutputData exec = genericExecutor(
                    extCmdFactory.create().setSaveWithoutSharedLocks(true),
                    new String[]
                    {
                        "lsscsi"
                        // do not use `lsscsi -w` as only one of the two devices will show the WWN
                        // although the other scsi device is also connected / mapped to the very same volume
                    },
                    "Failed to list scsi volumes (using 'lsscsi')",
                    "Failed to list scsi volumes (using 'lsscsi')",
                    Commands.SKIP_EXIT_CODE_CHECK
                );
                List<LsscsiRow> lsscsiRows = new ArrayList<>();
                String stdout = new String(exec.stdoutData);

                String[] lines = stdout.split("\n");
                for (String line : lines)
                {
                    String[] columns = line.split("\\s+");
                    if (columns.length == 6)
                    {
                        lsscsiRows.add(
                            new LsscsiRow(
                                // cut the leading+trailing '[', ']'
                                columns[0].substring(1, columns[0].length() - 1),
                                columns[1],
                                columns[5]
                            )
                        );
                    }
                }
                cachedLsscsiRows = lsscsiRows;
            }
        }
    }
}
