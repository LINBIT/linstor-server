package com.linbit.linstor.storage;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;

import com.linbit.Checks;
import com.linbit.ChildProcessTimeoutException;
import com.linbit.InvalidNameException;
import com.linbit.SizeConv;
import com.linbit.SizeConv.SizeUnit;
import com.linbit.extproc.ExtCmd;
import com.linbit.extproc.ExtCmd.OutputData;
import com.linbit.fsevent.FileSystemWatch;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.StltConfigAccessor;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.storage.utils.Crypt;
import com.linbit.linstor.timer.CoreTimer;

/**
 * Logical Volume Manager - Storage driver for linstor
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 * @author Gabor Hernadi &lt;gabor.hernadi@linbit.com&gt;
 */
public class LvmThinDriver extends LvmDriver
{
    public static final String LVM_THIN_POOL_DEFAULT = "linstorthinpool";

    public static final String LVM_CONVERT_DEFAULT = "lvconvert";

    private static final String ID_SNAP_DELIMITER = "_";

    protected String lvmConvertCommand = LVM_CONVERT_DEFAULT;

    protected String thinPoolName = LVM_THIN_POOL_DEFAULT;

    public LvmThinDriver(
        ErrorReporter errorReporter,
        FileSystemWatch fileSystemWatch,
        CoreTimer timer,
        StorageDriverKind storageDriverKind,
        StltConfigAccessor stltCfgAccessor,
        Crypt crypt
    )
    {
        super(errorReporter, fileSystemWatch, timer, storageDriverKind, stltCfgAccessor, crypt);
    }

    @Override
    public void startVolume(final String identifier, String cryptKey, Props vlmDfnProps) throws StorageException
    {
        final String qualifiedIdentifier = volumeGroup + File.separator + identifier;
        final String[] command = new String[]
        {
            lvmChangeCommand,
            "-ay",  // activate volume
            "-K",   // these parameters are needed to set a
            // snapshot to active and enabled
            qualifiedIdentifier
        };
        try
        {
            final ExtCmd extCommand = new ExtCmd(timer, errorReporter);
            final OutputData outputData = extCommand.exec(command);
            checkExitCode(outputData, command, "Failed to start volume [%s]. ", qualifiedIdentifier);
        }
        catch (ChildProcessTimeoutException | IOException exc)
        {
            throw new StorageException(
                "Failed to start volume",
                String.format("Failed to start volume [%s]", qualifiedIdentifier),
                (exc instanceof ChildProcessTimeoutException) ?
                    "External command timed out" :
                    "External command threw an IOException",
                null,
                String.format("External command: %s", glue(command, " ")),
                exc
            );
        }
        super.startVolume(identifier, cryptKey, vlmDfnProps); // call to possibly open dm-crypt
    }

    @Override
    public void stopVolume(final String identifier, boolean isEncrypted, Props vlmDfnProps) throws StorageException
    {
        super.stopVolume(identifier, isEncrypted, vlmDfnProps); // call to possibly close dm-crypt

        final String qualifiedIdentifier = volumeGroup + File.separator + identifier;
        final String[] command = new String[]
        {
            lvmChangeCommand,
            "-an",
            qualifiedIdentifier
        };
        try
        {
            final ExtCmd extCommand = new ExtCmd(timer, errorReporter);
            final OutputData outputData = extCommand.exec(command);
            checkExitCode(outputData, command, "Failed to stop volume [%s]. ", qualifiedIdentifier);
        }
        catch (ChildProcessTimeoutException | IOException exc)
        {
            throw new StorageException(
                "Failed to stop volume",
                String.format("Failed to stop volume [%s]", qualifiedIdentifier),
                (exc instanceof ChildProcessTimeoutException) ?
                    "External command timed out" :
                    "External command threw an IOException",
                null,
                String.format("External command: %s", glue(command, " ")),
                exc
            );
        }
    }

    @Override
    public long getTotalSpace()
        throws StorageException
    {
        final String qualifiedPoolName = volumeGroup + File.separator + thinPoolName;

        long totalSpace;
        final String[] command = new String[]
            {
                lvmLvsCommand,
                qualifiedPoolName,
                "-o", "lv_size",
                "--units", "k",
                "--noheadings",
                "--nosuffix"
            };
        String rawOut = null;
        try
        {
            final ExtCmd extCommand = new ExtCmd(timer, errorReporter);
            final OutputData output = extCommand.exec(command);

            checkExitCode(output, command);

            rawOut = new String(output.stdoutData);
            totalSpace = StorageUtils.parseDecimalAsLong(rawOut.trim());
        }
        catch (NumberFormatException nfexc)
        {
            throw new StorageException(
                "Unable to parse thin pool size.",
                "Pool: " + qualifiedPoolName + "; size to parse: '" + rawOut + "'",
                null,
                null,
                "External command used to query size: " + glue(command, " "),
                nfexc
            );
        }
        catch (ChildProcessTimeoutException | IOException exc)
        {
            throw new StorageException(
                "Failed to query thin pool size",
                String.format("Failed to query the size of thin pool: %s", qualifiedPoolName),
                (exc instanceof ChildProcessTimeoutException) ?
                    "External command timed out" :
                    "External command threw an IOException",
                null,
                String.format("External command: %s", glue(command, " ")),
                exc
            );
        }

        return totalSpace;
    }

    @Override
    public long getFreeSpace() throws StorageException
    {
        final String qualifiedPoolName = volumeGroup + File.separator + thinPoolName;

        LvmCapacityStats lvmCapacityStats = getLvmCapacityStats(qualifiedPoolName);

        BigDecimal freeFraction = lvmCapacityStats.getDataFraction().negate().add(BigDecimal.valueOf(1L));
        BigInteger freeBytes = lvmCapacityStats.getThinPoolSizeBytes().multiply(freeFraction).toBigInteger();

        return SizeConv.convert(freeBytes, SizeUnit.UNIT_B, SizeUnit.UNIT_KiB).longValueExact();
    }

    @Override
    public Map<String, String> getTraits(final String identifier) throws StorageException
    {
        final long extentSize = getExtentSize("unused");

        final HashMap<String, String> traits = new HashMap<>();

        final String size = Long.toString(extentSize);
        traits.put(ApiConsts.KEY_STOR_POOL_ALLOCATION_UNIT, size);

        return traits;
    }

    @Override
    public long getAllocated(String identifier)
        throws StorageException
    {
        final String qualifiedVlmIdentifier = volumeGroup + File.separator + identifier;

        LvmCapacityStats lvmCapacityStats = getLvmCapacityStats(qualifiedVlmIdentifier);

        BigInteger allocatedBytes =
            lvmCapacityStats.getThinPoolSizeBytes().multiply(lvmCapacityStats.getDataFraction()).toBigInteger();

        return SizeConv.convert(allocatedBytes, SizeUnit.UNIT_B, SizeUnit.UNIT_KiB).longValueExact();
    }

    @Override
    protected String[] getCreateCommand(String identifier, long size)
    {
        return new String[]
        {
            lvmCreateCommand,
            "--virtualsize", size + "k", // -V
            "--thinpool", thinPoolName,
            "--name", identifier,        // -n
            volumeGroup
        };
    }

    @Override
    protected void checkConfiguration(
        Map<String, String> storPoolNamespace,
        Map<String, String> nodeNamespace,
        Map<String, String> stltNamespace
    )
        throws StorageException
    {
        super.checkConfiguration(storPoolNamespace, nodeNamespace, stltNamespace);
        checkCommand(storPoolNamespace, StorageConstants.CONFIG_LVM_CONVERT_COMMAND_KEY);
        checkThinPoolEntry(storPoolNamespace);
    }

    @Override
    protected void applyConfiguration(
        Map<String, String> storPoolNamespace,
        Map<String, String> nodeNamespace,
        Map<String, String> stltNamespace
    )
    {
        super.applyConfiguration(storPoolNamespace, nodeNamespace, stltNamespace);
        thinPoolName = getThinPoolNameFromConfig(storPoolNamespace);
        lvmConvertCommand = getAsString(
            storPoolNamespace, StorageConstants.CONFIG_LVM_CONVERT_COMMAND_KEY, lvmConvertCommand
        );
    }

    @Override
    protected String getSnapshotIdentifier(String identifier, String snapshotName)
    {
        return identifier + ID_SNAP_DELIMITER + snapshotName;
    }

    @Override
    protected String[] getCreateSnapshotCommand(String identifier, String snapshotName)
    {
        final String qualifiedIdentifier = volumeGroup + File.separator + identifier;
        final String[] command = new String[]
        {
            lvmCreateCommand,
            "--snapshot",           // -s
            "--name", getSnapshotIdentifier(identifier, snapshotName), // -n
            qualifiedIdentifier
        };
        return command;
    }

    @Override
    protected String[] getRestoreSnapshotCommand(
        String sourceIdentifier,
        String snapshotName,
        String targetIdentifier,
        boolean isEncrypted
    )
    {
        final String[] command = new String[]
        {
            lvmCreateCommand,
            "--snapshot",           // -s
            "--name", targetIdentifier, // -n
            volumeGroup + File.separator + getSnapshotIdentifier(sourceIdentifier, snapshotName)
        };
        return command;
    }

    @Override
    protected void rollbackStorageVolume(String volumeIdentifier, String snapshotName)
        throws StorageException
    {
        final String[] command = new String[]
            {
                lvmConvertCommand,
                "--merge",
                volumeGroup + File.separator + getSnapshotIdentifier(volumeIdentifier, snapshotName)
            };

        executeRollback(volumeIdentifier, snapshotName, command);

        // --merge removes the snapshot.
        // For consistency with other backends, we wish to keep the snapshot.
        // Hence we create it again here.
        // The layers above have been stopped, so the content should be identical to the original snapshot.

        createSnapshot(volumeIdentifier, snapshotName);
    }

    @Override
    protected String[] getDeleteSnapshotCommand(String identifier, String snapshotName)
    {
        return getDeleteCommand(getSnapshotIdentifier(identifier, snapshotName));
    }

    @Override
    public boolean snapshotExists(String volumeIdentifier, String snapshotName)
        throws StorageException
    {
        return storageVolumeExists(getSnapshotIdentifier(volumeIdentifier, snapshotName), VolumeType.SNAPSHOT);
    }

    private void checkThinPoolEntry(Map<String, String> config) throws StorageException
    {
        super.checkVolumeGroupEntry(config);

        String newThinPoolName = getThinPoolNameFromConfig(config).trim();
        String newBasePoolName = getVolumeGroupFromConfig(config).trim();
        checkName(newThinPoolName);

        checkThinPoolExists(config, newBasePoolName + "/" + newThinPoolName);
    }

    private void checkThinPoolExists(Map<String, String> config, String newFullThinPoolId) throws StorageException
    {
        final String[] checkCommand = new String[]
            {
                getLvmLvsCommandFromConfig(config),
                newFullThinPoolId
            };
        try
        {
            final ExtCmd extCommand = new ExtCmd(timer, errorReporter);
            final OutputData output = extCommand.exec(checkCommand);
            if (output.exitCode != 0)
            {
                throw new StorageException(
                    "Invalid configuration",
                    "Unknown thin pool",
                    String.format("Thin pool [%s] not found.", newFullThinPoolId),
                    "Specify a valid and existing thin pool name or create the desired thin pool manually",
                    null
                );
            }
        }
        catch (ChildProcessTimeoutException | IOException exc)
        {
            throw new StorageException(
                "Failed to verify thin pool name",
                null,
                (exc instanceof ChildProcessTimeoutException) ?
                    "External command timed out" :
                        "External command threw an IOException",
                        null,
                        String.format("External command: %s", glue(checkCommand, " ")),
                        exc
                );
        }
    }

    private void checkName(String newThinPoolName) throws StorageException
    {
        try
        {
            Checks.nameCheck(
                newThinPoolName,
                1,
                Integer.MAX_VALUE,
                VALID_CHARS,
                VALID_INNER_CHARS
            );
        }
        catch (InvalidNameException invalidNameExc)
        {
            throw new StorageException(
                "Invalid configuration",
                null,
                String.format("Invalid name for thin pool: %s", newThinPoolName),
                "Specify a valid and existing thin pool name",
                null
            );
        }
    }

    private String getThinPoolNameFromConfig(Map<String, String> config)
    {
        return getAsString(config, StorageConstants.CONFIG_LVM_THIN_POOL_KEY, thinPoolName);
    }

    private LvmCapacityStats getLvmCapacityStats(String qualifiedIdentifier)
        throws StorageException
    {
        LvmCapacityStats lvmCapacityStats;
        final String[] command = new String[]
            {
                lvmLvsCommand,
                qualifiedIdentifier,
                "-o", "lv_size,data_percent",
                "--separator", StorageUtils.DELIMITER,
                "--units", "b",
                "--noheadings",
                "--nosuffix"
            };
        String rawOut = null;
        try
        {
            final ExtCmd extCommand = new ExtCmd(timer, errorReporter);
            final OutputData output = extCommand.exec(command);

            checkExitCode(output, command);

            rawOut = new String(output.stdoutData);
            String[] data = rawOut.split(StorageUtils.DELIMITER);
            if (data.length != 2)
            {
                throw new StorageException(
                    "LVM capacity stats output has unexpected number of entries",
                    "LV: " + qualifiedIdentifier + "; output to parse: '" + rawOut + "'",
                    null,
                    null,
                    "External command used to query capacity stats: " + glue(command, " ")
                );
            }

            BigDecimal thinPoolSizeBytes = StorageUtils.parseDecimal(data[0].trim());

            BigDecimal dataPercent = StorageUtils.parseDecimal(data[1].trim());
            BigDecimal dataFraction = dataPercent.movePointLeft(2);

            lvmCapacityStats = new LvmCapacityStats(thinPoolSizeBytes, dataFraction);
        }
        catch (NumberFormatException nfexc)
        {
            throw new StorageException(
                "Unable to parse LVM thin pool capacity stats.",
                "LV: " + qualifiedIdentifier + "; output to parse: '" + rawOut + "'",
                null,
                null,
                "External command used to query capacity stats: " + glue(command, " "),
                nfexc
            );
        }
        catch (ChildProcessTimeoutException | IOException exc)
        {
            throw new StorageException(
                "Failed to query thin pool capacity stats",
                String.format("Failed to query the capacity stats of LV: %s", qualifiedIdentifier),
                (exc instanceof ChildProcessTimeoutException) ?
                    "External command timed out" :
                    "External command threw an IOException",
                null,
                String.format("External command: %s", glue(command, " ")),
                exc
            );
        }
        return lvmCapacityStats;
    }

    private static class LvmCapacityStats
    {
        private final BigDecimal thinPoolSizeBytes;
        private final BigDecimal dataFraction;

        LvmCapacityStats(BigDecimal thinPoolSizeBytesRef, BigDecimal dataFractionRef)
        {
            thinPoolSizeBytes = thinPoolSizeBytesRef;
            dataFraction = dataFractionRef;
        }

        public BigDecimal getThinPoolSizeBytes()
        {
            return thinPoolSizeBytes;
        }

        public BigDecimal getDataFraction()
        {
            return dataFraction;
        }
    }
}
