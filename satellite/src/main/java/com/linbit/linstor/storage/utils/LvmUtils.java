package com.linbit.linstor.storage.utils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.linbit.SizeConv;
import com.linbit.SizeConv.SizeUnit;
import com.linbit.extproc.ExtCmd;
import com.linbit.extproc.ExtCmd.OutputData;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.StorageUtils;

import static com.linbit.linstor.storage.utils.LvmCommands.LVS_COL_ATTRIBUTES;
import static com.linbit.linstor.storage.utils.LvmCommands.LVS_COL_DATA_PERCENT;
import static com.linbit.linstor.storage.utils.LvmCommands.LVS_COL_IDENTIFIER;
import static com.linbit.linstor.storage.utils.LvmCommands.LVS_COL_PATH;
import static com.linbit.linstor.storage.utils.LvmCommands.LVS_COL_POOL_LV;
import static com.linbit.linstor.storage.utils.LvmCommands.LVS_COL_SIZE;
import static com.linbit.linstor.storage.utils.LvmCommands.LVS_COL_VG;

/**
 * @author Gabor Hernadi &lt;gabor.hernadi@linbit.com&gt;
 */
public class LvmUtils
{
    // DO NOT USE "," or "." AS DELIMITER due to localization issues
    public static final String DELIMITER = ";";
    private static final float LVM_DEFAULT_DATA_PERCENT = 100;

    private LvmUtils()
    {
    }

    public static class LvsInfo
    {
        public final String volumeGroup;
        public final String thinPool;
        public final String identifier;
        public final String path;
        public final long size;
        public final float dataPercent;
        public final String attributes;

        LvsInfo(
            String volumeGroupRef,
            String thinPoolRef,
            String identifierRef,
            String pathRef,
            long sizeRef,
            float dataPercentRef,
            String attributesRef
        )
        {
            volumeGroup = volumeGroupRef;
            thinPool = thinPoolRef;
            identifier = identifierRef;
            path = pathRef;
            size = sizeRef;
            dataPercent = dataPercentRef;
            attributes = attributesRef;
        }
    }

    public static HashMap<String, LvsInfo> getLvsInfo(
        final ExtCmd ec,
        final Set<String> volumeGroups
    )
        throws StorageException
    {
        final OutputData output = LvmCommands.lvs(ec, volumeGroups);
        final String stdOut = new String(output.stdoutData);

        final HashMap<String, LvsInfo> infoByIdentifier = new HashMap<>();

        final String[] lines = stdOut.split("\n");
        final int expectedColCount = 7;
        for (final String line : lines)
        {
            final String[] data = line.trim().split(DELIMITER);
            if (data.length == expectedColCount)
            {
                final String identifier = data[LVS_COL_IDENTIFIER];
                final String path = data[LVS_COL_PATH];
                final String sizeStr = data[LVS_COL_SIZE];
                final String vgStr = data[LVS_COL_VG];
                final String thinPoolStr;
                final float dataPercent;
                final String attributes = data[LVS_COL_ATTRIBUTES].trim();
                if (data.length <= LVS_COL_DATA_PERCENT ||
                    data[LVS_COL_POOL_LV] == null ||
                    data[LVS_COL_POOL_LV].isEmpty()
                )
                {
                    thinPoolStr = null;
                }
                else
                {
                    thinPoolStr = data[LVS_COL_POOL_LV];
                }

                String dataPercentStr = data[LVS_COL_DATA_PERCENT].trim();
                if (dataPercentStr.isEmpty())
                {
                    dataPercent = LVM_DEFAULT_DATA_PERCENT;
                }
                else
                {
                    try
                    {
                        dataPercent = StorageUtils.parseDecimalAsFloat(dataPercentStr);
                    }
                    catch (NumberFormatException nfExc)
                    {
                        throw new StorageException(
                            "Unable to parse data_percent of thin lv",
                            "Data percent to parse: '" + dataPercentStr + "'",
                            null,
                            null,
                            "External command used to query logical volume info: " +
                                String.join(" ", output.executedCommand),
                            nfExc
                        );
                    }
                }

                long size;
                try
                {
                    size = StorageUtils.parseDecimalAsLong(sizeStr.trim());
                }
                catch (NumberFormatException nfExc)
                {
                    throw new StorageException(
                        "Unable to parse logical volume size",
                        "Size to parse: '" + sizeStr + "'",
                        null,
                        null,
                        "External command used to query logical volume info: " +
                            String.join(" ", output.executedCommand),
                        nfExc
                    );
                }

                final LvsInfo state = new LvsInfo(
                    vgStr,
                    thinPoolStr,
                    identifier,
                    path,
                    size,
                    dataPercent,
                    attributes
                );
                infoByIdentifier.put(identifier, state);
            }
        }
        return infoByIdentifier;
    }


    public static Map<String, Long> getExtentSize(ExtCmd extCmd, Set<String> volumeGroups) throws StorageException
    {
        return ParseUtils.parseSimpleTable(
            LvmCommands.getExtentSize(extCmd, volumeGroups),
            DELIMITER,
            "extent size"
        );
    }

    public static Map<String, Long> getVgTotalSize(ExtCmd extCmd, Set<String> volumeGroups) throws StorageException
    {
        return ParseUtils.parseSimpleTable(
            LvmCommands.getVgTotalSize(extCmd, volumeGroups),
            DELIMITER,
            "total size"
        );
    }

    public static Map<String, Long> getVgFreeSize(ExtCmd extCmd, Set<String> volumeGroups) throws StorageException
    {
        return ParseUtils.parseSimpleTable(
            LvmCommands.getVgFreeSize(extCmd, volumeGroups),
            DELIMITER,
            "free size"
        );
    }

    public static Map<String, Long> getThinTotalSize(ExtCmd extCmd, Set<String> volumeGroups) throws StorageException
    {
        return ParseUtils.parseSimpleTable(
            LvmCommands.getVgThinTotalSize(extCmd, volumeGroups),
            DELIMITER,
            "total thin size"
        );
    }

    public static Map<String, Long> getThinFreeSize(ExtCmd extCmd, Set<String> volumeGroups) throws StorageException
    {
        final int expectedColums = 3;

        final Map<String, Long> result = new HashMap<>();

        OutputData output = LvmCommands.getVgThinFreeSize(extCmd, volumeGroups);
        final String stdOut = new String(output.stdoutData);
        final String[] lines = stdOut.split("\n");

        for (final String line : lines)
        {
            final String[] data = line.trim().split(DELIMITER);
            if (data.length == expectedColums)
            {
                try
                {
                    BigDecimal thinPoolSizeBytes = StorageUtils.parseDecimal(data[1].trim());

                    BigDecimal dataPercent = StorageUtils.parseDecimal(data[2].trim());
                    BigDecimal dataFraction = dataPercent.movePointLeft(2);
                    BigDecimal freeFraction = dataFraction.negate().add(BigDecimal.valueOf(1L));

                    BigInteger freeBytes = thinPoolSizeBytes.multiply(freeFraction).toBigInteger();
                    long freeSpace = SizeConv.convert(freeBytes, SizeUnit.UNIT_B, SizeUnit.UNIT_KiB).longValueExact();

                    result.put(
                        data[0],
                        freeSpace
                    );
                }
                catch (NumberFormatException nfExc)
                {
                    throw new StorageException(
                        "Unable to parse free thin sizes",
                        "Numeric value to parse: '" + data[1] + "'",
                        null,
                        null,
                        "External command: " + String.join(" ", output.executedCommand),
                        nfExc
                    );
                }
            }
            else
            {
                throw new StorageException(
                    "Unable to parse free thin sizes",
                    "Expected " + expectedColums + " columns, but got " + data.length,
                    "Failed to parse line: " + line,
                    null,
                    "External command: " + String.join(" ", output.executedCommand)
                );
            }
        }
        return result;
    }

    public static void checkVgExists(ExtCmd extCmd, String volumeGroup) throws StorageException
    {
        OutputData output = LvmCommands.listExistingVolumeGroups(extCmd);
        final String stdOut = new String(output.stdoutData);
        final String[] volumeGroups = stdOut.split("\n");
        boolean found = false;
        for (String vg : volumeGroups)
        {
            if (vg.trim().equals(volumeGroup.trim()))
            {
                found = true;
                break;
            }
        }
        if (!found)
        {
            throw new StorageException("Volume group '" + volumeGroup + "' not found");
        }
    }
}
