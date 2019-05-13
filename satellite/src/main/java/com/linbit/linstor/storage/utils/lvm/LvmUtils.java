package com.linbit.linstor.storage.utils.lvm;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linbit.SizeConv;
import com.linbit.SizeConv.SizeUnit;
import com.linbit.extproc.ExtCmd;
import com.linbit.extproc.ExtCmd.OutputData;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.StorageUtils;
import com.linbit.utils.ExceptionThrowingFunction;

/**
 * @author Gabor Hernadi &lt;gabor.hernadi@linbit.com&gt;
 */
public class LvmUtils
{
    // DO NOT USE "," or "." AS DELIMITER due to localization issues
    public static final String DELIMITER = ";";
    private static final float LVM_DEFAULT_DATA_PERCENT = 100;
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

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

        JsonLvs lvs;
        try
        {
            lvs = OBJECT_MAPPER.readValue(stdOut, JsonLvs.class);
        }
        catch (IOException exc)
        {
            throw new StorageException("Failed to parse lvs output", exc);
        }

        for (JsonLvs.Entry report : lvs.reportList)
        {
            for (JsonLvs.LogicalVolume lv : report.lvList)
            {
                final float dataPercent = parseFloat(
                    lv.dataPercentStr.trim(),
                    LVM_DEFAULT_DATA_PERCENT,
                    "data_percent of thin lv",
                    output
                );

                final long size = asSize(
                    lv.sizeStr.trim(),
                    "logical volume size",
                    output
                );

                final LvsInfo state = new LvsInfo(
                    lv.volumeGroup,
                    lv.thinPool,
                    lv.name,
                    lv.path,
                    size,
                    dataPercent,
                    lv.attributes
                );
                infoByIdentifier.put(lv.volumeGroup + "/" + lv.name, state);
            }
        }

        return infoByIdentifier;
    }

    private static long parseLong(
        String str,
        Long defaultvalue,
        String description,
        OutputData output
    )
        throws StorageException
    {
        long ret;
        try
        {
            if (str == null || str.isEmpty())
            {
                if (defaultvalue == null)
                {
                    throw new StorageException("Cannot parse string '" + str + "'");
                }
                ret = defaultvalue;
            }
            else
            {
                ret = StorageUtils.parseDecimalAsLong(str);
            }
        }
        catch (NumberFormatException nfExc)
        {
            throw new StorageException(
                "Unable to parse " + description,
                "String to parse: '" + str + "'",
                null,
                null,
                "External command used to query logical volume info: " +
                    String.join(" ", output.executedCommand),
                nfExc
            );
        }
        return ret;
    }

    private static float parseFloat(
        String str,
        float defaultValue,
        String description,
        final OutputData output
    )
        throws StorageException
    {
        final float ret;
        if (str.isEmpty())
        {
            ret = defaultValue;
        }
        else
        {
            try
            {
                ret = StorageUtils.parseDecimalAsFloat(str);
            }
            catch (NumberFormatException nfExc)
            {
                throw new StorageException(
                    "Unable to parse " + description,
                    "String failed to parse: '" + str + "'",
                    null,
                    null,
                    "External command used to query logical volume info: " +
                        String.join(" ", output.executedCommand),
                    nfExc
                );
            }
        }
        return ret;
    }

    public static Map<String, Long> getExtentSize(ExtCmd extCmd, Set<String> volumeGroups) throws StorageException
    {
        final OutputData outputData = LvmCommands.getExtentSize(extCmd, volumeGroups);

        return parseVgsData(
            outputData,
            sizeExtractor(vg -> vg.extentSizeStr, "extent size", outputData)
        );
    }

    public static Map<String, Long> getVgTotalSize(ExtCmd extCmd, Set<String> volumeGroups) throws StorageException
    {
        final OutputData outputData = LvmCommands.getVgTotalSize(extCmd, volumeGroups);
        return parseVgsData(
            outputData,
            sizeExtractor(vg -> vg.capacityStr, "total size", outputData)
        );
    }

    public static Map<String, Long> getVgFreeSize(ExtCmd extCmd, Set<String> volumeGroups) throws StorageException
    {
        final OutputData outputData = LvmCommands.getVgFreeSize(extCmd, volumeGroups);
        return parseVgsData(
            outputData,
            sizeExtractor(vg -> vg.freeStr, "free size", outputData)
        );
    }

    public static Map<String, Long> getThinTotalSize(ExtCmd extCmd, Set<String> volumeGroups) throws StorageException
    {
        final OutputData outputData = LvmCommands.getVgThinTotalSize(extCmd, volumeGroups);
        return parseLvsData(
            outputData,
            sizeExtractor(lv -> lv.sizeStr, "total thin size", outputData)
        );
    }

    public static Map<String, Long> getThinFreeSize(ExtCmd extCmd, Set<String> volumeGroups) throws StorageException
    {
        final OutputData outputData = LvmCommands.getVgThinFreeSize(extCmd, volumeGroups);
        final String stdOut = new String(outputData.stdoutData);

        final HashMap<String, Long> ret = new HashMap<>();

        JsonVgs vgs;
        try
        {
            vgs = OBJECT_MAPPER.readValue(stdOut, JsonVgs.class);
        }
        catch (IOException exc)
        {
            throw new StorageException("Failed to parse lvs output", exc);
        }

        for (JsonVgs.Entry report : vgs.reportList)
        {
            for (JsonVgs.VolumeGroup vg : report.vgList)
            {
                BigDecimal thinPoolSizeBytes = StorageUtils.parseDecimal(vg.lvSizeStr.trim());

                BigDecimal dataPercent = StorageUtils.parseDecimal(vg.dataPercentStr.trim());
                BigDecimal dataFraction = dataPercent.movePointLeft(2);
                BigDecimal freeFraction = dataFraction.negate().add(BigDecimal.valueOf(1L));

                BigInteger freeBytes = thinPoolSizeBytes.multiply(freeFraction).toBigInteger();
                long freeSpace = SizeConv.convert(freeBytes, SizeUnit.UNIT_B, SizeUnit.UNIT_KiB).longValueExact();

                ret.put(vg.lvName, freeSpace);
            }
        }
        return ret;
    }

    public static void checkVgExists(ExtCmd extCmd, String volumeGroup) throws StorageException
    {
        final OutputData outputData = LvmCommands.listExistingVolumeGroups(extCmd);

        JsonVgs vgs;
        try
        {
            vgs = OBJECT_MAPPER.readValue(new String(outputData.stdoutData), JsonVgs.class);
        }
        catch (IOException exc)
        {
            throw new StorageException("Failed to parse lvs output", exc);
        }

        boolean found = false;
        for (JsonVgs.Entry report : vgs.reportList)
        {
            for (JsonVgs.VolumeGroup vg : report.vgList)
            {
                if (vg.name.equals(volumeGroup))
                {
                    found = true;
                    break;
                }
            }
        }
        if (!found)
        {
            throw new StorageException("Volume group '" + volumeGroup + "' not found");
        }
    }

    private static <T> Map<String, T> parseVgsData(
        OutputData outputData,
        ExceptionThrowingFunction<JsonVgs.VolumeGroup, T, StorageException> dataExtractor
    )
        throws StorageException
    {
        final HashMap<String, T> ret = new HashMap<>();

        JsonVgs vgs;
        try
        {
            vgs = OBJECT_MAPPER.readValue(new String(outputData.stdoutData), JsonVgs.class);
        }
        catch (IOException exc)
        {
            throw new StorageException("Failed to parse lvs output", exc);
        }

        for (JsonVgs.Entry report : vgs.reportList)
        {
            for (JsonVgs.VolumeGroup vg : report.vgList)
            {
                T data = dataExtractor.apply(vg);
                ret.put(
                    vg.name,
                    data
                );
            }
        }

        return ret;
    }

    private static <T> Map<String, T> parseLvsData(
        OutputData outputData,
        ExceptionThrowingFunction<JsonLvs.LogicalVolume, T, StorageException> dataExtractor
    )
        throws StorageException
    {
        final HashMap<String, T> ret = new HashMap<>();

        JsonLvs lvs;
        try
        {
            lvs = OBJECT_MAPPER.readValue(new String(outputData.stdoutData), JsonLvs.class);
        }
        catch (IOException exc)
        {
            throw new StorageException("Failed to parse lvs output", exc);
        }

        for (JsonLvs.Entry report : lvs.reportList)
        {
            for (JsonLvs.LogicalVolume lv : report.lvList)
            {
                T data = dataExtractor.apply(lv);
                ret.put(
                    lv.name,
                    data
                );
            }
        }

        return ret;
    }


    private static <T> ExceptionThrowingFunction<T, Long, StorageException> sizeExtractor(
        ExceptionThrowingFunction<T, String, StorageException> toString,
        String description,
        OutputData outputData
    )
    {
        return type -> asSize(toString.apply(type).trim(), description, outputData);
    }

    private static <T> Long asSize(
        String strRef,
        String description,
        OutputData outputData
    )
        throws StorageException
    {
        String str = strRef;
        if (str.endsWith("k"))
        {
            str = str.substring(0, str.length() - 1);
        }
        return parseLong(
            str,
            null,
            description,
            outputData
        );
    }
}
