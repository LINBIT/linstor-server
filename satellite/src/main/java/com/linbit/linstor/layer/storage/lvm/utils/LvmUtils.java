package com.linbit.linstor.layer.storage.lvm.utils;

import com.linbit.SizeConv;
import com.linbit.SizeConv.SizeUnit;
import com.linbit.extproc.ExtCmd.OutputData;
import com.linbit.extproc.ExtCmdFactory;
import com.linbit.linstor.layer.storage.utils.ParseUtils;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.StorageUtils;
import com.linbit.utils.ExceptionThrowingFunction;
import com.linbit.utils.StringUtils;

import static com.linbit.linstor.layer.storage.lvm.utils.LvmCommands.LVS_COL_ATTRIBUTES;
import static com.linbit.linstor.layer.storage.lvm.utils.LvmCommands.LVS_COL_CHUNK_SIZE;
import static com.linbit.linstor.layer.storage.lvm.utils.LvmCommands.LVS_COL_DATA_PERCENT;
import static com.linbit.linstor.layer.storage.lvm.utils.LvmCommands.LVS_COL_IDENTIFIER;
import static com.linbit.linstor.layer.storage.lvm.utils.LvmCommands.LVS_COL_METADATA_PERCENT;
import static com.linbit.linstor.layer.storage.lvm.utils.LvmCommands.LVS_COL_PATH;
import static com.linbit.linstor.layer.storage.lvm.utils.LvmCommands.LVS_COL_POOL_LV;
import static com.linbit.linstor.layer.storage.lvm.utils.LvmCommands.LVS_COL_SIZE;
import static com.linbit.linstor.layer.storage.lvm.utils.LvmCommands.LVS_COL_VG;

import java.io.File;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author Gabor Hernadi &lt;gabor.hernadi@linbit.com&gt;
 */
public class LvmUtils
{
    // DO NOT USE "," or "." AS DELIMITER due to localization issues
    public static final String DELIMITER = ";";
    private static final float LVM_DEFAULT_DATA_PERCENT = 100;

    private static final HashMap<Set<String>, String> CACHED_LVM_CONFIG_STRING = new HashMap<>();

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
        public final String metaDataPercentStr;
        public final long chunkSizeInKib;

        LvsInfo(
            String volumeGroupRef,
            String thinPoolRef,
            String identifierRef,
            String pathRef,
            long sizeRef,
            float dataPercentRef,
            String attributesRef,
            String metaDataPercentStrRef,
            long chunkSizeInKibRef
        )
        {
            volumeGroup = volumeGroupRef;
            thinPool = thinPoolRef;
            identifier = identifierRef;
            path = pathRef;
            size = sizeRef;
            dataPercent = dataPercentRef;
            attributes = attributesRef;
            metaDataPercentStr = metaDataPercentStrRef;
            chunkSizeInKib = chunkSizeInKibRef;
        }
    }

    private static String getLvmConfig(ExtCmdFactory extCmdFactory, Set<String> volumeGroups)
        throws StorageException
    {
        String lvmConfig = CACHED_LVM_CONFIG_STRING.get(volumeGroups);

        if (lvmConfig == null)
        {
            Set<String> vlmGrps = new HashSet<>();
            for (String vlmGrp : volumeGroups)
            {
                int thinPoolIdx = vlmGrp.indexOf("/");
                if (thinPoolIdx != -1)
                {
                    // thin vlmGrp, we only need the first part, the "actual" volume group, not the thin pool
                    vlmGrps.add(vlmGrp.substring(0, thinPoolIdx));
                }
                else
                {
                    vlmGrps.add(vlmGrp);
                }
            }

            lvmConfig = CACHED_LVM_CONFIG_STRING.get(vlmGrps);
            if (lvmConfig == null)
            {
                HashSet<String> pvSet = new HashSet<>();
                for (String vg : vlmGrps)
                {
                    pvSet.addAll(getPhysicalVolumes(extCmdFactory, vg));
                }

                if (pvSet.isEmpty())
                {
                    lvmConfig = "";
                }
                else
                {
                    lvmConfig = getLvmFilterByPhysicalVolumes(pvSet);
                }

                CACHED_LVM_CONFIG_STRING.put(vlmGrps, lvmConfig);
                CACHED_LVM_CONFIG_STRING.put(volumeGroups, lvmConfig);
            }
            else
            {
                CACHED_LVM_CONFIG_STRING.put(volumeGroups, lvmConfig);
            }
        }
        return lvmConfig;
    }

    public static String getLvmFilterByPhysicalVolumes(String devicePathRef)
    {
        return getLvmFilterByPhysicalVolumes(Collections.singleton(devicePathRef));
    }

    public static String getLvmFilterByPhysicalVolumes(Collection<String> pvSet)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("devices { filter=[");
        for (String pv : pvSet)
        {
            sb.append("'a|").append(pv).append("|',");
        }
        /*
         * Although we do not add a "trailing accept everything else" (see LvmCommands#LVM_CONF_IGNORE_DRBD_DEVICES),
         * here we DO want to have a "trailing reject everything else", since we only want to include our whitelisted
         * paths
         */
        sb.append("'r|.*|'] }");
        return sb.toString();
    }

    private static String recacheLvmConfig(ExtCmdFactory extCmdFactory, String volumeGroup)
        throws StorageException
    {
        return recacheLvmConfig(extCmdFactory, Collections.singleton(volumeGroup));
    }

    private static String recacheLvmConfig(ExtCmdFactory extCmdFactory, Set<String> volumeGroups)
        throws StorageException
    {
        CACHED_LVM_CONFIG_STRING.remove(volumeGroups);
        return getLvmConfig(extCmdFactory, volumeGroups);
    }

    public static HashMap<String, LvsInfo> getLvsInfo(
        final ExtCmdFactory ecf,
        final Set<String> volumeGroups
        )
            throws StorageException
    {
        final OutputData output = execWithRetry(
            ecf,
            volumeGroups,
            config -> LvmCommands.lvs(ecf.create(), volumeGroups, config)
        );
        final String stdOut = new String(output.stdoutData);

        final HashMap<String, LvsInfo> infoByIdentifier = new HashMap<>();

        final String[] lines = stdOut.split("\n");
        final int expectedColCount = 9;
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
                if (
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
                                StringUtils.joinShellQuote(output.executedCommand),
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
                            StringUtils.joinShellQuote(output.executedCommand),
                            nfExc
                        );
                }

                String metaDataPercentStr = data[LVS_COL_METADATA_PERCENT];

                String chunkSizeInKiBStr = data[LVS_COL_CHUNK_SIZE];
                long chunkSizeInKib;
                try
                {
                    chunkSizeInKib = StorageUtils.parseDecimalAsLong(chunkSizeInKiBStr);
                }
                catch (NumberFormatException nfExc)
                {
                    throw new StorageException(
                        "Unable to parse logical chunk size",
                        "Size to parse: '" + chunkSizeInKiBStr + "'",
                        null,
                        null,
                        "External command used to query logical volume info: " +
                            StringUtils.joinShellQuote(output.executedCommand),
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
                    attributes,
                    metaDataPercentStr,
                    chunkSizeInKib
                );
                infoByIdentifier.put(vgStr + File.separator + identifier, state);
            }
        }
        return infoByIdentifier;
    }

    public static Map<String, Long> getExtentSize(ExtCmdFactory extCmdFactory, Set<String> volumeGroups)
        throws StorageException
    {
        return ParseUtils.parseSimpleTable(
            execWithRetry(
                extCmdFactory,
                volumeGroups,
                config -> LvmCommands.getExtentSize(extCmdFactory.create(), volumeGroups, config)
            ),
            DELIMITER,
            "extent size"
        );
    }

    public static Map<String, Long> getVgTotalSize(ExtCmdFactory extCmdFactory, Set<String> volumeGroups)
        throws StorageException
    {
        return ParseUtils.parseSimpleTable(
            execWithRetry(
                extCmdFactory,
                volumeGroups,
                config -> LvmCommands.getVgTotalSize(extCmdFactory.create(), volumeGroups, config)
            ),
            DELIMITER,
            "total size"
        );
    }

    public static Map<String, Long> getVgFreeSize(ExtCmdFactory extCmdFactory, Set<String> volumeGroups)
        throws StorageException
    {
        return ParseUtils.parseSimpleTable(
            execWithRetry(
                extCmdFactory,
                volumeGroups,
                config -> LvmCommands.getVgFreeSize(extCmdFactory.create(), volumeGroups, config)
            ),
            DELIMITER,
            "free size"
        );
    }

    public static Map<String, Long> getThinTotalSize(ExtCmdFactory extCmdFactory, Set<String> volumeGroups)
        throws StorageException
    {
        return ParseUtils.parseSimpleTable(
            execWithRetry(
                extCmdFactory,
                volumeGroups,
                config -> LvmCommands.getVgThinTotalSize(extCmdFactory.create(), volumeGroups, config)
            ),
            DELIMITER,
            "total thin size"
        );
    }

    public static Map<String, Long> getThinFreeSize(ExtCmdFactory extCmdFactory, Set<String> volumeGroups)
        throws StorageException
    {
        final int expectedColums = 3;

        final Map<String, Long> result = new HashMap<>();

        OutputData output = execWithRetry(
            extCmdFactory,
            volumeGroups,
            config -> LvmCommands.getVgThinFreeSize(extCmdFactory.create(), volumeGroups, config)
        );
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
                        "External command: " + StringUtils.joinShellQuote(output.executedCommand),
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
                    "External command: " + StringUtils.joinShellQuote(output.executedCommand)
                    );
            }
        }
        return result;
    }

    public static boolean checkVgExistsBool(
        final ExtCmdFactory extCmdFactory,
        final String volumeGroup
    )
        throws StorageException
    {
        OutputData output = execWithRetry(
            extCmdFactory,
            Collections.singleton(volumeGroup),
            config -> LvmCommands.listExistingVolumeGroups(extCmdFactory.create(), config)
        );
        final String stdOut = new String(output.stdoutData);
        final String[] volumeGroupList = stdOut.split("\n");
        final String specVlmGrp = volumeGroup.trim();
        boolean matchFlag = false;
        for (String curVlmGrp : volumeGroupList)
        {
            if (curVlmGrp.trim().equals(specVlmGrp))
            {
                matchFlag = true;
                break;
            }
        }
        return matchFlag;
    }

    public static void checkVgExists(ExtCmdFactory extCmdFactory, String volumeGroup) throws StorageException
    {
        if (!checkVgExistsBool(extCmdFactory, volumeGroup))
        {
            throw new StorageException("Volume group '" + volumeGroup + "' not found");
        }
    }

    public static boolean checkThinPoolExistsBool(
        ExtCmdFactory extCmdFactory,
        String volumeGroup,
        String thinPool
    )
        throws StorageException
    {
        HashMap<String, LvsInfo> map = getLvsInfo(extCmdFactory, Collections.singleton(volumeGroup));
        LvsInfo lvsInfo = map.get(volumeGroup + File.separator + thinPool);
        return lvsInfo != null;
    }

    public static void checkThinPoolExists(
        ExtCmdFactory extCmdFactory,
        String volumeGroup,
        String thinPool
    )
        throws StorageException
    {
        if (!checkThinPoolExistsBool(extCmdFactory, volumeGroup, thinPool))
        {
            throw new StorageException("Thin pool '" + volumeGroup + "/" + thinPool + "' not found");
        }
    }

    public static List<String> getPhysicalVolumes(ExtCmdFactory extCmdFactory, String volumeGroup)
        throws StorageException
    {
        // no lvm config here. this method is used to build the --config param. using execWithRetry here would cause an
        // endless-recursion!

        final OutputData output = LvmCommands.listPhysicalVolumes(extCmdFactory.create(), volumeGroup, "");
        final String stdOut = new String(output.stdoutData);
        final List<String> pvs = new ArrayList<>();
        final String[] lines = stdOut.split("\n");
        for (String line : lines)
        {
            String trimmedLine = line.trim();
            if (!trimmedLine.isEmpty())
            {
                pvs.add(line.trim());
            }
        }
        return pvs;
    }

    public static OutputData execWithRetry(
        ExtCmdFactory ecf,
        Set<String> volumeGroups,
        ExceptionThrowingFunction<String, OutputData, StorageException> fkt
    ) throws StorageException
    {
        OutputData outputData;
        try
        {
            outputData = fkt.accept(getLvmConfig(ecf, volumeGroups));
        }
        catch  (StorageException storExc)
        {
            outputData = fkt.accept(recacheLvmConfig(ecf, volumeGroups));
        }
        return outputData;
    }
}
