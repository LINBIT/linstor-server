package com.linbit.linstor.layer.storage.lvm.utils;

import com.linbit.extproc.ExtCmd.OutputData;
import com.linbit.extproc.ExtCmdFactory;
import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.propscon.ReadOnlyProps;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.StorageUtils;
import com.linbit.utils.ExceptionThrowingFunction;
import com.linbit.utils.ExceptionThrowingSupplier;
import com.linbit.utils.StringUtils;
import com.linbit.utils.TimedCache;

import static com.linbit.linstor.layer.storage.lvm.utils.LvmCommands.LVS_COL_ATTRIBUTES;
import static com.linbit.linstor.layer.storage.lvm.utils.LvmCommands.LVS_COL_CHUNK_SIZE;
import static com.linbit.linstor.layer.storage.lvm.utils.LvmCommands.LVS_COL_DATA_PERCENT;
import static com.linbit.linstor.layer.storage.lvm.utils.LvmCommands.LVS_COL_IDENTIFIER;
import static com.linbit.linstor.layer.storage.lvm.utils.LvmCommands.LVS_COL_METADATA_PERCENT;
import static com.linbit.linstor.layer.storage.lvm.utils.LvmCommands.LVS_COL_PATH;
import static com.linbit.linstor.layer.storage.lvm.utils.LvmCommands.LVS_COL_POOL_LV;
import static com.linbit.linstor.layer.storage.lvm.utils.LvmCommands.LVS_COL_SIZE;
import static com.linbit.linstor.layer.storage.lvm.utils.LvmCommands.LVS_COL_STRIPES;
import static com.linbit.linstor.layer.storage.lvm.utils.LvmCommands.LVS_COL_VG;
import static com.linbit.linstor.layer.storage.lvm.utils.LvmCommands.VGS_COL_VG_NAME;

import java.io.File;
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

    private static final String LVM_CACHE_PROP_KEY = ApiConsts.NAMESPC_STORAGE_DRIVER + "/" +
        ApiConsts.KEY_STOR_POOL_LVM_SIZES_CACHE_TIME;
    private static final long DFLT_LVM_CACHE_TIME_IN_MS = 10_000L;
    /**
     * TimedCache's key: Collection of volume groups to "filter" for
     * <br/>
     * TimedCache's value's outer map's key: single volume-group name
     * <br/>
     * LvmCache's map's key: LV identifier
     */
    private static final TimedCache<Collection<String>, Map<String, Map<String, LvsInfo>>> CACHED_LVS;
    /**
     * TimedCache's key: Collection of volume groups to "filter" for
     * <br/>
     * TimedCache's value's map's key: single volume-group name
     */
    private static final TimedCache<Collection<String>, Map<String, VgsInfo>> CACHED_VGS_THIN;
    /**
     * TimedCache's key: Collection of volume groups to "filter" for
     * <br/>
     * TimedCache's value's map's key: single volume-group name
     */
    private static final TimedCache<Collection<String>, Map<String, VgsInfo>> CACHED_VGS_THICK;

    static
    {
        CACHED_LVS = new TimedCache<>(DFLT_LVM_CACHE_TIME_IN_MS);
        CACHED_VGS_THIN = new TimedCache<>(DFLT_LVM_CACHE_TIME_IN_MS);
        CACHED_VGS_THICK = new TimedCache<>(DFLT_LVM_CACHE_TIME_IN_MS);
    }

    private LvmUtils()
    {
    }

    public static class LvsInfo
    {
        public final String volumeGroup;
        public final @Nullable String thinPool;
        public final String identifier;
        public final String path;
        public final long size;
        public final float dataPercent;
        public final String attributes;
        public final String metaDataPercentStr;
        public final long chunkSizeInKib;
        public final int stripes;

        LvsInfo(
            String volumeGroupRef,
            @Nullable String thinPoolRef,
            String identifierRef,
            String pathRef,
            long sizeRef,
            float dataPercentRef,
            String attributesRef,
            String metaDataPercentStrRef,
            long chunkSizeInKibRef,
            int stripesRef
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
            stripes = stripesRef;
        }
    }

    public static class VgsInfo
    {
        public final String vgName;
        public final @Nullable Long vgExtentSize;
        public final @Nullable Long vgSize;
        public final @Nullable Long vgFree;
        public final @Nullable String lvName;
        public final @Nullable Long lvSize;
        public final @Nullable Float dataPercent;

        public VgsInfo(
            String vgNameRef,
            @Nullable Long vgExtentSizeRef,
            @Nullable Long vgSizeRef,
            @Nullable Long vgFreeRef,
            @Nullable String lvNameRef,
            @Nullable Long lvSizeRef,
            @Nullable Float dataPercentRef
        )
        {
            vgName = vgNameRef;
            vgExtentSize = vgExtentSizeRef;
            vgSize = vgSizeRef;
            vgFree = vgFreeRef;
            lvName = lvNameRef;
            lvSize = lvSizeRef;
            dataPercent = dataPercentRef;
        }
    }

    private static String getLvmConfig(ExtCmdFactory extCmdFactory, Set<String> volumeGroups)
        throws StorageException
    {
        @Nullable String lvmConfig = CACHED_LVM_CONFIG_STRING.get(volumeGroups);

        if (lvmConfig == null)
        {
            Set<String> vlmGrps = new HashSet<>();
            if (volumeGroups != null)
            {
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
        recacheNext();
        return getLvmConfig(extCmdFactory, volumeGroups);
    }

    private static <T> Map<String, T> retryIfNotAllContainingVgsExist(
        ExtCmdFactory extCmdFactory,
        Set<String> volumeGroups,
        ExceptionThrowingSupplier<Map<String, T>, StorageException> supplierRef
    )
        throws StorageException
    {
        @Nullable Map<String, T> ret = supplierRef.supply();
        boolean someMissing = ret == null;
        if (ret != null)
        {
            for (String vg : volumeGroups)
            {
                if (!ret.containsKey(vg))
                {
                    someMissing = true;
                    break;
                }
            }
        }
        if (someMissing)
        {
            recacheLvmConfig(extCmdFactory, volumeGroups);
            recacheNext();
            ret = supplierRef.supply();
        }
        return ret;
    }

    public static synchronized void updateCacheTime(ReadOnlyProps stltConfRef, ReadOnlyProps nodePropsRef)
    {
        PriorityProps prioProps = new PriorityProps(nodePropsRef, stltConfRef);
        @Nullable String prop = prioProps.getProp(LVM_CACHE_PROP_KEY);
        long cacheTime;
        if (prop == null)
        {
            cacheTime = DFLT_LVM_CACHE_TIME_IN_MS;
        }
        else
        {
            cacheTime = Long.parseLong(prop);
        }

        CACHED_LVS.setMaxCacheTime(cacheTime, true);
        CACHED_VGS_THIN.setMaxCacheTime(cacheTime, true);
        CACHED_VGS_THICK.setMaxCacheTime(cacheTime, true);
    }

    public static synchronized void recacheNext()
    {
        recacheNextLvs();
        recacheNextVgs();
    }

    public static synchronized void recacheNextLvs()
    {
        CACHED_LVS.clear();
        CACHED_VGS_THIN.clear();
    }

    public static synchronized void recacheNextVgs()
    {
        CACHED_VGS_THICK.clear();
        CACHED_VGS_THIN.clear();
    }

    public static synchronized Map<String /* vg */, Map<String/* lv */, LvsInfo>> getLvsInfo(
        final ExtCmdFactory ecf,
        final Set<String> volumeGroups
    )
        throws StorageException
    {
        final long now = System.currentTimeMillis();
        @Nullable Map<String /* vg */, Map<String/* lv */, LvsInfo>> ret = CACHED_LVS.get(volumeGroups, now);
        if (ret == null)
        {
            ret = getLvsInfoImpl(ecf, volumeGroups);
            CACHED_LVS.put(volumeGroups, ret, now);
        }
        return ret;
    }

    public static synchronized Map<String, VgsInfo> getVgsInfo(
        ExtCmdFactory extCmdFactoryRef,
        Set<String> volumeGroupSetRef,
        boolean thinRef
    )
        throws StorageException
    {
        final long now = System.currentTimeMillis();
        TimedCache<Collection<String>, Map<String, VgsInfo>> cache = thinRef ? CACHED_VGS_THIN : CACHED_VGS_THICK;

        @Nullable Map<String, VgsInfo> ret = cache.get(volumeGroupSetRef, now);
        if (ret == null)
        {
            ret = retryIfNotAllContainingVgsExist(
                extCmdFactoryRef,
                volumeGroupSetRef,
                () -> getVgsInfoImpl(extCmdFactoryRef, volumeGroupSetRef, thinRef)
            );
            cache.put(volumeGroupSetRef, ret, now);
        }
        return ret;
    }

    private static Map<String /* vg */, Map<String/* lv */, LvsInfo>> getLvsInfoImpl(
        final ExtCmdFactory ecf,
        final Set<String> volumeGroups
    )
        throws StorageException
    {
        final Map<String /* vg */, Map<String/* lv */, LvsInfo>> ret = new HashMap<>();
        final OutputData output = execWithRetry(
            ecf,
            volumeGroups,
            config -> LvmCommands.lvs(ecf.create(), volumeGroups, config)
        );
        final String stdOut = new String(output.stdoutData);


        final String[] lines = stdOut.split("\n");
        final int expectedColCount = LvmCommands.LVS_COLUMN_COUNT;
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

                String stripesStr = data[LVS_COL_STRIPES];
                int stripes;
                try
                {
                    stripes = StorageUtils.parseDecimalAsInt(stripesStr);
                }
                catch (NumberFormatException nfExc)
                {
                    throw new StorageException(
                        "Unable to parse stripes",
                        "Number to parse: '" + stripesStr + "'",
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
                    chunkSizeInKib,
                    stripes
                );
                ret.computeIfAbsent(vgStr, ignored -> new HashMap<>())
                    .put(identifier, state);
            }
        }
        return ret;
    }

    private static HashMap<String, VgsInfo> getVgsInfoImpl(
        final ExtCmdFactory ecf,
        final Set<String> volumeGroups,
        final boolean thinRef
    )
        throws StorageException
    {
        final HashMap<String, VgsInfo> infoByVgName = new HashMap<>();
        final OutputData output = execWithRetry(
            ecf,
            volumeGroups,
            config -> thinRef ?
                LvmCommands.vgsThin(ecf.create(), volumeGroups, config) :
                LvmCommands.vgsThick(ecf.create(), volumeGroups, config)
        );
        final String stdOut = new String(output.stdoutData);

        final String[] lines = stdOut.split("\n");
        final int expectedColCount = thinRef ? LvmCommands.VGS_THIN_COLUMN_COUNT : LvmCommands.VGS_THICK_COLUMN_COUNT;
        for (final String line : lines)
        {
            final String[] data = line.trim().split(DELIMITER);
            if (data.length == expectedColCount)
            {
                final String vgName = data[VGS_COL_VG_NAME];
                final String vgExtentSizeStr = data[LvmCommands.VGS_COL_VG_EXTENT_SIZE];
                final String vgSizeStr = data[LvmCommands.VGS_COL_VG_SIZE];
                final String vgFreeStr = data[LvmCommands.VGS_COL_VG_FREE];

                final long vgExentSize;
                final long vgSize;
                final long vgFree;
                final @Nullable String lvName;
                final @Nullable Long lvSize;
                final @Nullable Float dataPercent;

                if (thinRef)
                {
                    lvName = data[LvmCommands.VGS_COL_LV_NAME];
                    final String lvSizeStr = data[LvmCommands.VGS_COL_LV_SIZE];
                    final String dataPercentStr = data[LvmCommands.VGS_COL_DATA_PERCENT];

                    lvSize = parseLong(lvSizeStr, "lv_size", vgName, output.executedCommand, null, true);
                    dataPercent = parseFloat(
                        dataPercentStr,
                        "data_percent",
                        vgName,
                        output.executedCommand,
                        LVM_DEFAULT_DATA_PERCENT,
                        true
                    );
                }
                else
                {
                    lvName = null;
                    lvSize = null;
                    dataPercent = null;
                }


                vgExentSize = parseLong(vgExtentSizeStr, "vg_extent", vgName, output.executedCommand, null, true);
                vgSize = parseLong(vgSizeStr, "vg_size", vgName, output.executedCommand, null, true);
                vgFree = parseLong(vgFreeStr, "vg_free", vgName, output.executedCommand, null, true);

                final VgsInfo state = new VgsInfo(
                    vgName,
                    vgExentSize,
                    vgSize,
                    vgFree,
                    lvName,
                    lvSize,
                    dataPercent
                );
                if (lvName != null)
                {
                    infoByVgName.put(vgName + File.separator + lvName, state);
                }
                infoByVgName.put(vgName, state);
            }
        }
        return infoByVgName;
    }

    public static Map<String /* vg */, Map<String/* thinPool */, Long>> getThinFreeSize(
        ExtCmdFactory extCmdFactory,
        Set<String> volumeGroups
    )
        throws StorageException
    {
        return retryIfNotAllContainingVgsExist(
            extCmdFactory,
            volumeGroups,
            () -> getThinFreeSizeImpl(extCmdFactory, volumeGroups)
        );
    }

    private static Map<String /* vg */, Map<String/* thinPool */, Long>> getThinFreeSizeImpl(
        ExtCmdFactory extCmdFactory,
        Set<String> volumeGroups
    )
        throws StorageException
    {
        final Map<String /* vg */, Map<String/* thinPool */, Long>> result = new HashMap<>();
        final Map<String, VgsInfo> vgsInfoMap = getVgsInfo(
            extCmdFactory,
            volumeGroups,
            true
        );
        for (VgsInfo vgsInfo : vgsInfoMap.values())
        {
            if (vgsInfo.lvName != null)
            {
                long freeSpace = (long) (vgsInfo.lvSize * (1 - vgsInfo.dataPercent / 100.0));
                result.computeIfAbsent(vgsInfo.vgName, ignored -> new HashMap<>())
                    .put(vgsInfo.lvName, freeSpace);
            }
        }
        return result;

    }

    public static void checkVgExists(ExtCmdFactory extCmdFactory, String volumeGroup) throws StorageException
    {
        if (!checkVgExistsBool(extCmdFactory, volumeGroup))
        {
            throw new StorageException("Volume group '" + volumeGroup + "' not found");
        }
    }

    public static boolean checkVgExistsBool(
        final ExtCmdFactory extCmdFactory,
        final String volumeGroup
    )
        throws StorageException
    {
        boolean exists = checkVgExistsBoolImpl(extCmdFactory, volumeGroup);
        if (!exists)
        {
            recacheLvmConfig(extCmdFactory, volumeGroup);
            recacheNext();
            exists = checkVgExistsBoolImpl(extCmdFactory, volumeGroup);
        }
        return exists;
    }

    private static boolean checkVgExistsBoolImpl(final ExtCmdFactory extCmdFactory, final String volumeGroup)
        throws StorageException
    {
        Map<String, VgsInfo> vgsInfo = getVgsInfo(
            extCmdFactory,
            Collections.emptySet(),
            false
        );
        return vgsInfo.containsKey(volumeGroup);
    }

    public static boolean checkThinPoolExistsBool(
        ExtCmdFactory extCmdFactory,
        String volumeGroup,
        String thinPool
    )
        throws StorageException
    {
        boolean exists = checkThinPoolExistsBoolImpl(extCmdFactory, volumeGroup, thinPool);
        if (!exists)
        {
            recacheLvmConfig(extCmdFactory, volumeGroup);
            recacheNext();
            exists = checkThinPoolExistsBoolImpl(extCmdFactory, volumeGroup, thinPool);
        }
        return exists;
    }

    private static boolean checkThinPoolExistsBoolImpl(ExtCmdFactory extCmdFactory, String volumeGroup, String thinPool)
        throws StorageException
    {
        boolean ret = false;
        Map<String /* vg */, Map<String/* lv */, LvsInfo>> lvsInfo = getLvsInfo(
            extCmdFactory,
            Collections.singleton(volumeGroup)
        );
        @Nullable Map<String/* lv */, LvsInfo> lvInfoByVolumeGroup = lvsInfo.get(volumeGroup);
        if (lvInfoByVolumeGroup != null)
        {
            ret = lvInfoByVolumeGroup.containsKey(thinPool);
        }
        return ret;
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

    private static float parseFloat(
        @Nullable String numToParseRef,
        String descriptionOfNumberRef,
        String vgNameRef,
        String[] executedCommandRef,
        @Nullable Float defaultValueIfNullOrEmptyRef,
        boolean allowNullRef
    )
        throws StorageException
    {
        return parseGeneric(
            numToParseRef,
            descriptionOfNumberRef,
            vgNameRef,
            executedCommandRef,
            defaultValueIfNullOrEmptyRef,
            allowNullRef,
            StorageUtils::parseDecimalAsFloat
        );
    }

    private static long parseLong(
        @Nullable String numToParseRef,
        String descriptionOfNumberRef,
        String vgNameRef,
        String[] executedCommandRef,
        @Nullable Long defaultValueIfNullOrEmptyRef,
        boolean allowNullRef
    )
        throws StorageException
    {
        return parseGeneric(
            numToParseRef,
            descriptionOfNumberRef,
            vgNameRef,
            executedCommandRef,
            defaultValueIfNullOrEmptyRef,
            allowNullRef,
            StorageUtils::parseDecimalAsLong
        );
    }

    private static <T> @Nullable T parseGeneric(
        @Nullable String numToParseRef,
        String descriptionOfNumberRef,
        String vgNameRef,
        String[] executedCommandRef,
        @Nullable T defaultValueIfNullOrEmptyRef,
        boolean allowNullRef,
        ExceptionThrowingFunction<String, T, StorageException> parserRef
    )
        throws StorageException
    {
        T ret;
        if (numToParseRef == null || numToParseRef.trim().isEmpty())
        {
            if (defaultValueIfNullOrEmptyRef != null || allowNullRef)
            {
                ret = defaultValueIfNullOrEmptyRef;
            }
            else
            {
                throw new StorageException(
                    "Unable to parse '" + descriptionOfNumberRef + "' of vgs for volume group '" + vgNameRef + "'",
                    "Column '" + descriptionOfNumberRef + "' was unexpectedly null",
                    null,
                    null,
                    "External command used to query logical volume info: " +
                        StringUtils.joinShellQuote(executedCommandRef),
                    null
                );
            }
        }
        else
        {
            try
            {
                ret = parserRef.accept(numToParseRef.trim());
            }
            catch (NumberFormatException nfExc)
            {
                throw new StorageException(
                    "Unable to parse '" + descriptionOfNumberRef + "' of vgs for volume group '" + vgNameRef + "'",
                    "Data percent to parse: '" + numToParseRef + "'",
                    null,
                    null,
                    "External command used to query logical volume info: " +
                        StringUtils.joinShellQuote(executedCommandRef),
                    nfExc
                );
            }
        }
        return ret;
    }
}
