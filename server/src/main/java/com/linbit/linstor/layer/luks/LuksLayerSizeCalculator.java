package com.linbit.linstor.layer.luks;

import com.linbit.SizeConv;
import com.linbit.SizeConv.SizeUnit;
import com.linbit.exceptions.InvalidSizeException;
import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.objects.AbsResource;
import com.linbit.linstor.core.objects.AbsVolume;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.ResourceGroup;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.SnapshotVolume;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.layer.AbsLayerSizeCalculator;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.ReadOnlyProps;
import com.linbit.linstor.storage.StorageConstants;
import com.linbit.linstor.storage.data.adapter.luks.LuksVlmData;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.ExtTools;
import com.linbit.linstor.storage.kinds.ExtToolsInfo;
import com.linbit.linstor.storage.kinds.ExtToolsInfo.Version;
import com.linbit.linstor.utils.layer.LayerVlmUtils;
import com.linbit.utils.ShellUtils;
import com.linbit.utils.SignedAlign;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Singleton
public class LuksLayerSizeCalculator extends AbsLayerSizeCalculator<LuksVlmData<?>>
{
    public static final String LUKS2_OPT_METADATA_SIZE = "--luks2-metadata-size";
    public static final String LUKS2_OPT_KEYSLOTS_SIZE = "--luks2-keyslots-size";
    public static final String LUKS2_OPT_OFFSET = "--offset";
    private static final String LUKS2_OPT_ALIGN_PAYLOAD = "--align-payload";
    private static final long DFLT_LUKS2_METADATA_SIZE_IN_BYTES = 16L << 10;
    private static final long DFLT_ALIGNMENT_1MIB_IN_BYTES = 1L << 20;
    // linstor calculates in KiB
    private static final long LUKS1_HEADER_SIZE_IN_KIB = 2L << 10;
    private static final long LUKS2_HEADER_SIZE_IN_KIB = 16L << 10;
    private static final long LUKS2_HEADER_SIZE_IN_BYTES = LUKS2_HEADER_SIZE_IN_KIB << 10;

    private static final Pattern PATTERN_SIZE = Pattern.compile("(\\d+)([kKmMgGtT]i?[bB]?|[sS]|)");

    @Inject
    public LuksLayerSizeCalculator(AbsLayerSizeCalculatorInit initRef)
    {
        super(initRef, DeviceLayerKind.LUKS);
    }

    @Override
    protected void updateAllocatedSizeFromUsableSizeImpl(LuksVlmData<?> luksDataRef)
        throws DatabaseException, InvalidSizeException
    {
        long luksHeaderSize = getLuksHeaderSize(luksDataRef);
        long grossSize = luksDataRef.getUsableSize() + luksHeaderSize;
        luksDataRef.setAllocatedSize(grossSize);

        VlmProviderObject<?> childVlmData = luksDataRef.getSingleChild();
        childVlmData.setUsableSize(grossSize);
        updateAllocatedSizeFromUsableSize(childVlmData);

        /*
         * Layers below us will update our dataChild's usable size.
         * We need to take that updated size for further calculations.
         */
        long usableSizeChild = childVlmData.getUsableSize();
        luksDataRef.setAllocatedSize(usableSizeChild);
        luksDataRef.setUsableSize(usableSizeChild - luksHeaderSize);
    }

    @Override
    protected void updateUsableSizeFromAllocatedSizeImpl(LuksVlmData<?> luksDataRef)
        throws DatabaseException, InvalidSizeException
    {
        long luksHeaderSize = getLuksHeaderSize(luksDataRef);
        long grossSize = luksDataRef.getAllocatedSize();
        long netSize = grossSize - luksHeaderSize;

        luksDataRef.setUsableSize(netSize);

        VlmProviderObject<?> childVlmData = luksDataRef.getSingleChild();
        childVlmData.setAllocatedSize(grossSize);
        updateUsableSizeFromAllocatedSize(childVlmData);

        /*
         * Layers below us will update our dataChild's usable size.
         * We need to take that updated size for further calculations.
         */
        long usableSizeChild = childVlmData.getUsableSize();
        luksDataRef.setAllocatedSize(usableSizeChild);
        luksDataRef.setUsableSize(usableSizeChild - luksHeaderSize);
    }

    private long getLuksHeaderSize(VlmProviderObject<?> vlmDataRef)
        throws InvalidSizeException
    {
        Peer peer = vlmDataRef.getRscLayerObject()
            .getAbsResource()
            .getNode()
            .getPeer();
        ExtToolsInfo cryptSetupInfo = peer.getExtToolsManager()
            .getExtToolInfo(ExtTools.CRYPT_SETUP);

        final long luksHeaderSize;
        if (cryptSetupInfo != null && cryptSetupInfo.isSupported())
        {
            if (cryptSetupInfo.hasVersionOrHigher(new Version(2, 1)))
            {
                luksHeaderSize = calcLuks2HeaderSize(vlmDataRef);
            }
            else
            {
                luksHeaderSize = LUKS1_HEADER_SIZE_IN_KIB;
            }
        }
        else
        {
            throw new InvalidSizeException(
                "Could not calculate size of LUKS volume, since cryptsetup's version could not be determined",
                null
            );
        }
        return luksHeaderSize;
    }

    /**
     * <p>From LUKS2 sources (luks2_json_metadata.c, luks2_json_format.c) and documentation
     * (https://gitlab.com/cryptsetup/LUKS2-docs,
     * https://cdn.kernel.org/pub/linux/utils/cryptsetup/v2.1/v2.1.0-ReleaseNotes):</p>
     * <p><b>LUKS2 header structure</b>:
     * <pre>
     * [metadata area 1] [metadata area 2] [keyslots area] [alignment padding] [data]
     * </pre>
     * Where each "metadata area" = binary header + JSON area (controlled as one unit).</p>
     * <p><b>Key parameters</b>:
     * <ul>
     *  <li><code>"--offset &lt;sectors>"</code>: Directly sets data offset (= total header size) in 512-byte sectors
     *  </li>
     *  <li><code>"--luks2-metadata-size &lt;size></code>": Size of ONE metadata area (default: 16K, max: 4M)</li>
     *  <li><code>"--luks2-keyslots-size &lt;size>"</code>: Size of keyslots area (default: fills space to offset)</li>
     * </ul>
     * Default total header size: 16 MiB (LUKS2_DEFAULT_HDR_SIZE)<br/>
     * Default alignment: 1 MiB, or device's optimal_io_size if larger</p>
     * <p><b>LINSTOR header size calculation</b>:
     * <ol>
     *  <li>If <code>--offset</code> is given:<br/>
     *   <code>headerSize = offset * 512</code>
     *  </li>
     *  <li>Else if <code>--luks2-keyslots-size</code> is given:<br/>
     *   <code>headerSize = 2 * metadataSize + keyslotsSize</code><br/>
     *   // metadataSize from --luks2-metadata-size, default 16K
     *  </li>
     *  <li>Else:<br/>
     *   <code>headerSize = 16 MiB (default)</code>
     *  </li>
     *  <li>Round up headerSize to alignment (1 MiB or device's optimal_io_size)</li>
     * </ol>
     */
    private long calcLuks2HeaderSize(VlmProviderObject<?> vlmDataRef)
    {
        PriorityProps prioProps = getPrioProps(vlmDataRef);

        final @Nullable String userOptProp = prioProps.getProp(
            ApiConsts.KEY_STOR_DRIVER_LUKS_FORMAT_OPTIONS,
            ApiConsts.NAMESPC_STORAGE_DRIVER
        );
        List<String> userOptions = userOptProp != null ?
            ShellUtils.shellSplit(userOptProp) :
            new ArrayList<>();

        final long alignedHeaderSizeInBytes;
        final long unalignedHeaderSizeInBytes;
        @Nullable Long cryptsetupOffsetInBytes = getLongOptionValue(
            userOptions,
            LUKS2_OPT_OFFSET,
            SizeUnit.UNIT_SECTORS
        );
        if (cryptsetupOffsetInBytes != null)
        {
            // --offset overrules everything. --luks2-keyslots-size as well as underlying alignments.
            alignedHeaderSizeInBytes = cryptsetupOffsetInBytes;
        }
        else
        {
            // we can ignore --luks2-metadata-size since it can only be max 4M. 2*4M is still just 8M, so that
            // would still result in default 16M header size
            @Nullable Long cryptsetupLuks2KeyslotsSize = getLongOptionValue(
                userOptions,
                LUKS2_OPT_KEYSLOTS_SIZE
            );
            if (cryptsetupLuks2KeyslotsSize == null)
            {
                unalignedHeaderSizeInBytes = LUKS2_HEADER_SIZE_IN_BYTES;
            }
            else
            {
                @Nullable Long cryptsetupLuks2MetadataSize = getLongOptionValue(
                    userOptions,
                    LUKS2_OPT_METADATA_SIZE
                );
                cryptsetupLuks2MetadataSize = cryptsetupLuks2MetadataSize == null ?
                    DFLT_LUKS2_METADATA_SIZE_IN_BYTES :
                    cryptsetupLuks2MetadataSize;

                unalignedHeaderSizeInBytes = 2 * cryptsetupLuks2MetadataSize + cryptsetupLuks2KeyslotsSize;
            }

            long alignment = getAlignment(vlmDataRef, userOptions);

            alignedHeaderSizeInBytes = new SignedAlign(alignment)
                .ceiling(unalignedHeaderSizeInBytes);
        }
        return SizeConv.convert(
            alignedHeaderSizeInBytes,
            SizeUnit.UNIT_B,
            SizeUnit.UNIT_KiB
        );
    }

    /**
     * <p>This method takes the underlying device's <code>/sys/block/&lt;dev>/queue/optimal_io_size</code> as well as
     * the cryptsetup --align-payload argument into account.</p>
     * <p>Currently this method does NOT take <code>/sys/block/&lt;dev>/alignment_offset</code> into account.</p>
     *
     */
    private long getAlignment(VlmProviderObject<?> vlmDataRef, List<String> userOptions)
    {
        @Nullable Long cryptsetupAlignPayloadInBytes = getLongOptionValue(
            userOptions,
            LUKS2_OPT_ALIGN_PAYLOAD,
            SizeUnit.UNIT_SECTORS
        );

        long alignment = DFLT_ALIGNMENT_1MIB_IN_BYTES;
        if (cryptsetupAlignPayloadInBytes != null)
        {
            // if --align-payload is given, optimal_io_size is completely ignored.
            alignment = cryptsetupAlignPayloadInBytes;
        }
        else
        {
            final long maxOptIoSize = getMaxOptIoSize(vlmDataRef);
            alignment = Math.max(alignment, maxOptIoSize);
        }
        return alignment;
    }

    private long getMaxOptIoSize(VlmProviderObject<?> vlmDataRef) throws InvalidKeyException
    {
        long ret = 0;
        Set<StorPool> storPoolSet = LayerVlmUtils.getStorPoolSet(vlmDataRef);
        for (StorPool sp : storPoolSet)
        {
            @Nullable String strValue = sp.getProps()
                .getProp(
                    StorageConstants.BLK_DEV_OPT_IO_SIZE,
                    StorageConstants.NAMESPACE_INTERNAL
                );
            if (strValue != null)
            {
                try
                {
                    long parsed = Long.parseLong(strValue);
                    ret = Math.max(parsed, ret);
                }
                catch (NumberFormatException ignored)
                {
                    errorReporter.logWarning(
                        "LuksHeaderSize: Failed to parse '%s' from prop %s. Defaulting to 0 opt_io_size " +
                            "(no recommendation/hint)",
                        strValue,
                        StorageConstants.NAMESPACE_INTERNAL + "/" + StorageConstants.BLK_DEV_OPT_IO_SIZE
                    );
                }
            }
        }
        return ret;
    }

    /**
     * Calls {@link #getLongOptionValue(List, String, SizeUnit)} with SizeUnit_UNIT_B as default parameter.
     */
    private @Nullable Long getLongOptionValue(List<String> userOptionsRef, String optRef)
    {
        return getLongOptionValue(userOptionsRef, optRef, SizeUnit.UNIT_B);
    }

    /**
     * <p>Returns the value of the LAST <code>"--&lt;key> &lt;value>"</code> or <code>"--&lt;key>=&lt;value>"</code>
     * option pair. If the <code>&lt;value></code> contains a size unit (s, k, kb, kib, m, mb, mib, g ...) the size
     * will converted to bytes.</p>
     * <p>If the <code>&lt;key></code> was either not found or had no following <code>&lt;value></code>,
     * <code>null</code> is returned.
     */
    @SuppressWarnings("checkstyle:magicnumber")
    private @Nullable Long getLongOptionValue(List<String> userOptionsRef, String optRef, SizeUnit dfltSizeUnit)
    {
        @Nullable Long ret = null;
        @Nullable String val = findLastValue(userOptionsRef, optRef);
        if (val != null && !val.isBlank())
        {
            // val might either just be a number (i.e. in case of --offset or if the user specified the value in
            // bytes. It could however also have the format "128M"

            // copied from cryptsetup's source: src/utils_tools.c:
            /*
             * Device size string parsing, suffixes:
             * s|S - 512 bytes sectors
             * k _|K _|m _|M _|g _|G _|t _|T __- 1024 base
             * kiB|KiB|miB|MiB|giB|GiB|tiB|TiB - 1024 base
             * kb |KB |mM |MB |gB |GB |tB |TB_ - 1000 base
             *
             * _________^ typo comes from cryptsetup source :)
             */
            Matcher matcher = PATTERN_SIZE.matcher(val);
            if (matcher.matches())
            {
                try
                {
                    ret = Long.parseLong(matcher.group(1));
                    String unit = matcher.group(2);
                    SizeUnit sizeUnit;
                    if (unit.isBlank())
                    {
                        sizeUnit = dfltSizeUnit;
                    }
                    else
                    {
                        boolean forcePowerOfTwo = unit.length() == 1 || unit.length() == 3;
                        sizeUnit = SizeUnit.parse(unit, forcePowerOfTwo);
                    }

                    ret = SizeConv.convert(ret, sizeUnit, SizeUnit.UNIT_B);
                }
                catch (NumberFormatException ignored)
                {
                    errorReporter.logWarning(
                        "LuksHeaderSize: Failed to parse '%s' from option '%s %s'.",
                        matcher.group(1),
                        optRef,
                        val
                    );
                }
            }
        }
        return ret;
    }

    /**
     * Finds the <code>value</code> of a "--&lt;key> &lt;value>" or "--&lt;key>=&lt;value>" pair.
     * @return The found <code>value</code> or <code>null</code> if <code>key</code> was not found or if
     * <code>key</code> is the last entry in <code>userOptionsRef</code>.
     */
    private @Nullable String findLastValue(List<String> userOptionsRef, String optRef)
    {
        Iterator<String> it = userOptionsRef.iterator();
        @Nullable String val = null;
        while (it.hasNext())
        {
            String opt = it.next();
            if (opt.equals(optRef))
            {
                if (it.hasNext())
                {
                    val = it.next();
                }
                else
                {
                    // just in case we found already found a "<key> <value>" pair and now encounter a "<key><EOF>".
                    // since we declared to return the LAST <value> and the last <key> has no <value>, we should also
                    // return null here.
                    val = null;
                }
            }
            else if (opt.startsWith(optRef + "="))
            {
                val = opt.substring(optRef.length() + 1);
                if (val.isBlank())
                {
                    val = null;
                }
            }
        }
        return val;
    }

    private PriorityProps getPrioProps(VlmProviderObject<?> vlmDataRef)
    {
        final AbsVolume<?> vlm = vlmDataRef.getVolume();
        final AbsResource<?> rsc = vlm.getAbsResource();
        final ResourceDefinition rscDfn = vlm.getResourceDefinition();
        final VolumeDefinition vlmDfn = vlm.getVolumeDefinition();
        final ResourceGroup rscGrp = rscDfn.getResourceGroup();

        final ReadOnlyProps vlmProps;
        final ReadOnlyProps rscProps;
        if (vlm instanceof Volume volume)
        {
            vlmProps = volume.getProps();
            rscProps = ((Resource) rsc).getProps();
        }
        else
        {
            vlmProps = ((SnapshotVolume) vlm).getVlmProps();
            rscProps = ((Snapshot) rsc).getRscProps();
        }

        final PriorityProps prioProps = new PriorityProps(
            vlmProps,
            rscProps
        );
        for (StorPool storPool : LayerVlmUtils.getStorPoolSet(vlmDataRef))
        {
            prioProps.addProps(storPool.getProps());
        }
        prioProps.addProps(
            rsc.getNode().getProps(),
            vlmDfn.getProps(),
            rscDfn.getProps(),
            rscGrp.getVolumeGroupProps(vlmDfn.getVolumeNumber()),
            rscGrp.getProps(),
            stltProps
        );
        return prioProps;
    }
}
