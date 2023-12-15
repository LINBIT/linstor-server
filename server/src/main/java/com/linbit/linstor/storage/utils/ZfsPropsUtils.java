package com.linbit.linstor.storage.utils;

import com.linbit.SizeConv;
import com.linbit.SizeConv.SizeUnit;
import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.ResourceGroup;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ZfsPropsUtils
{
    private static final Pattern PATTERN_EXTENT_SIZE = Pattern.compile("(\\d+)\\s*(.*)");
    public static final int DEFAULT_ZFS_EXTENT_SIZE = 8; // 8K

    private ZfsPropsUtils()
    {
    }

    public static long extractZfsVolBlockSizePrivileged(
        VlmProviderObject<Resource> vlmDataRef,
        @SystemContext AccessContext sysCtx,
        Props stltProps
    )
        throws AccessDeniedException, StorageException
    {
        long extentSize = DEFAULT_ZFS_EXTENT_SIZE;

        Volume vlm = (Volume) vlmDataRef.getVolume();
        Resource rsc = vlm.getAbsResource();
        ResourceDefinition rscDfn = vlm.getResourceDefinition();
        ResourceGroup rscGrp = rscDfn.getResourceGroup();
        VolumeDefinition vlmDfn = vlm.getVolumeDefinition();
        PriorityProps prioProp = new PriorityProps(
            vlm.getProps(sysCtx),
            rsc.getProps(sysCtx),
            vlmDataRef.getStorPool().getProps(sysCtx),
            rsc.getNode().getProps(sysCtx),
            vlmDfn.getProps(sysCtx),
            rscDfn.getProps(sysCtx),
            rscGrp.getVolumeGroupProps(sysCtx, vlmDfn.getVolumeNumber()),
            rscGrp.getProps(sysCtx),
            stltProps
        );
        String zfsCreateProp = prioProp.getProp(
            ApiConsts.KEY_STOR_POOL_ZFS_CREATE_OPTIONS,
            ApiConsts.NAMESPC_STORAGE_DRIVER,
            ""
        );
        List<String> additionalOptions = MkfsUtils.shellSplit(zfsCreateProp);
        String[] zfscreateOptions = new String[additionalOptions.size()];
        additionalOptions.toArray(zfscreateOptions);

        try
        {
            for (int idx = 0; idx < zfscreateOptions.length; idx++)
            {
                String opt = zfscreateOptions[idx];
                String extSizeStr;

                /*
                 * might be {..., "-b", "32k", ... } but also {..., "-b32k", ... }
                 */
                if (opt.equals("-b"))
                {
                    extSizeStr = zfscreateOptions[idx + 1];
                }
                else if (opt.startsWith("-b"))
                {
                    extSizeStr = opt;
                }
                else if (opt.equals("-o"))
                {
                    extSizeStr = zfscreateOptions[idx + 1].startsWith("volblocksize=") ?
                        zfscreateOptions[idx + 1] :
                        null;
                }
                else if (opt.startsWith("-ovolblocksize="))
                {
                    extSizeStr = opt;
                }
                else
                {
                    extSizeStr = null;
                }

                if (extSizeStr != null)
                {
                    Matcher matcher = PATTERN_EXTENT_SIZE.matcher(extSizeStr);
                    if (matcher.find())
                    {
                        long val = Long.parseLong(matcher.group(1));
                        SizeUnit unit = SizeUnit.parse(matcher.group(2), true);

                        extentSize = SizeConv.convert(val, unit, SizeUnit.UNIT_KiB);
                    }
                    else
                    {
                        throw new StorageException("Could not find blocksize in string: " + extSizeStr);
                    }
                    break;
                }
            }
        }
        catch (ArrayIndexOutOfBoundsException boundsExc)
        {
            throw new StorageException(
                "Expected additional argument while looking for extentSize in: " + Arrays.toString(zfscreateOptions)
            );
        }
        catch (NumberFormatException nfe)
        {
            throw new StorageException("Could not parse blocksize", nfe);
        }
        catch (IllegalArgumentException exc)
        {
            throw new StorageException("Could not parse blocksize unit ", exc);
        }
        return extentSize;
    }
}
