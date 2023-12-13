package com.linbit.linstor.core.objects.utils;

import com.linbit.ImplementationError;
import com.linbit.SizeConv;
import com.linbit.SizeConv.SizeUnit;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.ResourceGroup;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.core.repository.SystemConfRepository;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.InvalidValueException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.StorageConstants;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.data.RscLayerSuffixes;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.storage.kinds.ExtTools;
import com.linbit.linstor.storage.kinds.ExtToolsInfo;
import com.linbit.linstor.storage.utils.MkfsUtils;
import com.linbit.linstor.utils.externaltools.ExtToolsManager;
import com.linbit.linstor.utils.layer.LayerRscUtils;
import com.linbit.linstor.utils.layer.LayerVlmUtils;
import com.linbit.utils.MathUtils;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Singleton
public class MixedStorPoolHelper
{
    private static final Pattern PATTERN_EXTENT_SIZE = Pattern.compile("(\\d+)\\s*(.*)");

    private final AccessContext sysCtx;
    private final SystemConfRepository systemConfRepository;

    @Inject
    public MixedStorPoolHelper(@SystemContext AccessContext sysCtxRef, SystemConfRepository systemConfRepositoryRef)
    {
        sysCtx = sysCtxRef;
        systemConfRepository = systemConfRepositoryRef;
    }

    public void handleMixedStoragePools(Volume vlmRef)
        throws AccessDeniedException, DatabaseException, ImplementationError, StorageException
    {
        VolumeDefinition vlmDfn = vlmRef.getVolumeDefinition();
        ResourceDefinition rscDfn = vlmDfn.getResourceDefinition();
        Set<StorPool> storPoolSet = getAllStorPools(rscDfn);
        if (hasMixedStoragePools(storPoolSet))
        {
            ensureAllStltsHaveDrbdVersion(storPoolSet);
            try
            {
                rscDfn.getProps(sysCtx)
                    .setProp(
                        InternalApiConsts.KEY_FORCE_INITIAL_SYNC_PERMA,
                        ApiConsts.VAL_TRUE,
                        ApiConsts.NAMESPC_DRBD_OPTIONS
                    );
            }
            catch (AccessDeniedException | InvalidKeyException | InvalidValueException exc)
            {
                throw new ImplementationError(exc);
            }
        }

        Props vlmDfnProps = vlmDfn.getProps(sysCtx);

        try
        {
            vlmDfnProps.setProp(
                InternalApiConsts.ALLOCATION_GRANULARITY,
                Long.toString(getLeastCommonExtentSizeInKib(vlmRef)),
                StorageConstants.NAMESPACE_INTERNAL
            );
        }
        catch (InvalidKeyException | InvalidValueException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    private void ensureAllStltsHaveDrbdVersion(Set<StorPool> storPoolSetRef)
    {
        try
        {
            for (StorPool sp : storPoolSetRef)
            {
                ExtToolsManager extToolsMgr = sp.getNode()
                    .getPeer(sysCtx)
                    .getExtToolsManager();
                ExtToolsInfo info = extToolsMgr.getExtToolInfo(ExtTools.DRBD9_KERNEL);
                if (info == null)
                {
                    throw new ApiRcException(
                        ApiCallRcImpl.simpleEntry(
                            ApiConsts.FAIL_STLT_DOES_NOT_SUPPORT_LAYER,
                            sp.getNode() + " does not support DRBD9!"
                        )
                    );
                }
                if (!DeviceProviderKind.doesDrbdVersionSupportStorPoolMixing(info.getVersion()))
                {
                    throw new ApiRcException(
                        ApiCallRcImpl.simpleEntry(
                            ApiConsts.FAIL_STLT_DOES_NOT_SUPPORT_LAYER,
                            String.format(
                                "%s has DRBD version %s, but version %s (or higher) is required",
                                sp.getNode(),
                                info.getVersion(),
                                info.getVersionMinor() == 1 ?
                                    DeviceProviderKind.SP_MIXING_REQ_DRBD91_MIN_VERISON :
                                    DeviceProviderKind.SP_MIXING_REQ_DRBD92_MIN_VERISON
                            )
                        )
                    );
                }
            }
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    private Set<StorPool> getAllStorPools(ResourceDefinition resourceDefinitionRef)
        throws AccessDeniedException
    {
        Set<StorPool> ret = new HashSet<>();
        Iterator<Resource> rscIt = resourceDefinitionRef.iterateResource(sysCtx);
        while (rscIt.hasNext())
        {
            Resource rsc = rscIt.next();
            Set<StorPool> storPools = LayerVlmUtils.getStorPools(rsc, sysCtx, false);
            ret.addAll(storPools);
        }
        return ret;
    }

    private boolean hasMixedStoragePools(Set<StorPool> storagePoolsRef)
    {
        boolean usesThick = false;
        boolean usesThin = false;

        Set<String> allocationGranularities = new HashSet<>();
        try
        {
            for (StorPool sp : storagePoolsRef)
            {
                DeviceProviderKind kind = sp.getDeviceProviderKind();
                if (!kind.equals(DeviceProviderKind.DISKLESS))
                {
                    if (kind.usesThinProvisioning())
                    {
                        usesThin = true;
                    }
                    else
                    {
                        usesThick = true;
                    }
                    allocationGranularities.add(
                        sp.getProps(sysCtx)
                            .getProp(InternalApiConsts.ALLOCATION_GRANULARITY, StorageConstants.NAMESPACE_INTERNAL)
                    );
                }
            }
        }
        catch (InvalidKeyException | AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        return (usesThick && usesThin) || allocationGranularities.size() > 1;
    }

    private long getLeastCommonExtentSizeInKib(Volume vlmRef)
        throws AccessDeniedException, StorageException
    {
        SortedSet<Long> minGranularities = new TreeSet<>();
        Long vlmDfnExtentSize = getCurrentVlmDfnExtentSize(vlmRef.getVolumeDefinition());
        if (vlmDfnExtentSize != null)
        {
            minGranularities.add(vlmDfnExtentSize);
        }
        Set<Long> storPoolAndVlmExtentSizes = extractStorPoolAndVlmExtentSizes(vlmRef);
        minGranularities.addAll(storPoolAndVlmExtentSizes);
        return MathUtils.getLeastCommonMultiple(minGranularities);
    }

    private @Nullable Long getCurrentVlmDfnExtentSize(VolumeDefinition vlmDfnRef)
        throws AccessDeniedException
    {
        String currentGranularityStr;
        try
        {
            currentGranularityStr = vlmDfnRef.getProps(sysCtx)
                .getProp(
                    InternalApiConsts.ALLOCATION_GRANULARITY,
                    StorageConstants.NAMESPACE_INTERNAL
                );
        }
        catch (InvalidKeyException exc)
        {
            throw new ImplementationError(exc);
        }
        return currentGranularityStr == null ? null : Long.parseLong(currentGranularityStr);
    }

    private Set<Long> extractStorPoolAndVlmExtentSizes(Volume vlmRef)
        throws AccessDeniedException, StorageException
    {
        Set<Long> extentSizes = new HashSet<>();

        AbsRscLayerObject<Resource> rscData = vlmRef.getAbsResource().getLayerData(sysCtx);
        Set<AbsRscLayerObject<Resource>> storRscDataSet = LayerRscUtils.getRscDataByLayer(
            rscData,
            DeviceLayerKind.STORAGE,
            RscLayerSuffixes::isNonMetaDataLayerSuffix
        );
        for (AbsRscLayerObject<Resource> storRscData : storRscDataSet)
        {
            for (VlmProviderObject<Resource> vlmData : storRscData.getVlmLayerObjects().values())
            {
                StorPool sp = vlmData.getStorPool();
                String spGranu = sp.getProps(sysCtx)
                    .getProp(
                        InternalApiConsts.ALLOCATION_GRANULARITY,
                        StorageConstants.NAMESPACE_INTERNAL
                    );
                if (spGranu != null)
                {
                    extentSizes.add(Long.parseLong(spGranu));
                }
                Long vlmExtentSize = null;
                switch (sp.getDeviceProviderKind())
                {
                    case DISKLESS: // fall-through
                    case EBS_INIT:// fall-through
                    case EBS_TARGET:// fall-through
                    case EXOS:// fall-through
                    case FILE:// fall-through
                    case FILE_THIN:// fall-through
                    case LVM:// fall-through
                    case LVM_THIN:// fall-through
                    case REMOTE_SPDK:// fall-through
                    case SPDK:// fall-through
                    case STORAGE_SPACES:// fall-through
                    case STORAGE_SPACES_THIN:
                        break;
                    case ZFS:// fall-through
                    case ZFS_THIN:
                        vlmExtentSize = extractZfsVolBlockSize(vlmData);
                        break;
                    case FAIL_BECAUSE_NOT_A_VLM_PROVIDER_BUT_A_VLM_LAYER:
                    default:
                        throw new ImplementationError("Not implemented for kind: " + sp.getDeviceProviderKind());
                }
                if (vlmExtentSize != null)
                {
                    extentSizes.add(vlmExtentSize);
                }
            }
        }

        return extentSizes;
    }

    private Long extractZfsVolBlockSize(VlmProviderObject<Resource> vlmDataRef)
        throws AccessDeniedException, StorageException
    {
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
            systemConfRepository.getStltConfForView(sysCtx)
        );
        String zfsCreateProp = prioProp.getProp(
            ApiConsts.KEY_STOR_POOL_ZFS_CREATE_OPTIONS,
            ApiConsts.NAMESPC_STORAGE_DRIVER,
            ""
        );
        List<String> additionalOptions = MkfsUtils.shellSplit(zfsCreateProp);
        String[] zfscreateOptions = new String[additionalOptions.size()];
        additionalOptions.toArray(zfscreateOptions);

        Long extentSize = null;
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
