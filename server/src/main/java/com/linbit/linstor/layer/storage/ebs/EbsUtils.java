package com.linbit.linstor.layer.storage.ebs;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.CoreModule.RemoteMap;
import com.linbit.linstor.core.identifier.RemoteName;
import com.linbit.linstor.core.objects.AbsResource;
import com.linbit.linstor.core.objects.AbsVolume;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.SnapshotVolume;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.objects.remotes.AbsRemote;
import com.linbit.linstor.core.objects.remotes.EbsRemote;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.ReadOnlyProps;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.data.provider.ebs.EbsData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.utils.layer.LayerRscUtils;

import javax.annotation.Nullable;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class EbsUtils
{
    private static final String EBS_NAMESPC = ApiConsts.NAMESPC_STLT + "/" + ApiConsts.NAMESPC_EBS;

    private static final String EBS_SNAP_ID_BASE_KEY = EBS_NAMESPC + "/" + InternalApiConsts.KEY_EBS_SNAP_ID;
    private static final String EBS_VLM_ID_BASE_KEY = EBS_NAMESPC + "/" + InternalApiConsts.KEY_EBS_VLM_ID;

    public static final String EBS_VLM_STATE_IN_USE = "in-use";
    public static final String EBS_VLM_STATE_COMPLETED = "completed";

    public static final String EBS_SNAP_STATE_COMPLETED = "completed";


    private EbsUtils()
    {
        // utils class
    }

    public static boolean isEbs(AccessContext accCtx, AbsResource<?> rscOrSnapRef) throws AccessDeniedException
    {
        // for now, we only monitor target resources, and snapshots only exist on target anyways
        return rscOrSnapRef.getNode().getNodeType(accCtx).equals(Node.Type.EBS_TARGET);
    }

    public static String getEbsVlmId(AccessContext accCtx, EbsData<?> vlmDataRef) throws AccessDeniedException
    {
        ReadOnlyProps props;
        AbsVolume<?> absVlm = vlmDataRef.getVolume();
        if (absVlm instanceof Volume)
        {
            props = ((Volume) absVlm).getProps(accCtx);
        }
        else
        {
            props = ((SnapshotVolume) absVlm).getVlmProps(accCtx);
        }
        return props.getProp(getEbsVlmIdKey(vlmDataRef));
    }

    public static String getEbsVlmIdKey(EbsData<?> vlmDataRef)
    {
        return getEbsVlmIdKey(vlmDataRef.getRscLayerObject().getResourceNameSuffix());
    }

    public static String getEbsVlmIdKey(String rscSuffixRef)
    {
        return EBS_VLM_ID_BASE_KEY + rscSuffixRef;
    }

    public static String getEbsSnapId(AccessContext accCtx, EbsData<Snapshot> snapVlmDataRef)
        throws AccessDeniedException
    {
        return ((SnapshotVolume) snapVlmDataRef.getVolume())
            .getSnapVlmProps(accCtx)
            .getProp(getEbsSnapIdKey(snapVlmDataRef));
    }

    public static String getEbsSnapId(ReadOnlyProps snapVlmPropsRef, String rscLayerSuffix)
    {
        return snapVlmPropsRef.getProp(getEbsSnapIdKey(rscLayerSuffix));
    }

    public static String getEbsSnapId(Map<String, String> snapVlmPropsPojoRef, String rscLayerSuffix)
    {
        return snapVlmPropsPojoRef.get(getEbsSnapIdKey(rscLayerSuffix));
    }

    public static String getEbsSnapIdKey(EbsData<Snapshot> snapVlmDataRef)
    {
        return getEbsSnapIdKey(snapVlmDataRef.getRscLayerObject().getResourceNameSuffix());
    }

    public static String getEbsSnapIdKey(String rscSuffixRef)
    {
        return EBS_SNAP_ID_BASE_KEY + rscSuffixRef;
    }

    public static EbsRemote getEbsRemote(
        AccessContext accCtxRef,
        RemoteMap remoteMap,
        StorPool storPoolRef,
        ReadOnlyProps stltPropsRef
    )
    {
        AbsRemote remote;
        try
        {
            remote = remoteMap.get(
                new RemoteName(
                    getPrioProps(accCtxRef, storPoolRef, stltPropsRef).getProp(
                        ApiConsts.NAMESPC_STORAGE_DRIVER + "/" + ApiConsts.NAMESPC_EBS + "/" + ApiConsts.KEY_REMOTE
                    ),
                    true
                )
            );
        }
        catch (InvalidKeyException | InvalidNameException | AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        if (!(remote instanceof EbsRemote))
        {
            throw new ImplementationError(
                "Unexpected remote type: " + (remote == null ? "null" : remote.getClass().getSimpleName())
            );
        }
        return (EbsRemote) remote;
    }

    public static PriorityProps getPrioProps(
        AccessContext accCtx,
        StorPool spRef,
        ReadOnlyProps stltProps
    )
        throws AccessDeniedException
    {
        return new PriorityProps(
            spRef.getProps(accCtx),
            spRef.getNode().getProps(accCtx),
            stltProps
        );
    }

    public static boolean hasAnyEbsProp(ReadOnlyProps propsRef)
    {
        @Nullable ReadOnlyProps namespace = propsRef.getNamespace(EBS_NAMESPC);
        return namespace != null && !namespace.isEmpty();
    }

    public static boolean hasAnyEbsProp(Map<String, String> propsPojoRef)
    {
        boolean ret = false;
        for (String key : propsPojoRef.keySet())
        {
            if (key.startsWith(EBS_VLM_ID_BASE_KEY))
            {
                ret = true;
                break;
            }
        }
        return ret;
    }

    public static boolean hasEbsVlms(Resource rscRef, AccessContext accCtxRef) throws AccessDeniedException
    {
        boolean hasEbsVlm = false;
        Set<AbsRscLayerObject<Resource>> storRscDataSet = LayerRscUtils.getRscDataByLayer(
            rscRef.getLayerData(accCtxRef),
            DeviceLayerKind.STORAGE
        );
        for (AbsRscLayerObject<Resource> storRscData : storRscDataSet)
        {
            for (VlmProviderObject<Resource> storVlmData : storRscData.getVlmLayerObjects().values())
            {
                DeviceProviderKind providerKind = storVlmData.getProviderKind();
                if (providerKind.equals(DeviceProviderKind.EBS_INIT) || providerKind.equals(
                    DeviceProviderKind.EBS_TARGET
                ))
                {
                    hasEbsVlm = true;
                    break;
                }
            }
        }
        return hasEbsVlm;
    }

    public static boolean isSnapshotCompleted(AccessContext accessContextRef, Snapshot snapshotRef)
        throws AccessDeniedException
    {
        boolean allCompleted = true;
        Iterator<SnapshotVolume> snapVlmIt = snapshotRef.iterateVolumes();
        while (snapVlmIt.hasNext())
        {
            SnapshotVolume snapVlm = snapVlmIt.next();
            allCompleted &= snapVlm.getState(accessContextRef).equals(EBS_SNAP_STATE_COMPLETED);
        }
        return allCompleted;
    }
}
