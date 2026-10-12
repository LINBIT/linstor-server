package com.linbit.linstor.layer.storage.ebs;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.CoreModule.RemoteMap;
import com.linbit.linstor.core.identifier.RemoteName;
import com.linbit.linstor.core.objects.AbsResource;
import com.linbit.linstor.core.objects.AbsVolume;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.core.objects.SnapshotVolume;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.objects.remotes.AbsRemote;
import com.linbit.linstor.core.objects.remotes.EbsRemote;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.ReadOnlyProps;
import com.linbit.linstor.storage.data.provider.ebs.EbsData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.utils.layer.LayerRscUtils;

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

    public static boolean isEbs(AbsResource<?> rscOrSnapRef)
    {
        // for now, we only monitor target resources, and snapshots only exist on target anyways
        return rscOrSnapRef.getNode().getNodeType().equals(Node.Type.EBS_TARGET);
    }

    public static @Nullable String getEbsVlmId(EbsData<?> vlmDataRef)
    {
        ReadOnlyProps props;
        AbsVolume<?> absVlm = vlmDataRef.getVolume();
        if (absVlm instanceof Volume volume)
        {
            props = volume.getProps();
        }
        else
        {
            props = ((SnapshotVolume) absVlm).getVlmProps();
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

    public static @Nullable String getEbsSnapId(EbsData<Snapshot> snapVlmDataRef)
    {
        return ((SnapshotVolume) snapVlmDataRef.getVolume())
            .getSnapVlmProps()
            .getProp(getEbsSnapIdKey(snapVlmDataRef));
    }

    public static @Nullable String getEbsSnapId(ReadOnlyProps snapVlmPropsRef, String rscLayerSuffix)
    {
        return snapVlmPropsRef.getProp(getEbsSnapIdKey(rscLayerSuffix));
    }

    public static @Nullable String getEbsSnapId(Map<String, String> snapVlmPropsPojoRef, String rscLayerSuffix)
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
                    getPrioProps(storPoolRef, stltPropsRef).getProp(
                        ApiConsts.NAMESPC_STORAGE_DRIVER + "/" + ApiConsts.NAMESPC_EBS + "/" + ApiConsts.KEY_REMOTE
                    ),
                    true
                )
            );
        }
        catch (InvalidKeyException | InvalidNameException exc)
        {
            throw new ImplementationError(exc);
        }
        if (!(remote instanceof EbsRemote ebsRemote))
        {
            throw new ImplementationError(
                "Unexpected remote type: " + (remote == null ? "null" : remote.getClass().getSimpleName())
            );
        }
        return ebsRemote;
    }

    public static PriorityProps getPrioProps(
        StorPool spRef,
        ReadOnlyProps stltProps
    )
    {
        return new PriorityProps(
            spRef.getProps(),
            spRef.getNode().getProps(),
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

    public static boolean hasEbsVlms(Resource rscRef)
    {
        boolean hasEbsVlm = false;
        Set<AbsRscLayerObject<Resource>> storRscDataSet = LayerRscUtils.getRscDataByLayer(
            rscRef.getLayerData(),
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

    public static boolean isSnapshotCompleted(Snapshot snapshotRef)
    {
        boolean allCompleted = true;
        Iterator<SnapshotVolume> snapVlmIt = snapshotRef.iterateVolumes();
        while (snapVlmIt.hasNext())
        {
            SnapshotVolume snapVlm = snapVlmIt.next();
            @Nullable String state = snapVlm.getState();
            if (state == null)
            {
                /*
                 * The in-memory state is only populated by the EBS status poller, so it can be unset
                 * right after the snapshot was taken or after a controller restart. A snapshot whose
                 * creation flow finished (SUCCESSFUL flag) was already confirmed 'completed' by AWS
                 * before the satellite reported success, so fall back to that flag.
                 */
                allCompleted &= snapshotRef.getSnapshotDefinition()
                    .getFlags()
                    .isSet(SnapshotDefinition.Flags.SUCCESSFUL);
            }
            else
            {
                allCompleted &= state.equals(EBS_SNAP_STATE_COMPLETED);
            }
        }
        return allCompleted;
    }
}
