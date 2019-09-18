package com.linbit.linstor.api.rest.v1.serializer;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.interfaces.AutoSelectFilterApi;
import com.linbit.linstor.api.interfaces.RscDfnLayerDataApi;
import com.linbit.linstor.api.interfaces.RscLayerDataApi;
import com.linbit.linstor.api.interfaces.VlmDfnLayerDataApi;
import com.linbit.linstor.api.interfaces.VlmLayerDataApi;
import com.linbit.linstor.api.pojo.DrbdRscPojo;
import com.linbit.linstor.api.pojo.LuksRscPojo;
import com.linbit.linstor.api.pojo.MaxVlmSizeCandidatePojo;
import com.linbit.linstor.api.pojo.NetInterfacePojo;
import com.linbit.linstor.api.pojo.NvmeRscPojo;
import com.linbit.linstor.api.pojo.RscPojo;
import com.linbit.linstor.api.pojo.StorageRscPojo;
import com.linbit.linstor.api.pojo.VlmDfnPojo;
import com.linbit.linstor.api.rest.v1.serializer.JsonGenTypes.AutoSelectFilter;
import com.linbit.linstor.core.apis.NetInterfaceApi;
import com.linbit.linstor.core.apis.NodeApi;
import com.linbit.linstor.core.apis.ResourceApi;
import com.linbit.linstor.core.apis.ResourceConnectionApi;
import com.linbit.linstor.core.apis.ResourceDefinitionApi;
import com.linbit.linstor.core.apis.ResourceGroupApi;
import com.linbit.linstor.core.apis.SnapshotDefinitionListItemApi;
import com.linbit.linstor.core.apis.SnapshotVolumeDefinitionApi;
import com.linbit.linstor.core.apis.StorPoolApi;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceConnection;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.core.objects.StorPoolDefinitionData;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.core.objects.VolumeGroup;
import com.linbit.linstor.satellitestate.SatelliteResourceState;
import com.linbit.linstor.satellitestate.SatelliteState;
import com.linbit.linstor.satellitestate.SatelliteVolumeState;
import com.linbit.linstor.stateflags.FlagsHelper;
import com.linbit.linstor.storage.interfaces.layers.drbd.DrbdRscObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.utils.Pair;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Json
{
    public static String deviceProviderKindAsString(DeviceProviderKind deviceProviderKind)
    {
        return deviceProviderKind.name();
    }

    private static String getLayerTypeString(DeviceLayerKind deviceLayerKind)
    {
        return deviceLayerKind.name();
    }

    public static JsonGenTypes.Node apiToNode(
        NodeApi nodeApi
    )
    {
        JsonGenTypes.Node nd = new JsonGenTypes.Node();
        nd.name = nodeApi.getName();
        nd.type = nodeApi.getType();
        nd.connection_status = nodeApi.connectionStatus().toString();
        nd.props = nodeApi.getProps();
        nd.flags = Node.Flags.toStringList(nodeApi.getFlags());

        List<JsonGenTypes.NetInterface> netIfsList =
            nodeApi.getNetInterfaces().stream().map(Json::apiToNetInterface).collect(Collectors.toList());
        NetInterfaceApi activeStltConn = nodeApi.getActiveStltConn();
        for (JsonGenTypes.NetInterface netInterface : netIfsList)
        {
            if (activeStltConn != null && netInterface.uuid.equalsIgnoreCase(activeStltConn.getUuid().toString()))
            {
                netInterface.is_active = Boolean.TRUE;
            }

        }

        nd.net_interfaces = netIfsList;
        nd.uuid = nodeApi.getUuid().toString();
        return nd;
    }

    public static JsonGenTypes.NetInterface apiToNetInterface(NetInterfaceApi netIfApi)
    {
        JsonGenTypes.NetInterface netif = new JsonGenTypes.NetInterface();
        netif.name = netIfApi.getName();
        netif.address = netIfApi.getAddress();
        if (netIfApi.isUsableAsSatelliteConnection())
        {
            netif.satellite_encryption_type = netIfApi.getSatelliteConnectionEncryptionType();
            netif.satellite_port = netIfApi.getSatelliteConnectionPort();
        }
        netif.is_active = Boolean.FALSE; // default value
        netif.uuid = netIfApi.getUuid().toString();
        return netif;
    }

    public static NetInterfaceApi netInterfacetoApi(JsonGenTypes.NetInterface netif)
    {
        return new NetInterfacePojo(
            null,
            netif.name,
            netif.address,
            netif.satellite_port,
            netif.satellite_encryption_type
        );
    }

    public static JsonGenTypes.StoragePoolDefinition storPoolDfnApiToStoragePoolDefinition(
        StorPoolDefinitionData.StorPoolDfnApi apiData
    )
    {
        JsonGenTypes.StoragePoolDefinition storPoolDfn = new JsonGenTypes.StoragePoolDefinition();
        storPoolDfn.storage_pool_name = apiData.getName();
        storPoolDfn.props = apiData.getProps();
        return storPoolDfn;
    }

    public static List<JsonGenTypes.ApiCallRc> apiCallRcToJson(ApiCallRc apiCallRc)
    {
        return apiCallRc.getEntries().stream()
            .map(apiCallRcEntry ->
            {
               JsonGenTypes.ApiCallRc jsonApiCallRc = new JsonGenTypes.ApiCallRc();
              jsonApiCallRc.message = apiCallRcEntry.getMessage();
              jsonApiCallRc.cause = apiCallRcEntry.getCause();
              jsonApiCallRc.correction = apiCallRcEntry.getCorrection();
              jsonApiCallRc.details = apiCallRcEntry.getDetails();
              jsonApiCallRc.error_report_ids = new ArrayList<>(apiCallRcEntry.getErrorIds());
              jsonApiCallRc.obj_refs = apiCallRcEntry.getObjRefs();
              jsonApiCallRc.ret_code = apiCallRcEntry.getReturnCode();
               return jsonApiCallRc;
            }).collect(Collectors.toList());
    }

    public static JsonGenTypes.StoragePool storPoolApiToStoragePool(
        StorPoolApi storPoolApi
    )
    {
        JsonGenTypes.StoragePool storPoolData = new JsonGenTypes.StoragePool();
        storPoolData.storage_pool_name = storPoolApi.getStorPoolName();
        storPoolData.node_name = storPoolApi.getNodeName();
        storPoolData.provider_kind = Json.deviceProviderKindAsString(
            storPoolApi.getDeviceProviderKind()
        );
        storPoolData.props = storPoolApi.getStorPoolProps();
        storPoolData.static_traits = storPoolApi.getStorPoolStaticTraits();
        storPoolData.free_capacity = storPoolApi.getFreeCapacity().orElse(null);
        storPoolData.total_capacity = storPoolApi.getTotalCapacity().orElse(null);
        storPoolData.free_space_mgr_name = storPoolApi.getFreeSpaceManagerName();
        storPoolData.uuid = storPoolApi.getStorPoolUuid().toString();
        storPoolData.reports = apiCallRcToJson(storPoolApi.getReports());
        storPoolData.supports_snapshots = storPoolApi.supportsSnapshots();

        return storPoolData;
    }

    public static JsonGenTypes.DrbdResourceDefinitionLayer pojoToDrbdRscDfnLayer(
        DrbdRscPojo.DrbdRscDfnPojo drbdRscDfnPojo
    )
    {
        JsonGenTypes.DrbdResourceDefinitionLayer drbdResourceDefinitionLayer = new
            JsonGenTypes.DrbdResourceDefinitionLayer();
        drbdResourceDefinitionLayer.resource_name_suffix = drbdRscDfnPojo.getRscNameSuffix();
        drbdResourceDefinitionLayer.peer_slots = (int) drbdRscDfnPojo.getPeerSlots();
        drbdResourceDefinitionLayer.al_stripes = drbdRscDfnPojo.getAlStripes();
        drbdResourceDefinitionLayer.al_stripe_size_kib = drbdRscDfnPojo.getAlStripeSize();
        drbdResourceDefinitionLayer.port = drbdRscDfnPojo.getPort();
        drbdResourceDefinitionLayer.transport_type = drbdRscDfnPojo.getTransportType();
        drbdResourceDefinitionLayer.secret = drbdRscDfnPojo.getSecret();
        drbdResourceDefinitionLayer.down = drbdRscDfnPojo.isDown();
        return drbdResourceDefinitionLayer;
    }

    public static JsonGenTypes.ResourceDefinition apiToResourceDefinition(
        ResourceDefinitionApi rscDfnApi
    )
    {
        JsonGenTypes.ResourceDefinition rscDfn = new JsonGenTypes.ResourceDefinition();
        rscDfn.name = rscDfnApi.getResourceName();
        if (rscDfnApi.getExternalName() != null)
        {
            rscDfn.external_name = new String(rscDfnApi.getExternalName(), StandardCharsets.UTF_8);
        }
        rscDfn.flags = ResourceDefinition.Flags.toStringList(rscDfnApi.getFlags());
        rscDfn.props = rscDfnApi.getProps();
        rscDfn.uuid = rscDfnApi.getUuid().toString();

        rscDfn.layer_data = new ArrayList<>();

        for (Pair<String, RscDfnLayerDataApi> layer : rscDfnApi.getLayerData())
        {
            JsonGenTypes.ResourceDefinitionLayer rscDfnLayerData = new JsonGenTypes.ResourceDefinitionLayer();
            rscDfnLayerData.type = layer.objA;

            if (layer.objB != null)
            {
                switch (layer.objB.getLayerKind())
                {
                    case DRBD:
                        DrbdRscPojo.DrbdRscDfnPojo drbdRscDfnPojo = (DrbdRscPojo.DrbdRscDfnPojo) layer.objB;
                        rscDfnLayerData.data = pojoToDrbdRscDfnLayer(drbdRscDfnPojo);
                        break;
                    case LUKS:
                    case STORAGE:
                    case NVME:
                    default:
                        throw new ImplementationError("Not implemented Kind case");
                }
            }
            rscDfn.layer_data.add(rscDfnLayerData);
        }
        rscDfn.resource_group_name = rscDfnApi.getResourceGroup().getName();
        return rscDfn;
    }

    public static JsonGenTypes.DrbdVolumeDefinition pojoToDrbdVolumeDefinition(
        DrbdRscPojo.DrbdVlmDfnPojo drbdVlmDfnPojo
    )
    {
        JsonGenTypes.DrbdVolumeDefinition drbdVlmDfn = new JsonGenTypes.DrbdVolumeDefinition();
        drbdVlmDfn.resource_name_suffix = drbdVlmDfnPojo.getRscNameSuffix();
        drbdVlmDfn.volume_number = drbdVlmDfnPojo.getVlmNr();
        drbdVlmDfn.minor_number = drbdVlmDfnPojo.getMinorNr();
        return drbdVlmDfn;
    }

    public static JsonGenTypes.VolumeDefinition apiToVolumeDefinition(
        VolumeDefinition.VlmDfnApi vlmDfnApi
    )
    {
        JsonGenTypes.VolumeDefinition vlmDfn = new JsonGenTypes.VolumeDefinition();
        vlmDfn.volume_number = vlmDfnApi.getVolumeNr();
        vlmDfn.size_kib = vlmDfnApi.getSize();
        vlmDfn.props = vlmDfnApi.getProps();
        vlmDfn.flags = FlagsHelper.toStringList(
            VolumeDefinition.VlmDfnFlags.class,
            vlmDfnApi.getFlags()
        );
        vlmDfn.uuid = vlmDfnApi.getUuid().toString();

        vlmDfn.layer_data = new ArrayList<>();
        for (Pair<String, VlmDfnLayerDataApi> layer : vlmDfnApi.getVlmDfnLayerData())
        {
            JsonGenTypes.VolumeDefinitionLayer volumeDefinitionLayerData = new JsonGenTypes.VolumeDefinitionLayer();
            volumeDefinitionLayerData.type = layer.objA;

            if (layer.objB != null)
            {
                switch (layer.objB.getLayerKind())
                {
                    case DRBD:
                        DrbdRscPojo.DrbdVlmDfnPojo drbdVlmDfnPojo = (DrbdRscPojo.DrbdVlmDfnPojo) layer.objB;
                        volumeDefinitionLayerData.data = pojoToDrbdVolumeDefinition(drbdVlmDfnPojo);
                        break;
                    case LUKS:
                    case STORAGE:
                    case NVME:
                    default:
                        throw new ImplementationError("Not implemented Kind case");
                }
            }
            vlmDfn.layer_data.add(volumeDefinitionLayerData);
        }
        return vlmDfn;
    }

    public static VolumeDefinition.VlmDfnApi VolumeDefinitionToApi(
        JsonGenTypes.VolumeDefinition vlmDfn
    )
    {
        ArrayList<Pair<String, VlmDfnLayerDataApi>> layerData = new ArrayList<>(); // empty will be ignored
        return new VlmDfnPojo(
            null,
            vlmDfn.volume_number,
            vlmDfn.size_kib,
            FlagsHelper.fromStringList(VolumeDefinition.VlmDfnFlags.class, vlmDfn.flags),
            vlmDfn.props,
            layerData
        );
    }

    public static JsonGenTypes.DrbdResource pojoToDrbdResource(DrbdRscPojo drbdRscPojo)
    {
        JsonGenTypes.DrbdResource drbdResource = new JsonGenTypes.DrbdResource();
        drbdResource.drbd_resource_definition = pojoToDrbdRscDfnLayer(drbdRscPojo.getDrbdRscDfn());
        drbdResource.node_id = drbdRscPojo.getNodeId();
        drbdResource.peer_slots = (int) drbdRscPojo.getPeerSlots();
        drbdResource.al_stripes = drbdRscPojo.getAlStripes();
        drbdResource.al_size = drbdRscPojo.getAlStripeSize();
        drbdResource.flags = FlagsHelper.toStringList(DrbdRscObject.DrbdRscFlags.class, drbdRscPojo.getFlags());
        drbdResource.drbd_volumes = drbdRscPojo.getVolumeList().stream()
            .map(Json::pojoToDrbdVolume)
            .collect(Collectors.toList());
        return drbdResource;
    }

    public static JsonGenTypes.StorageResource pojoToStorageResource(StorageRscPojo storageRscPojo)
    {
        JsonGenTypes.StorageResource storageResource = new JsonGenTypes.StorageResource();
        storageResource.storage_volumes = storageRscPojo.getVolumeList().stream()
            .map(Json::apiToStorageVolume)
            .collect(Collectors.toList());
        return storageResource;
    }

    public static JsonGenTypes.LUKSResource pojoToLUKSResource(LuksRscPojo luksRscPojo)
    {
        JsonGenTypes.LUKSResource luksResource = new JsonGenTypes.LUKSResource();
        luksResource.luks_volumes = luksRscPojo.getVolumeList().stream()
            .map(Json::pojoToLUKSVolume)
            .collect(Collectors.toList());
        return luksResource;
    }

    public static JsonGenTypes.NVMEResource pojoToNVMEResource(NvmeRscPojo nvmeRscPojo)
    {
        JsonGenTypes.NVMEResource nvmeResource = new JsonGenTypes.NVMEResource();
        nvmeResource.nvme_volumes = nvmeRscPojo.getVolumeList().stream()
            .map(Json::pojoToNVMEVolume)
            .collect(Collectors.toList());
        return nvmeResource;
    }

    public static JsonGenTypes.ResourceLayer apiToResourceLayer(RscLayerDataApi rscLayerDataApi)
    {
        JsonGenTypes.ResourceLayer resourceLayer = new JsonGenTypes.ResourceLayer();
        resourceLayer.resource_name_suffix = rscLayerDataApi.getRscNameSuffix();
        resourceLayer.children = rscLayerDataApi.getChildren().stream()
            .map(Json::apiToResourceLayer)
            .collect(Collectors.toList());
        resourceLayer.type = getLayerTypeString(rscLayerDataApi.getLayerKind());
        switch (rscLayerDataApi.getLayerKind())
        {
            case DRBD:
                DrbdRscPojo drbdRscPojo = (DrbdRscPojo) rscLayerDataApi;
                resourceLayer.drbd = pojoToDrbdResource(drbdRscPojo);
                break;
            case STORAGE:
                StorageRscPojo storageRscPojo = (StorageRscPojo) rscLayerDataApi;
                resourceLayer.storage = pojoToStorageResource(storageRscPojo);
                break;
            case LUKS:
                LuksRscPojo luksRscPojo = (LuksRscPojo) rscLayerDataApi;
                resourceLayer.luks = pojoToLUKSResource(luksRscPojo);
                break;
            case NVME:
                NvmeRscPojo nvmeRscPojo = (NvmeRscPojo) rscLayerDataApi;
                resourceLayer.nvme = pojoToNVMEResource(nvmeRscPojo);
                break;
            default:
        }
        return resourceLayer;
    }

    public static JsonGenTypes.ResourceWithVolumes apiToResourceWithVolumes(
        ResourceApi rscApi,
        Map<NodeName, SatelliteState> satelliteStates,
        boolean withVolumes
    )
    {
        JsonGenTypes.ResourceWithVolumes rsc = new JsonGenTypes.ResourceWithVolumes();
        rsc.name = rscApi.getName();
        rsc.node_name = rscApi.getNodeName();
        rsc.flags = FlagsHelper.toStringList(Resource.Flags.class, rscApi.getFlags());
        rsc.props = rscApi.getProps();
        rsc.layer_object = apiToResourceLayer(rscApi.getLayerData());
        rsc.uuid = rscApi.getUuid().toString();

        if (withVolumes)
        {
            rsc.volumes = rscApi.getVlmList().stream().map(vlmApi ->
                {
                    JsonGenTypes.Volume vlmData = Json.apiToVolume(vlmApi);
                    JsonGenTypes.VolumeState vlmState = null;
                    try
                    {
                        final ResourceName rscNameRes = new ResourceName(rsc.name);
                        final NodeName linNodeName = new NodeName(rsc.node_name);
                        if (satelliteStates.containsKey(linNodeName) &&
                            satelliteStates.get(linNodeName)
                                .getResourceStates().containsKey(rscNameRes))
                        {
                            SatelliteResourceState satResState = satelliteStates
                                .get(linNodeName)
                                .getResourceStates()
                                .get(rscNameRes);

                            VolumeNumber vlmNumber = new VolumeNumber(vlmData.volume_number);
                            if (satResState.getVolumeStates().containsKey(vlmNumber))
                            {
                                vlmState = new JsonGenTypes.VolumeState();
                                SatelliteVolumeState satVlmState = satResState.getVolumeStates().get(vlmNumber);
                                vlmState.disk_state = satVlmState.getDiskState();
                            }
                        }
                    }
                    catch (InvalidNameException | ValueOutOfRangeException ignored)
                    {
                    }
                    vlmData.state = vlmState;
                    return vlmData;
                }).collect(Collectors.toList());
        }

        try
        {
            final ResourceName rscNameRes = new ResourceName(rscApi.getName());
            final NodeName linNodeName = new NodeName(rscApi.getNodeName());
            if (satelliteStates.containsKey(linNodeName) &&
                satelliteStates.get(linNodeName)
                    .getResourceStates().containsKey(rscNameRes))
            {
                rsc.state = new JsonGenTypes.ResourceState();
                rsc.state.in_use = satelliteStates.get(linNodeName)
                    .getResourceStates().get(rscNameRes).isInUse();
            }
        }
        catch (InvalidNameException ignored)
        {
        }
        return rsc;
    }

    public static JsonGenTypes.Resource apiToResource(
        ResourceApi rscApi,
        Map<NodeName, SatelliteState> satelliteStates
    )
    {
        return apiToResourceWithVolumes(rscApi, satelliteStates, false);
    }

    public static ResourceApi resourceToApi(JsonGenTypes.Resource resource)
    {
        return new RscPojo(
            resource.name,
            resource.node_name,
            FlagsHelper.fromStringList(Resource.Flags.class, resource.flags),
            resource.props
        );
    }

    public static JsonGenTypes.DrbdVolume pojoToDrbdVolume(
        DrbdRscPojo.DrbdVlmPojo drbdVlmPojo
    )
    {
        JsonGenTypes.DrbdVolume drbdVolume = new JsonGenTypes.DrbdVolume();
        drbdVolume.drbd_volume_definition = pojoToDrbdVolumeDefinition(drbdVlmPojo.getDrbdVlmDfn());
        drbdVolume.device_path = drbdVlmPojo.getDevicePath();
        drbdVolume.backing_device = drbdVlmPojo.getBackingDisk();
        drbdVolume.meta_disk = drbdVlmPojo.getMetaDisk();
        drbdVolume.allocated_size_kib = drbdVlmPojo.getAllocatedSize();
        drbdVolume.usable_size_kib = drbdVlmPojo.getUsableSize();
        drbdVolume.disk_state = drbdVlmPojo.getDiskState();
        drbdVolume.ext_meta_stor_pool = drbdVlmPojo.getExternalMetaDataStorPool();
        return drbdVolume;
    }

    public static JsonGenTypes.LUKSVolume pojoToLUKSVolume(
        LuksRscPojo.LuksVlmPojo luksVlmPojo
    )
    {
        JsonGenTypes.LUKSVolume luksVolume = new JsonGenTypes.LUKSVolume();
        luksVolume.volume_number = luksVlmPojo.getVlmNr();
        luksVolume.device_path = luksVlmPojo.getDevicePath();
        luksVolume.backing_device = luksVlmPojo.getBackingDevice();
        luksVolume.allocated_size_kib = luksVlmPojo.getAllocatedSize();
        luksVolume.usable_size_kib = luksVlmPojo.getUsableSize();
        luksVolume.opened = luksVlmPojo.isOpened();
        luksVolume.disk_state = luksVlmPojo.getDiskState();
        return luksVolume;
    }

    public static JsonGenTypes.NVMEVolume pojoToNVMEVolume(
        NvmeRscPojo.NvmeVlmPojo nvmeVlmPojo
    )
    {
        JsonGenTypes.NVMEVolume nvmeVolume = new JsonGenTypes.NVMEVolume();
        nvmeVolume.volume_number = nvmeVlmPojo.getVlmNr();
        nvmeVolume.device_path = nvmeVlmPojo.getDevicePath();
        nvmeVolume.backing_device = nvmeVlmPojo.getBackingDisk();
        nvmeVolume.allocated_size_kib = nvmeVlmPojo.getAllocatedSize();
        nvmeVolume.usable_size_kib = nvmeVlmPojo.getUsableSize();
        nvmeVolume.disk_state = nvmeVlmPojo.getDiskState();
        return nvmeVolume;
    }

    public static JsonGenTypes.StorageVolume apiToStorageVolume(
        VlmLayerDataApi vlmLayerDataApi
    )
    {
        JsonGenTypes.StorageVolume storageVolume = new JsonGenTypes.StorageVolume();
        storageVolume.volume_number = vlmLayerDataApi.getVlmNr();
        storageVolume.device_path = vlmLayerDataApi.getDevicePath();
        storageVolume.allocated_size_kib = vlmLayerDataApi.getAllocatedSize();
        storageVolume.usable_size_kib = vlmLayerDataApi.getUsableSize();
        storageVolume.disk_state = vlmLayerDataApi.getDiskState();
        return storageVolume;
    }

    public static JsonGenTypes.Volume apiToVolume(Volume.VlmApi vlmApi)
    {
        JsonGenTypes.Volume volume = new JsonGenTypes.Volume();
        volume.volume_number = vlmApi.getVlmNr();
        volume.storage_pool_name = vlmApi.getStorPoolName();
        volume.provider_kind = deviceProviderKindAsString(vlmApi.getStorPoolDeviceProviderKind());

        volume.device_path = vlmApi.getDevicePath();
        volume.allocated_size_kib = vlmApi.getAllocatedSize().orElse(null);

        volume.props = vlmApi.getVlmProps();
        volume.flags = FlagsHelper.toStringList(Volume.VlmFlags.class, vlmApi.getFlags());
        volume.uuid = vlmApi.getVlmUuid().toString();

        volume.layer_data_list = new ArrayList<>();

        for (Pair<String, VlmLayerDataApi> layerData : vlmApi.getVlmLayerData())
        {
            JsonGenTypes.VolumeLayer volumeLayerData = new JsonGenTypes.VolumeLayer();
            volumeLayerData.type = getLayerTypeString(layerData.objB.getLayerKind());

            switch (layerData.objB.getLayerKind())
            {
                case DRBD:
                    DrbdRscPojo.DrbdVlmPojo drbdVlm = (DrbdRscPojo.DrbdVlmPojo) layerData.objB;
                    volumeLayerData.data = pojoToDrbdVolume(drbdVlm);
                    break;
                case STORAGE:
                    volumeLayerData.data = apiToStorageVolume(layerData.objB);
                    break;
                case LUKS:
                    LuksRscPojo.LuksVlmPojo luksVlmPojo = (LuksRscPojo.LuksVlmPojo) layerData.objB;
                    volumeLayerData.data = pojoToLUKSVolume(luksVlmPojo);
                    break;
                case NVME:
                    NvmeRscPojo.NvmeVlmPojo nvmeVlmPojo = (NvmeRscPojo.NvmeVlmPojo) layerData.objB;
                    volumeLayerData.data = pojoToNVMEVolume(nvmeVlmPojo);
                    break;
                default:
            }

            volume.layer_data_list.add(volumeLayerData);
        }
        return volume;
    }

    public static class AutoSelectFilterData implements AutoSelectFilterApi
    {
        private final JsonGenTypes.AutoSelectFilter autoSelectFilter;

        public AutoSelectFilterData(JsonGenTypes.AutoSelectFilter autoSelectFilterRef)
        {
            autoSelectFilter = autoSelectFilterRef;
        }

        @Override
        public Integer getReplicaCount()
        {
            return autoSelectFilter.place_count;
        }

        @Override
        public String getStorPoolNameStr()
        {
            return autoSelectFilter.storage_pool;
        }

        @Override
        public List<String> getDoNotPlaceWithRscList()
        {
            return autoSelectFilter.not_place_with_rsc;
        }

        @Override
        public String getDoNotPlaceWithRscRegex()
        {
            return autoSelectFilter.not_place_with_rsc_regex;
        }

        @Override
        public List<String> getReplicasOnSameList()
        {
            return autoSelectFilter.replicas_on_same;
        }

        @Override
        public List<String> getReplicasOnDifferentList()
        {
            return autoSelectFilter.replicas_on_different;
        }

        @Override
        public List<DeviceLayerKind> getLayerStackList()
        {
            return LinstorParsingUtils.asDeviceLayerKind(autoSelectFilter.layer_stack);
        }

        @Override
        public List<DeviceProviderKind> getProviderList()
        {
            return LinstorParsingUtils.asProviderKind(autoSelectFilter.provider_list);
        }

        @Override
        public Boolean getDisklessOnRemaining()
        {
            return autoSelectFilter.diskless_on_remaining;
        }
    }

    public static JsonGenTypes.SnapshotVolumeDefinition apiToSnapshotVolumeDefinition(
        SnapshotVolumeDefinitionApi snapshotVlmDfnApi
    )
    {
        JsonGenTypes.SnapshotVolumeDefinition snapshotVolumeDefinition =
            new JsonGenTypes.SnapshotVolumeDefinition();

        snapshotVolumeDefinition.volume_number = snapshotVlmDfnApi.getVolumeNr();
        snapshotVolumeDefinition.size_kib = snapshotVlmDfnApi.getSize();

        return snapshotVolumeDefinition;
    }

    public static JsonGenTypes.Snapshot apiToSnapshot(
        SnapshotDefinitionListItemApi snapshotDfnListItemApi
    )
    {
        JsonGenTypes.Snapshot snapshot = new JsonGenTypes.Snapshot();
        snapshot.name = snapshotDfnListItemApi.getSnapshotName();
        snapshot.resource_name = snapshotDfnListItemApi.getRscDfn().getResourceName();
        snapshot.nodes = snapshotDfnListItemApi.getNodeNames();
        snapshot.props = snapshotDfnListItemApi.getProps();
        snapshot.flags = FlagsHelper.toStringList(SnapshotDefinition.Flags.class, snapshotDfnListItemApi.getFlags());
        snapshot.volume_definitions = snapshotDfnListItemApi.getSnapshotVlmDfnList().stream()
            .map(Json::apiToSnapshotVolumeDefinition)
            .collect(Collectors.toList());
        snapshot.uuid = snapshotDfnListItemApi.getUuid().toString();
        return snapshot;
    }

    public static JsonGenTypes.ResourceConnection apiToResourceConnection(
        ResourceConnectionApi rscConnApi
    )
    {
        JsonGenTypes.ResourceConnection resourceConnection = new JsonGenTypes.ResourceConnection();
        resourceConnection.node_a = rscConnApi.getSourceNodeName();
        resourceConnection.node_b = rscConnApi.getTargetNodeName();
        resourceConnection.props = rscConnApi.getProps();
        resourceConnection.flags = FlagsHelper.toStringList(
            ResourceConnection.Flags.class, rscConnApi.getFlags()
        );
        resourceConnection.port = rscConnApi.getPort();
        return resourceConnection;
    }

    public static JsonGenTypes.Candidate pojoToCandidate(MaxVlmSizeCandidatePojo pojo)
    {
        JsonGenTypes.Candidate candidate = new JsonGenTypes.Candidate();
        candidate.storage_pool = pojo.getStorPoolDfnApi().getName();
        candidate.max_volume_size_kib = pojo.getMaxVlmSize();
        candidate.node_names = pojo.getNodeNames();
        candidate.all_thin = pojo.areAllThin();
        return candidate;
    }

    public static JsonGenTypes.MaxVolumeSizes pojoToMaxVolumeSizes(List<MaxVlmSizeCandidatePojo> candidatePojos)
    {
        JsonGenTypes.MaxVolumeSizes maxVolumeSizes = new JsonGenTypes.MaxVolumeSizes();
        maxVolumeSizes.candidates = candidatePojos.stream().map(Json::pojoToCandidate).collect(Collectors.toList());
        return maxVolumeSizes;
    }

    public static JsonGenTypes.KeyValueStore apiToKeyValueStore(com.linbit.linstor.core.apis.KvsApi kvsApi)
    {
        JsonGenTypes.KeyValueStore keyValueStore = new JsonGenTypes.KeyValueStore();
        keyValueStore.name = kvsApi.getName();
        keyValueStore.props = kvsApi.getProps();
        return keyValueStore;
    }

    public static JsonGenTypes.ResourceGroup apiToResourceGroup(
        ResourceGroupApi rscGrpApi
    )
    {
        JsonGenTypes.ResourceGroup rscGrp = new JsonGenTypes.ResourceGroup();
        rscGrp.name = rscGrpApi.getName();
        if (rscGrpApi.getDescription() != null)
        {
            rscGrp.description = rscGrpApi.getDescription();
        }
        rscGrp.props = rscGrpApi.getProps();
        rscGrp.uuid = rscGrpApi.getUuid().toString();

        AutoSelectFilterApi autoSelectApi = rscGrpApi.getAutoSelectFilter();
        if (autoSelectApi != null)
        {
            AutoSelectFilter auto_select_filter = new AutoSelectFilter();
            if (autoSelectApi.getReplicaCount() != null)
            {
                auto_select_filter.place_count = autoSelectApi.getReplicaCount();
            }
            auto_select_filter.storage_pool = autoSelectApi.getStorPoolNameStr();
            auto_select_filter.not_place_with_rsc = autoSelectApi.getDoNotPlaceWithRscList();
            auto_select_filter.not_place_with_rsc_regex = autoSelectApi.getDoNotPlaceWithRscRegex();
            auto_select_filter.replicas_on_same = autoSelectApi.getReplicasOnSameList();
            auto_select_filter.replicas_on_different = autoSelectApi.getReplicasOnDifferentList();
            auto_select_filter.layer_stack = autoSelectApi.getLayerStackList().stream()
                .map(Json::getLayerTypeString).collect(Collectors.toList());
            auto_select_filter.provider_list = autoSelectApi.getProviderList().stream()
                .map(Json::deviceProviderKindAsString).collect(Collectors.toList());
            auto_select_filter.diskless_on_remaining = autoSelectApi.getDisklessOnRemaining();

            rscGrp.select_filter = auto_select_filter;
        }
        return rscGrp;
    }

    public static JsonGenTypes.VolumeGroup apiToVolumeGroup(
        VolumeGroup.VlmGrpApi vlmGrpApi
    )
    {
        JsonGenTypes.VolumeGroup vlmGrp = new JsonGenTypes.VolumeGroup();
        vlmGrp.volume_number = vlmGrpApi.getVolumeNr();
        vlmGrp.props = vlmGrpApi.getProps();
        vlmGrp.uuid = vlmGrpApi.getUUID().toString();
        return vlmGrp;
    }
}
