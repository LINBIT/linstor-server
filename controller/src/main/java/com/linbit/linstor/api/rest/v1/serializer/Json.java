package com.linbit.linstor.api.rest.v1.serializer;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRc.RcEntry;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.interfaces.AutoSelectFilterApi;
import com.linbit.linstor.api.interfaces.RscDfnLayerDataApi;
import com.linbit.linstor.api.interfaces.RscLayerDataApi;
import com.linbit.linstor.api.interfaces.VlmDfnLayerDataApi;
import com.linbit.linstor.api.interfaces.VlmLayerDataApi;
import com.linbit.linstor.api.pojo.AutoSelectFilterPojo;
import com.linbit.linstor.api.pojo.BCacheRscPojo;
import com.linbit.linstor.api.pojo.CacheRscPojo;
import com.linbit.linstor.api.pojo.DrbdRscPojo;
import com.linbit.linstor.api.pojo.EbsRemotePojo;
import com.linbit.linstor.api.pojo.EffectivePropertiesPojo;
import com.linbit.linstor.api.pojo.EffectivePropertiesPojo.EffectivePropertyPojo;
import com.linbit.linstor.api.pojo.EffectivePropertiesPojo.PropPojo;
import com.linbit.linstor.api.pojo.ExternalFilePojo;
import com.linbit.linstor.api.pojo.LinstorRemotePojo;
import com.linbit.linstor.api.pojo.LuksRscPojo;
import com.linbit.linstor.api.pojo.MaxVlmSizeCandidatePojo;
import com.linbit.linstor.api.pojo.NetInterfacePojo;
import com.linbit.linstor.api.pojo.NvmeRscPojo;
import com.linbit.linstor.api.pojo.QueryAllSizeInfoRequestPojo;
import com.linbit.linstor.api.pojo.QueryAllSizeInfoResponsePojo;
import com.linbit.linstor.api.pojo.QueryAllSizeInfoResponsePojo.QueryAllSizeInfoResponseEntryPojo;
import com.linbit.linstor.api.pojo.QuerySizeInfoRequestPojo;
import com.linbit.linstor.api.pojo.QuerySizeInfoResponsePojo;
import com.linbit.linstor.api.pojo.RscPojo;
import com.linbit.linstor.api.pojo.S3RemotePojo;
import com.linbit.linstor.api.pojo.SchedulePojo;
import com.linbit.linstor.api.pojo.StorageRscPojo;
import com.linbit.linstor.api.pojo.VlmDfnPojo;
import com.linbit.linstor.api.pojo.WritecacheRscPojo;
import com.linbit.linstor.api.pojo.backups.BackupInfoPojo;
import com.linbit.linstor.api.pojo.backups.BackupInfoStorPoolPojo;
import com.linbit.linstor.api.pojo.backups.BackupInfoVlmPojo;
import com.linbit.linstor.api.pojo.backups.BackupNodeQueuesPojo;
import com.linbit.linstor.api.pojo.backups.BackupSnapQueuesPojo;
import com.linbit.linstor.api.pojo.backups.ScheduleDetailsPojo;
import com.linbit.linstor.api.pojo.backups.ScheduledRscsPojo;
import com.linbit.linstor.api.rest.v1.serializer.JsonGenTypes.AutoSelectFilter;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.core.apis.BackupApi;
import com.linbit.linstor.core.apis.BackupApi.BackupS3Api;
import com.linbit.linstor.core.apis.BackupApi.BackupVlmApi;
import com.linbit.linstor.core.apis.BackupApi.BackupVlmS3Api;
import com.linbit.linstor.core.apis.NetInterfaceApi;
import com.linbit.linstor.core.apis.NetInterfaceApi.StltConn;
import com.linbit.linstor.core.apis.NodeApi;
import com.linbit.linstor.core.apis.NodeConnectionApi;
import com.linbit.linstor.core.apis.ResourceApi;
import com.linbit.linstor.core.apis.ResourceConnectionApi;
import com.linbit.linstor.core.apis.ResourceDefinitionApi;
import com.linbit.linstor.core.apis.ResourceGroupApi;
import com.linbit.linstor.core.apis.SnapshotApi;
import com.linbit.linstor.core.apis.SnapshotDefinitionListItemApi;
import com.linbit.linstor.core.apis.SnapshotVolumeApi;
import com.linbit.linstor.core.apis.SnapshotVolumeDefinitionApi;
import com.linbit.linstor.core.apis.StorPoolApi;
import com.linbit.linstor.core.apis.StorPoolDefinitionApi;
import com.linbit.linstor.core.apis.VolumeApi;
import com.linbit.linstor.core.apis.VolumeDefinitionApi;
import com.linbit.linstor.core.apis.VolumeGroupApi;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.SharedStorPoolName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceConnection;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.SnapshotDefinition;
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
import com.linbit.linstor.storage.kinds.ExtTools;
import com.linbit.linstor.storage.kinds.ExtToolsInfo;
import com.linbit.utils.Base64;
import com.linbit.utils.Pair;
import com.linbit.utils.PairNonNull;

import java.nio.charset.StandardCharsets;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.stream.Collectors;

import com.google.common.net.UrlEscapers;

public class Json
{
    private static final int COMPAT_RD_PORT = -1;

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
        if (activeStltConn != null)
        {
            for (JsonGenTypes.NetInterface netInterface : netIfsList)
            {
                netInterface.is_active = netInterface.uuid.equalsIgnoreCase(activeStltConn.getUuid().toString()) ?
                    Boolean.TRUE :
                    Boolean.FALSE;
            }
        }

        nd.net_interfaces = netIfsList;
        nd.uuid = nodeApi.getUuid().toString();
        nd.resource_layers = nodeApi.getDeviceLayerKindNames();
        nd.storage_providers = nodeApi.getDeviceProviderKindNames();
        nd.unsupported_layers = nodeApi.getUnsupportedLayersWithReasons();
        nd.unsupported_providers = nodeApi.getUnsupportedProvidersWithReasons();
        nd.eviction_timestamp = nodeApi.getEvictionTimestamp();
        return nd;
    }

    public static JsonGenTypes.NetInterface apiToNetInterface(NetInterfaceApi netIfApi)
    {
        JsonGenTypes.NetInterface netif = new JsonGenTypes.NetInterface();
        netif.name = netIfApi.getName();
        netif.address = netIfApi.getAddress();
        @Nullable StltConn stltConn = netIfApi.getStltConn();
        if (stltConn != null)
        {
            netif.satellite_encryption_type = stltConn.getSatelliteConnectionEncryptionType();
            netif.satellite_port = stltConn.getSatelliteConnectionPort();
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

    public static JsonGenTypes.NodeConnection apiToNodeConnection(
        NodeConnectionApi nodeConnApi
    )
    {
        JsonGenTypes.NodeConnection nodeCon = new JsonGenTypes.NodeConnection();
        String localNodeName = nodeConnApi.getLocalNodeName();
        String otherNodeName = nodeConnApi.getOtherNodeApi().getName();
        if (localNodeName.compareTo(otherNodeName) < 0)
        {
            nodeCon.node_a = localNodeName;
            nodeCon.node_b = otherNodeName;
        }
        else
        {
            nodeCon.node_a = otherNodeName;
            nodeCon.node_b = localNodeName;
        }
        nodeCon.props = nodeConnApi.getProps();
        return nodeCon;
    }

    public static JsonGenTypes.StoragePoolDefinition storPoolDfnApiToStoragePoolDefinition(
        StorPoolDefinitionApi apiData
    )
    {
        JsonGenTypes.StoragePoolDefinition storPoolDfn = new JsonGenTypes.StoragePoolDefinition();
        storPoolDfn.storage_pool_name = apiData.getName();
        storPoolDfn.props = apiData.getProps();
        return storPoolDfn;
    }

    public static List<JsonGenTypes.ApiCallRc> apiCallRcToJson(ApiCallRc apiCallRc)
    {
        List<JsonGenTypes.ApiCallRc> json = new ArrayList<>();
        for (RcEntry apiCallRcEntry : apiCallRc)
        {
            JsonGenTypes.ApiCallRc jsonApiCallRc = new JsonGenTypes.ApiCallRc();
            jsonApiCallRc.message = apiCallRcEntry.getMessage();
            jsonApiCallRc.cause = apiCallRcEntry.getCause();
            jsonApiCallRc.correction = apiCallRcEntry.getCorrection();
            jsonApiCallRc.details = apiCallRcEntry.getDetails();
            jsonApiCallRc.error_report_ids = new ArrayList<>(apiCallRcEntry.getErrorIds());
            jsonApiCallRc.obj_refs = apiCallRcEntry.getObjRefs();
            jsonApiCallRc.ret_code = apiCallRcEntry.getReturnCode();
            jsonApiCallRc.created_at = apiCallRcEntry.getDateTime().format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
            json.add(jsonApiCallRc);
        }
        return json;
    }

    public static ApiCallRcImpl jsonToApiCallRc(JsonGenTypes.ApiCallRc... jsonRcs)
    {
        ApiCallRcImpl ret = new ApiCallRcImpl();
        for (JsonGenTypes.ApiCallRc json : jsonRcs)
        {
            ret.addEntry(
                ApiCallRcImpl.entryBuilder(json.ret_code, json.message)
                    .addAllErrorIds(json.error_report_ids)
                    .putAllObjRefs(json.obj_refs)
                    .setCause(json.cause)
                    .setCorrection(json.correction)
                    .setDetails(json.details)
                    .build()
            );
        }
        return ret;
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
        storPoolData.supports_snapshots = storPoolApi.getDeviceProviderKind().isSnapshotSupported();
        storPoolData.external_locking = storPoolApi.isExternalLocking();

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
        if (drbdRscDfnPojo.getPort() == null)
        {
            // old clients / apipy still require a TCP port here.
            drbdResourceDefinitionLayer.port = COMPAT_RD_PORT;
        }
        else
        {
            drbdResourceDefinitionLayer.port = drbdRscDfnPojo.getPort();
        }
        drbdResourceDefinitionLayer.transport_type = drbdRscDfnPojo.getTransportType();
        drbdResourceDefinitionLayer.secret = drbdRscDfnPojo.getSecret();
        drbdResourceDefinitionLayer.down = drbdRscDfnPojo.isDown();
        return drbdResourceDefinitionLayer;
    }

    public static JsonGenTypes.ResourceDefinition apiToResourceDefinition(
        ResourceDefinitionApi rscDfnApi,
        boolean withVlmDfns
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

        if (withVlmDfns)
        {
            rscDfn.volume_definitions = new ArrayList<>();
            for (VolumeDefinitionApi vlmDfnApi : rscDfnApi.getVlmDfnList())
            {
                rscDfn.volume_definitions.add(apiToVolumeDefinition(vlmDfnApi));
            }
        }

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
                    case WRITECACHE:
                    case CACHE:
                    case BCACHE:
                    default:
                        throw new ImplementationError("Not implemented kind case");
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
        VolumeDefinitionApi vlmDfnApi
    )
    {
        JsonGenTypes.VolumeDefinition vlmDfn = new JsonGenTypes.VolumeDefinition();
        vlmDfn.volume_number = vlmDfnApi.getVolumeNr();
        vlmDfn.size_kib = vlmDfnApi.getSize();
        vlmDfn.props = vlmDfnApi.getProps();
        vlmDfn.flags = FlagsHelper.toStringList(
            VolumeDefinition.Flags.class,
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
                    case WRITECACHE:
                    case CACHE:
                    case BCACHE:
                    default:
                        throw new ImplementationError("Not implemented Kind case");
                }
            }
            vlmDfn.layer_data.add(volumeDefinitionLayerData);
        }
        return vlmDfn;
    }

    public static VolumeDefinitionApi volumeDefinitionToApi(
        JsonGenTypes.VolumeDefinition vlmDfn
    )
    {
        ArrayList<Pair<String, VlmDfnLayerDataApi>> layerData = new ArrayList<>(); // empty will be ignored
        return new VlmDfnPojo(
            null,
            vlmDfn.volume_number,
            vlmDfn.size_kib,
            FlagsHelper.fromStringList(VolumeDefinition.Flags.class, vlmDfn.flags),
            vlmDfn.props,
            layerData
        );
    }

    public static JsonGenTypes.DrbdResource pojoToDrbdResource(DrbdRscPojo drbdRscPojo)
    {
        JsonGenTypes.DrbdResource drbdResource = new JsonGenTypes.DrbdResource();
        drbdResource.drbd_resource_definition = pojoToDrbdRscDfnLayer(drbdRscPojo.getDrbdRscDfn());
        drbdResource.node_id = drbdRscPojo.getNodeId();
        drbdResource.tcp_ports = new ArrayList<>(drbdRscPojo.getPorts());
        drbdResource.peer_slots = (int) drbdRscPojo.getPeerSlots();
        drbdResource.al_stripes = drbdRscPojo.getAlStripes();
        drbdResource.al_size = drbdRscPojo.getAlStripeSize();
        drbdResource.flags = FlagsHelper.toStringList(DrbdRscObject.DrbdRscFlags.class, drbdRscPojo.getFlags());
        drbdResource.drbd_volumes = drbdRscPojo.getVolumeList().stream()
            .map(Json::pojoToDrbdVolume)
            .collect(Collectors.toList());
        drbdResource.promotion_score = drbdRscPojo.getPromotionScore();
        drbdResource.may_promote = drbdRscPojo.mayPromote();
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

    public static JsonGenTypes.WritecacheResource pojoToWritecacheResource(WritecacheRscPojo writecacheRscPojo)
    {
        JsonGenTypes.WritecacheResource writecacheResource = new JsonGenTypes.WritecacheResource();
        writecacheResource.writecache_volumes = writecacheRscPojo.getVolumeList().stream()
            .map(Json::pojoToWritecacheVolume)
            .collect(Collectors.toList());
        return writecacheResource;
    }

    public static JsonGenTypes.CacheResource pojoToCacheResource(CacheRscPojo cacheRscPojo)
    {
        JsonGenTypes.CacheResource cacheResource = new JsonGenTypes.CacheResource();
        cacheResource.cache_volumes = cacheRscPojo.getVolumeList().stream()
            .map(Json::pojoToCacheVolume)
            .collect(Collectors.toList());
        return cacheResource;
    }

    public static JsonGenTypes.BCacheResource pojoToBCacheResource(BCacheRscPojo bcacheRscPojo)
    {
        JsonGenTypes.BCacheResource bcacheResource = new JsonGenTypes.BCacheResource();
        bcacheResource.bcache_volumes = bcacheRscPojo.getVolumeList().stream()
            .map(Json::pojoToBCacheVolume)
            .collect(Collectors.toList());
        return bcacheResource;
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
            case WRITECACHE:
                WritecacheRscPojo writecacheRscPojo = (WritecacheRscPojo) rscLayerDataApi;
                resourceLayer.writecache = pojoToWritecacheResource(writecacheRscPojo);
                break;
            case CACHE:
                CacheRscPojo cacheRscPojo = (CacheRscPojo) rscLayerDataApi;
                resourceLayer.cache = pojoToCacheResource(cacheRscPojo);
                break;
            case BCACHE:
                BCacheRscPojo bcacheRscPojo = (BCacheRscPojo) rscLayerDataApi;
                resourceLayer.bcache = pojoToBCacheResource(bcacheRscPojo);
                break;
            default:
                break;
        }
        return resourceLayer;
    }

    public static JsonGenTypes.VolumeState apiToVolumeState(
            Map<NodeName, SatelliteState> satelliteStates,
            String nodeName,
            String rscName,
            int volumeNumber)
    {
        JsonGenTypes.VolumeState vlmState = null;
        try
        {
            final ResourceName rscNameRes = new ResourceName(rscName);
            final NodeName linNodeName = new NodeName(nodeName);
            if (satelliteStates.containsKey(linNodeName) &&
                satelliteStates.get(linNodeName)
                    .getResourceStates().containsKey(rscNameRes))
            {
                SatelliteResourceState satResState = satelliteStates
                    .get(linNodeName)
                    .getResourceStates()
                    .get(rscNameRes);

                VolumeNumber vlmNumber = new VolumeNumber(volumeNumber);
                if (satResState.getVolumeStates().containsKey(vlmNumber))
                {
                    vlmState = new JsonGenTypes.VolumeState();
                    SatelliteVolumeState satVlmState = satResState.getVolumeStates().get(vlmNumber);
                    vlmState.disk_state = satVlmState.getDiskState();
                    vlmState.replication_states = new HashMap<>();
                    for (var entry : satVlmState.getReplicationStateMap().entrySet())
                    {
                        String peerName = entry.getKey().displayValue;
                        JsonGenTypes.ReplicationState replState = new JsonGenTypes.ReplicationState();
                        replState.replication_state = entry.getValue().toString();
                        if (satVlmState.getDonePercentageMap().containsKey(entry.getKey()))
                        {
                            replState.done_percentage =
                                Double.valueOf(satVlmState.getDonePercentageMap().get(entry.getKey()));
                        }
                        vlmState.replication_states.put(peerName, replState);
                    }
                }
            }
        }
        catch (InvalidNameException | ValueOutOfRangeException ignored)
        {
        }
        return vlmState;
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
        rsc.effective_props = apiToEffectiveProps(rscApi.getEffectivePropsPojo());
        rscApi.getCreateTimestamp().ifPresent(d -> rsc.create_timestamp = d.getTime());

        rsc.shared_name = getSharedName(rscApi);

        if (withVolumes)
        {
            rsc.volumes = rscApi.getVlmList().stream().map(
                vlmApi ->
                {
                    JsonGenTypes.Volume vlmData = Json.apiToVolume(vlmApi);
                    vlmData.state = apiToVolumeState(satelliteStates, rsc.node_name, rsc.name, vlmApi.getVlmNr());
                    return vlmData;
                }
            ).collect(Collectors.toList());
        }

        try
        {
            final ResourceName rscNameRes = new ResourceName(rscApi.getName());
            final NodeName linNodeName = new NodeName(rscApi.getNodeName());
            if (satelliteStates.containsKey(linNodeName) &&
                satelliteStates.get(linNodeName)
                    .getResourceStates().containsKey(rscNameRes))
            {
                SatelliteResourceState satResState = satelliteStates
                    .get(linNodeName).getResourceStates().get(rscNameRes);
                rsc.state = new JsonGenTypes.ResourceState();
                rsc.state.in_use = satResState.isInUse();

                if (rscApi.getLayerData().getLayerKind() == DeviceLayerKind.DRBD)
                {
                    rsc.layer_object.drbd.connections = new HashMap<>();
                    for (Map.Entry<NodeName, String> entry : satResState.getConnectionStates()
                        .getOrDefault(linNodeName, new HashMap<>()).entrySet())
                    {
                        JsonGenTypes.DrbdConnection con = new JsonGenTypes.DrbdConnection();
                        con.connected = entry.getValue().equals("Connected");
                        con.message = entry.getValue();
                        rsc.layer_object.drbd.connections.put(entry.getKey().displayValue, con);
                    }
                }
            }
        }
        catch (InvalidNameException ignored)
        {
        }
        return rsc;
    }

    private static Map<String, JsonGenTypes.EffectivePropertiesMapValue> apiToEffectiveProps(
        EffectivePropertiesPojo pojo
    )
    {
        Map<String, JsonGenTypes.EffectivePropertiesMapValue> jsonMap;
        if (pojo != null)
        {
            jsonMap = new TreeMap<>();
            for (Entry<String, EffectivePropertyPojo> pojoEntry : pojo.properties.entrySet())
            {
                JsonGenTypes.EffectivePropertiesMapValue jsonValue = new JsonGenTypes.EffectivePropertiesMapValue();
                EffectivePropertyPojo pojoValue = pojoEntry.getValue();
                jsonValue.descr = pojoValue.active.descr;
                jsonValue.type = pojoValue.active.type;
                jsonValue.value = pojoValue.active.value;
                jsonValue.other = new ArrayList<>();
                for (PropPojo pojoProp : pojoValue.other)
                {
                    JsonGenTypes.PropertyWithDescription jsonProp = new JsonGenTypes.PropertyWithDescription();
                    jsonProp.descr = pojoProp.descr;
                    jsonProp.type = pojoProp.type;
                    jsonProp.value = pojoProp.value;
                    jsonValue.other.add(jsonProp);
                }
                jsonMap.put(pojoEntry.getKey(), jsonValue);
            }
        }
        else
        {
            jsonMap = null;
        }
        return jsonMap;
    }

    private static @Nullable String getSharedName(ResourceApi rscApiRef)
    {
        LinkedList<RscLayerDataApi> toExplore = new LinkedList<>();
        toExplore.add(rscApiRef.getLayerData());
        String sharedName = null;
        while (!toExplore.isEmpty())
        {
            RscLayerDataApi cur = toExplore.removeFirst();
            if (cur.getLayerKind().equals(DeviceLayerKind.STORAGE))
            {
                if (!cur.getVolumeList().isEmpty())
                {
                    String freeSpaceManagerName = cur.getVolumeList().get(0).getStorPoolApi().getFreeSpaceManagerName();
                    if (SharedStorPoolName.isShared(freeSpaceManagerName))
                    {
                        sharedName = freeSpaceManagerName;
                    }
                }
                // else no volumes => no sharedName, or freeSpaceManagerName is not shared => not a shared name
                break;
            }
            toExplore.addAll(cur.getChildren());
        }
        return sharedName;
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
        drbdVolume.backing_device = drbdVlmPojo.getDataDevice();
        drbdVolume.meta_disk = drbdVlmPojo.getMetaDevice();
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
        luksVolume.backing_device = luksVlmPojo.getDataDevice();
        luksVolume.allocated_size_kib = luksVlmPojo.getAllocatedSize();
        luksVolume.usable_size_kib = luksVlmPojo.getUsableSize();
        luksVolume.opened = luksVlmPojo.isOpen();
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

    public static JsonGenTypes.WritecacheVolume pojoToWritecacheVolume(
        WritecacheRscPojo.WritecacheVlmPojo writecacheVlmPojo
    )
    {
        JsonGenTypes.WritecacheVolume writecacheVolume = new JsonGenTypes.WritecacheVolume();
        writecacheVolume.volume_number = writecacheVlmPojo.getVlmNr();
        writecacheVolume.device_path = writecacheVlmPojo.getDevicePath();
        writecacheVolume.device_path_cache = writecacheVlmPojo.getCacheDevice();
        writecacheVolume.allocated_size_kib = writecacheVlmPojo.getAllocatedSize();
        writecacheVolume.usable_size_kib = writecacheVlmPojo.getUsableSize();
        writecacheVolume.disk_state = writecacheVlmPojo.getDiskState();
        return writecacheVolume;
    }

    public static JsonGenTypes.CacheVolume pojoToCacheVolume(
        CacheRscPojo.CacheVlmPojo cacheVlmPojo
    )
    {
        JsonGenTypes.CacheVolume cacheVolume = new JsonGenTypes.CacheVolume();
        cacheVolume.volume_number = cacheVlmPojo.getVlmNr();
        cacheVolume.device_path = cacheVlmPojo.getDevicePath();
        cacheVolume.device_path_cache = cacheVlmPojo.getCacheDevice();
        cacheVolume.allocated_size_kib = cacheVlmPojo.getAllocatedSize();
        cacheVolume.usable_size_kib = cacheVlmPojo.getUsableSize();
        cacheVolume.disk_state = cacheVlmPojo.getDiskState();
        return cacheVolume;
    }

    public static JsonGenTypes.BCacheVolume pojoToBCacheVolume(
        BCacheRscPojo.BCacheVlmPojo bcacheVlmPojo
    )
    {
        JsonGenTypes.BCacheVolume cacheVolume = new JsonGenTypes.BCacheVolume();
        cacheVolume.volume_number = bcacheVlmPojo.getVlmNr();
        cacheVolume.device_path = bcacheVlmPojo.getDevicePath();
        cacheVolume.device_path_cache = bcacheVlmPojo.getCacheDevice();
        cacheVolume.allocated_size_kib = bcacheVlmPojo.getAllocatedSize();
        cacheVolume.usable_size_kib = bcacheVlmPojo.getUsableSize();
        cacheVolume.disk_state = bcacheVlmPojo.getDiskState();
        return cacheVolume;
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

    public static JsonGenTypes.Volume apiToVolume(VolumeApi vlmApi)
    {
        JsonGenTypes.Volume volume = new JsonGenTypes.Volume();
        volume.volume_number = vlmApi.getVlmNr();
        volume.storage_pool_name = vlmApi.getStorPoolName();
        volume.provider_kind = deviceProviderKindAsString(vlmApi.getStorPoolDeviceProviderKind());

        volume.device_path = vlmApi.getDevicePath();
        volume.allocated_size_kib = vlmApi.getAllocatedSize().orElse(null);

        volume.props = vlmApi.getVlmProps();
        volume.flags = FlagsHelper.toStringList(Volume.Flags.class, vlmApi.getFlags());
        volume.uuid = vlmApi.getVlmUuid().toString();
        volume.reports = apiCallRcToJson(vlmApi.getReports());

        volume.layer_data_list = new ArrayList<>();

        for (PairNonNull<String, VlmLayerDataApi> layerData : vlmApi.getVlmLayerData())
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
                case WRITECACHE:
                    WritecacheRscPojo.WritecacheVlmPojo writecacheVlmPojo = (WritecacheRscPojo.WritecacheVlmPojo) layerData.objB;
                    volumeLayerData.data = pojoToWritecacheVolume(writecacheVlmPojo);
                    break;
                case CACHE:
                    CacheRscPojo.CacheVlmPojo cacheVlmPojo = (CacheRscPojo.CacheVlmPojo) layerData.objB;
                    volumeLayerData.data = pojoToCacheVolume(cacheVlmPojo);
                    break;
                case BCACHE:
                    BCacheRscPojo.BCacheVlmPojo bcacheVlmPojo = (BCacheRscPojo.BCacheVlmPojo) layerData.objB;
                    volumeLayerData.data = pojoToBCacheVolume(bcacheVlmPojo);
                    break;
                default:
                    break;
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
            if (autoSelectFilterRef == null)
            {
                autoSelectFilter = new JsonGenTypes.AutoSelectFilter();
            }
            else
            {
                autoSelectFilter = autoSelectFilterRef;
            }
            if (
            (autoSelectFilter.storage_pool_list == null || autoSelectFilter.storage_pool_list.isEmpty()) &&
                autoSelectFilter.storage_pool != null
            )
            {
                autoSelectFilter.storage_pool_list = Collections.singletonList(autoSelectFilter.storage_pool);
            }
        }

        @Override
        public Integer getReplicaCount()
        {
            return autoSelectFilter.place_count;
        }

        @Override
        public Integer getAdditionalReplicaCount()
        {
            return autoSelectFilter.additional_place_count;
        }

        @Override
        public List<String> getNodeNameList()
        {
            return autoSelectFilter.node_name_list;
        }

        @Override
        public List<String> getStorPoolNameList()
        {
            return autoSelectFilter.storage_pool_list;
        }

        @Override
        public List<String> getStorPoolDisklessNameList()
        {
            return autoSelectFilter.storage_pool_diskless_list;
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
        public Map<String, Integer> getXReplicasOnDifferentMap()
        {
            return autoSelectFilter.x_replicas_on_different_map;
        }

        @Override
        public @Nullable List<DeviceLayerKind> getLayerStackList()
        {
            return autoSelectFilter.layer_stack != null ?
                LinstorParsingUtils.asDeviceLayerKind(autoSelectFilter.layer_stack) : null;
        }

        @Override
        public @Nullable List<DeviceProviderKind> getProviderList()
        {
            return autoSelectFilter.provider_list != null ?
                LinstorParsingUtils.asProviderKind(autoSelectFilter.provider_list) : null;
        }

        @Override
        public Boolean getDisklessOnRemaining()
        {
            return autoSelectFilter.diskless_on_remaining;
        }

        @Override
        public @Nullable List<String> skipAlreadyPlacedOnNodeNamesCheck()
        {
            return null;
        }

        @Override
        public @Nullable Boolean skipAlreadyPlacedOnAllNodeCheck()
        {
            return null;
        }

        @Override
        public String getDisklessType()
        {
            return autoSelectFilter.diskless_type;
        }

        @Override
        public @Nullable Map<ExtTools, ExtToolsInfo.Version> getRequiredExtTools()
        {
            return null;
        }

        @Override
        public @Nullable Integer getDrbdPortCount()
        {
            return autoSelectFilter.port_count;
        }
    }

    public static JsonGenTypes.SnapshotVolumeDefinition apiToSnapshotVolumeDefinition(
        SnapshotVolumeDefinitionApi snapshotVlmDfnApi
    )
    {
        JsonGenTypes.SnapshotVolumeDefinition jsonSnapVlmDfn =
            new JsonGenTypes.SnapshotVolumeDefinition();

        jsonSnapVlmDfn.volume_number = snapshotVlmDfnApi.getVolumeNr();
        jsonSnapVlmDfn.size_kib = snapshotVlmDfnApi.getSize();
        jsonSnapVlmDfn.snapshot_volume_definition_props = snapshotVlmDfnApi.getSnapVlmDfnPropsMap();
        jsonSnapVlmDfn.volume_definition_props = snapshotVlmDfnApi.getVlmDfnPropsMap();

        return jsonSnapVlmDfn;
    }

    public static JsonGenTypes.SnapshotNode apiToSnapshotNode(SnapshotApi snapshotApi)
    {
        JsonGenTypes.SnapshotNode jsonSnap = new JsonGenTypes.SnapshotNode();
        jsonSnap.node_name = snapshotApi.getNodeName();
        jsonSnap.snapshot_name = snapshotApi.getSnaphotDfn().getSnapshotName();
        jsonSnap.flags = FlagsHelper.toStringList(Snapshot.Flags.class, snapshotApi.getFlags());
        jsonSnap.uuid = snapshotApi.getSnapshotUuid().toString();
        jsonSnap.snapshot_props = snapshotApi.getSnapPropsMap();
        jsonSnap.resource_props = snapshotApi.getRscPropsMap();
        snapshotApi.getCreateTimestamp().ifPresent(d -> jsonSnap.create_timestamp = d.getTime());
        jsonSnap.snapshot_volumes = snapshotApi.getSnapshotVlmList()
            .stream()
            .map(Json::apiToSnapshotVolumeNode)
            .collect(Collectors.toList());
        return jsonSnap;
    }

    public static JsonGenTypes.SnapshotVolumeNode apiToSnapshotVolumeNode(SnapshotVolumeApi snapshotVolumeApi)
    {
        JsonGenTypes.SnapshotVolumeNode nodeSnapVlm = new JsonGenTypes.SnapshotVolumeNode();
        nodeSnapVlm.uuid = snapshotVolumeApi.getSnapshotVlmUuid().toString();
        nodeSnapVlm.vlm_nr = snapshotVolumeApi.getSnapshotVlmNr();
        nodeSnapVlm.state = snapshotVolumeApi.getState();

        nodeSnapVlm.snapshot_volume_props = snapshotVolumeApi.getSnapVlmPropsMap();
        nodeSnapVlm.volume_props = snapshotVolumeApi.getVlmPropsMap();

        // deprecated / compat
        nodeSnapVlm.props = new TreeMap<>(nodeSnapVlm.volume_props);
        nodeSnapVlm.props.putAll(nodeSnapVlm.snapshot_volume_props);

        return nodeSnapVlm;
    }

    public static JsonGenTypes.Snapshot apiToSnapshot(
        SnapshotDefinitionListItemApi snapshotDfnListItemApi
    )
    {
        JsonGenTypes.Snapshot snapshot = new JsonGenTypes.Snapshot();
        snapshot.name = snapshotDfnListItemApi.getSnapshotName();
        snapshot.resource_name = snapshotDfnListItemApi.getRscDfn().getResourceName();
        snapshot.nodes = snapshotDfnListItemApi.getNodeNames();
        snapshot.snapshot_definition_props = snapshotDfnListItemApi.getSnapDfnProps();
        snapshot.resource_definition_props = snapshotDfnListItemApi.getRscDfnProps();
        snapshot.flags = FlagsHelper.toStringList(SnapshotDefinition.Flags.class, snapshotDfnListItemApi.getFlags());
        snapshot.volume_definitions = snapshotDfnListItemApi.getSnapshotVlmDfnList().stream()
            .map(Json::apiToSnapshotVolumeDefinition)
            .collect(Collectors.toList());
        snapshot.uuid = snapshotDfnListItemApi.getUuid().toString();
        snapshot.snapshots = snapshotDfnListItemApi.getSnapshots().stream()
            .map(Json::apiToSnapshotNode)
            .collect(Collectors.toList());

        // deprecated / compat
        snapshot.props = new TreeMap<>(snapshot.resource_definition_props);
        snapshot.props.putAll(snapshot.snapshot_definition_props);

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
        resourceConnection.drbd_proxy_port_a = rscConnApi.getDrbdProxyPortSource();
        resourceConnection.drbd_proxy_port_b = rscConnApi.getDrbdProxyPortTarget();
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
            if (autoSelectApi.getStorPoolNameList() != null && autoSelectApi.getStorPoolNameList().size() == 1)
            {
                auto_select_filter.storage_pool = autoSelectApi.getStorPoolNameList().get(0);
            }
            auto_select_filter.storage_pool_list = autoSelectApi.getStorPoolNameList();
            auto_select_filter.not_place_with_rsc = autoSelectApi.getDoNotPlaceWithRscList();
            auto_select_filter.not_place_with_rsc_regex = autoSelectApi.getDoNotPlaceWithRscRegex();
            auto_select_filter.replicas_on_same = autoSelectApi.getReplicasOnSameList();
            auto_select_filter.replicas_on_different = autoSelectApi.getReplicasOnDifferentList();
            auto_select_filter.x_replicas_on_different_map = autoSelectApi.getXReplicasOnDifferentMap();
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
        VolumeGroupApi vlmGrpApi
    )
    {
        JsonGenTypes.VolumeGroup vlmGrp = new JsonGenTypes.VolumeGroup();
        vlmGrp.volume_number = vlmGrpApi.getVolumeNr();
        vlmGrp.props = vlmGrpApi.getProps();
        vlmGrp.uuid = vlmGrpApi.getUUID().toString();
        vlmGrp.flags = FlagsHelper.toStringList(VolumeGroup.Flags.class, vlmGrpApi.getFlags());
        return vlmGrp;
    }

    public static JsonGenTypes.ExternalFile apiToExternalFile(
        ExternalFilePojo pojo,
        boolean includeContent
    )
    {
        JsonGenTypes.ExternalFile json = new JsonGenTypes.ExternalFile();
        json.path = pojo.getFileName();
        if (includeContent)
        {
            json.content = Base64.encode(pojo.getContent());
        }
        return json;
    }

    public static JsonGenTypes.ExtFileCheckResult apiToExtFileCheckResult(boolean allowed)
    {
        JsonGenTypes.ExtFileCheckResult json = new JsonGenTypes.ExtFileCheckResult();
        json.allowed = allowed;
        return json;
    }

    public static JsonGenTypes.ResourceDefinitionCloneStarted resourceDefCloneStarted(
        String srcRscName,
        String clonedName,
        ApiCallRc messages
    )
    {
        JsonGenTypes.ResourceDefinitionCloneStarted response = new JsonGenTypes.ResourceDefinitionCloneStarted();
        response.location = String.format(
            "/v1/resource-definitions/%s/clone/%s",
            srcRscName,
            UrlEscapers.urlPathSegmentEscaper().escape(clonedName));
        response.source_name = srcRscName;
        response.clone_name = clonedName;
        response.messages = Json.apiCallRcToJson(messages);
        return response;
    }

    public static Map<String, JsonGenTypes.Backup> apiToBackup(Map<String, BackupApi> backups)
    {
        Map<String, JsonGenTypes.Backup> jsonBackups = new TreeMap<>();
        for (BackupApi backup : backups.values())
        {
            JsonGenTypes.Backup jsonBackup = new JsonGenTypes.Backup();

            jsonBackup.id = backup.getId();
            jsonBackup.start_time = backup.getStartTime();
            jsonBackup.start_timestamp = backup.getStartTimestamp();
            jsonBackup.finished_time = backup.getFinishedTime();
            jsonBackup.finished_timestamp = backup.getFinishedTimestamp();
            jsonBackup.origin_rsc = backup.getResourceName();
            jsonBackup.origin_snap = backup.getSnapKey();
            jsonBackup.origin_node = backup.getOriginNodeName();
            jsonBackup.success = backup.successful();
            jsonBackup.shipping = backup.isShipping();
            jsonBackup.restorable = backup.isRestoreable();

            jsonBackup.vlms = new ArrayList<>();
            for (BackupVlmApi backupVlmApi : backup.getVlms().values())
            {
                JsonGenTypes.BackupVolumes backupVlm = new JsonGenTypes.BackupVolumes();
                backupVlm.vlm_nr = backupVlmApi.getVlmNr();
                backupVlm.finished_time = backupVlmApi.getFinishedTime();
                backupVlm.finished_timestamp = backupVlmApi.getFinishedTimestamp();
                BackupVlmS3Api s3VlmApi = backupVlmApi.getS3();
                if (s3VlmApi != null)
                {
                    backupVlm.s3 = new JsonGenTypes.BackupVolumesS3();
                    backupVlm.s3.key = s3VlmApi.getS3Key();
                }
                jsonBackup.vlms.add(backupVlm);
            }

            BackupS3Api s3Api = backup.getS3();
            if (s3Api != null)
            {
                jsonBackup.s3 = new JsonGenTypes.BackupS3();
                jsonBackup.s3.meta_name = s3Api.getMetaName();
            }

            String basedOn = backup.getBasedOnId();
            if (basedOn != null)
            {
                jsonBackup.based_on_id = basedOn;
            }
            jsonBackups.put(backup.getId(), jsonBackup);
        }
        return jsonBackups;
    }

    public static JsonGenTypes.BackupInfo apiToBackupInfo(BackupInfoPojo pojo)
    {
        JsonGenTypes.BackupInfo json = new JsonGenTypes.BackupInfo();
        json.rsc = pojo.getRscName();
        json.snap = pojo.getSnapName();
        json.full = pojo.getFullBackupName();
        json.latest = pojo.getLatestBackupName();
        json.count = pojo.getBackupCount();
        json.dl_size_kib = pojo.getDlSizeKib();
        json.alloc_size_kib = pojo.getAllocSizeKib();
        json.storpools = new ArrayList<>();
        for (BackupInfoStorPoolPojo storPoolPojo : pojo.getStorpools())
        {
            JsonGenTypes.BackupInfoStorPool storPoolJson = new JsonGenTypes.BackupInfoStorPool();
            storPoolJson.name = storPoolPojo.getStorPoolName();
            storPoolJson.provider_kind = storPoolPojo.getProviderKind().name();
            storPoolJson.target_name = storPoolPojo.getTargetStorPoolName();
            storPoolJson.remaining_space_kib = storPoolPojo.getRemainingSpaceKib();
            storPoolJson.vlms = new ArrayList<>();
            for (BackupInfoVlmPojo vlmPojo : storPoolPojo.getVlms())
            {
                JsonGenTypes.BackupInfoVolume vlmJson = new JsonGenTypes.BackupInfoVolume();
                vlmJson.name = vlmPojo.getBackupVlmName();
                vlmJson.layer_type = vlmPojo.getLayerType().name();
                vlmJson.dl_size_kib = vlmPojo.getDlSizeKib();
                vlmJson.alloc_size_kib = vlmPojo.getAllocSizeKib();
                vlmJson.usable_size_kib = vlmPojo.getUseSizeKib();
                storPoolJson.vlms.add(vlmJson);
            }
            json.storpools.add(storPoolJson);
        }
        return json;
    }

    public static JsonGenTypes.S3Remote apiToS3Remote(S3RemotePojo pojo)
    {
        JsonGenTypes.S3Remote json = new JsonGenTypes.S3Remote();
        json.remote_name = pojo.getRemoteName();
        json.endpoint = pojo.getEndpoint();
        json.bucket = pojo.getBucket();
        json.region = pojo.getRegion();

        return json;
    }

    public static JsonGenTypes.LinstorRemote apiToLinstorRemote(LinstorRemotePojo pojo)
    {
        JsonGenTypes.LinstorRemote json = new JsonGenTypes.LinstorRemote();
        json.remote_name = pojo.getRemoteName();
        json.url = pojo.getUrl();
        // encrypted passphrase intentionally not serialized

        return json;
    }

    public static JsonGenTypes.EbsRemote apiToEbsRemote(EbsRemotePojo pojo)
    {
        JsonGenTypes.EbsRemote json = new JsonGenTypes.EbsRemote();
        json.remote_name = pojo.getRemoteName();
        json.endpoint = pojo.getUrl();
        json.region = pojo.getRegion();
        json.availability_zone = pojo.getAvailabilityZone();
        // encrypted access- and secret-key intentionally not serialized

        return json;
    }

    public static JsonGenTypes.Schedule apiToSchedule(SchedulePojo pojo)
    {
        JsonGenTypes.Schedule json = new JsonGenTypes.Schedule();
        json.schedule_name = pojo.getScheduleName();
        json.full_cron = pojo.getFullCron();
        json.inc_cron = pojo.getIncCron();
        json.keep_local = pojo.getKeepLocal();
        json.keep_remote = pojo.getKeepRemote();
        json.on_failure = pojo.getOnFailure();
        json.max_retries = pojo.getMaxRetries();

        return json;
    }

    public static JsonGenTypes.ScheduledRscs apiToScheduledRscs(ScheduledRscsPojo pojo)
    {
        JsonGenTypes.ScheduledRscs json = new JsonGenTypes.ScheduledRscs();
        json.rsc_name = pojo.rsc_name;
        json.remote_name = pojo.remote;
        json.schedule_name = pojo.schedule;
        json.reason = pojo.reason;
        json.last_snap_time = pojo.last_snap_time;
        json.last_snap_inc = pojo.last_snap_inc;
        json.next_exec_time = pojo.next_exec_time;
        json.next_exec_inc = pojo.next_exec_inc;
        json.next_planned_full = pojo.next_planned_full;
        json.next_planned_inc = pojo.next_planned_inc;
        return json;
    }

    public static JsonGenTypes.ScheduleDetails apiToScheduleDetails(ScheduleDetailsPojo pojo)
    {
        JsonGenTypes.ScheduleDetails json = new JsonGenTypes.ScheduleDetails();
        json.schedule_name = pojo.getSchedule();
        json.remote_name = pojo.getRemote();
        json.rsc_dfn = pojo.getRscDfn();
        json.rsc_grp = pojo.getRscGrp();
        json.ctrl = pojo.getCtrl();
        return json;
    }

    public static JsonGenTypes.SnapQueue apiToSnapQueues(BackupSnapQueuesPojo pojo)
    {
        JsonGenTypes.SnapQueue json = new JsonGenTypes.SnapQueue();
        json.snapshot_name = pojo.getSnapshotName();
        json.resource_name = pojo.getResourceName();
        json.remote_name = pojo.getRemoteName();
        json.incremental = pojo.isIncremental();
        json.based_on = pojo.getBasedOn();
        json.pref_node = pojo.getPrefNode();
        json.start_timestamp = pojo.getStartTimestamp();
        List<BackupNodeQueuesPojo> queue = pojo.getQueue();
        if (queue != null && !queue.isEmpty())
        {
            json.queue = new ArrayList<>();
            for (BackupNodeQueuesPojo node : queue)
            {
                json.queue.add(apiToNodeQueues(node));
            }
        }
        else
        {
            json.queue = Collections.emptyList();
        }
        return json;
    }

    public static JsonGenTypes.NodeQueue apiToNodeQueues(BackupNodeQueuesPojo pojo)
    {
        JsonGenTypes.NodeQueue json = new JsonGenTypes.NodeQueue();
        json.node_name = pojo.getNodeName();
        List<BackupSnapQueuesPojo> queue = pojo.getQueue();
        if (queue != null && !queue.isEmpty())
        {
            json.queue = new ArrayList<>();
            for (BackupSnapQueuesPojo item : queue)
            {
                json.queue.add(apiToSnapQueues(item));
            }
        }
        else
        {
            json.queue = Collections.emptyList();
        }
        return json;
    }

    public static QuerySizeInfoRequestPojo querySizeInfoReqToPojo(
        String rscGrpName,
        JsonGenTypes.QuerySizeInfoRequest req
    )
    {
        return new QuerySizeInfoRequestPojo(
            rscGrpName,
            AutoSelectFilterPojo.copy(new AutoSelectFilterData(req.select_filter)),
            req.ignore_cache_older_than_sec
        );
    }

    @SuppressWarnings("checkstyle:LineLengthCheck")
    public static JsonGenTypes.QuerySizeInfoResponse pojoToQuerySizeInfoResp(
        QuerySizeInfoResponsePojo pojo,
        ApiCallRc apiCallRcRef
    )
    {
        JsonGenTypes.QuerySizeInfoResponse resp = new JsonGenTypes.QuerySizeInfoResponse();
        if (pojo != null)
        {
            resp.space_info = new JsonGenTypes.QuerySizeInfoResponseSpaceInfo();
            JsonGenTypes.QuerySizeInfoResponseSpaceInfo spaceInfo = resp.space_info;
            spaceInfo.max_vlm_size_in_kib = pojo.getMaxVlmSize();
            spaceInfo.available_size_in_kib = pojo.getAvailableSize();
            spaceInfo.capacity_in_kib = pojo.getCapacity();
            spaceInfo.next_spawn_result = new ArrayList<>();
            spaceInfo.default_max_oversubscription_ratio = LinStor.OVERSUBSCRIPTION_RATIO_DEFAULT;

            List<JsonGenTypes.QuerySizeInfoSpawnResult> nextSpawnList = spaceInfo.next_spawn_result;
            for (StorPoolApi spApi : pojo.nextSpawnSpList())
            {
                JsonGenTypes.QuerySizeInfoSpawnResult spawnResult = new JsonGenTypes.QuerySizeInfoSpawnResult();
                spawnResult.node_name = spApi.getNodeName();
                spawnResult.stor_pool_name = spApi.getStorPoolName();
                spawnResult.stor_pool_oversubscription_ratio = spApi.getOversubscriptionRatio();
                spawnResult.stor_pool_free_capacity_oversubscription_ratio = spApi.getMaxFreeCapacityOversubscriptionRatio();
                spawnResult.stor_pool_total_capacity_oversubscription_ratio = spApi.getMaxTotalCapacityOversubscriptionRatio();
                nextSpawnList.add(spawnResult);
            }
        }
        if (apiCallRcRef != null)
        {
            resp.reports = apiCallRcToJson(apiCallRcRef);
        }
        return resp;
    }

    public static QueryAllSizeInfoRequestPojo queryAllSizeInfoReqToPojo(JsonGenTypes.QueryAllSizeInfoRequest qasiReqRef)
    {
        return new QueryAllSizeInfoRequestPojo(
            AutoSelectFilterPojo.copy(new AutoSelectFilterData(qasiReqRef.select_filter)),
            qasiReqRef.ignore_cache_older_than_sec
        );
    }

    public static JsonGenTypes.QueryAllSizeInfoResponse pojoToQueryAllSizeInfoResp(
        QueryAllSizeInfoResponsePojo resultPojoRef
    )
    {
        JsonGenTypes.QueryAllSizeInfoResponse resp = new JsonGenTypes.QueryAllSizeInfoResponse();
        if (resultPojoRef != null)
        {
            Map<String, QueryAllSizeInfoResponseEntryPojo> map = resultPojoRef.getResult();
            resp.result = new TreeMap<>();
            for (Entry<String, QueryAllSizeInfoResponseEntryPojo> entry : map.entrySet())
            {
                QueryAllSizeInfoResponseEntryPojo qsiResponsePojo = entry.getValue();
                resp.result.put(
                    entry.getKey(),
                    pojoToQuerySizeInfoResp(qsiResponsePojo.getQsiRespPojo(), qsiResponsePojo.getApiCallRc())
                );
            }

            ApiCallRc apiCallRc = resultPojoRef.getApiCallRc();
            if (apiCallRc != null)
            {
                resp.reports = apiCallRcToJson(apiCallRc);
            }
        }
        return resp;
    }

    private Json()
    {
    }

}
