package com.linbit.linstor.api.rest.v1.serializer;

import com.linbit.InvalidNameException;
import com.linbit.linstor.NetInterface;
import com.linbit.linstor.NodeName;
import com.linbit.linstor.Resource;
import com.linbit.linstor.ResourceConnection;
import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.SnapshotDefinition;
import com.linbit.linstor.SnapshotVolumeDefinition;
import com.linbit.linstor.Volume;
import com.linbit.linstor.VolumeDefinition;
import com.linbit.linstor.api.interfaces.AutoSelectFilterApi;
import com.linbit.linstor.api.interfaces.RscDfnLayerDataApi;
import com.linbit.linstor.api.interfaces.RscLayerDataApi;
import com.linbit.linstor.api.interfaces.VlmDfnLayerDataApi;
import com.linbit.linstor.api.interfaces.VlmLayerDataApi;
import com.linbit.linstor.api.pojo.LuksRscPojo;
import com.linbit.linstor.api.pojo.DrbdRscPojo;
import com.linbit.linstor.api.pojo.NetInterfacePojo;
import com.linbit.linstor.api.pojo.RscPojo;
import com.linbit.linstor.api.pojo.StorageRscPojo;
import com.linbit.linstor.api.pojo.VlmDfnPojo;
import com.linbit.linstor.api.protobuf.MaxVlmSizeCandidatePojo;
import com.linbit.linstor.satellitestate.SatelliteState;
import com.linbit.linstor.stateflags.FlagsHelper;
import com.linbit.linstor.storage.interfaces.layers.drbd.DrbdRscObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.utils.Pair;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonInclude;

public class Json
{
    public static String deviceProviderKindAsString(DeviceProviderKind deviceProviderKind)
    {
        String str;
        switch (deviceProviderKind)
        {
            case DISKLESS:
                str = "DISKLESS";
                break;
            default:
                str = deviceProviderKind.name();
                break;
        }
        return str;
    }

    private static String getLayerTypeString(DeviceLayerKind deviceLayerKind)
    {
        String str;
        switch (deviceLayerKind)
        {
            case LUKS:
                str = "LUKS";
                break;
                default:
                    str = deviceLayerKind.name();
                    break;
        }
        return str;
    }


    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class NetInterfaceData
    {
        public String name;
        public String address;
        public Integer stlt_port;
        public String stlt_encryption_type;

        public NetInterfaceData()
        {
        }

        public NetInterfaceData(NetInterface.NetInterfaceApi netIfApi)
        {
            name = netIfApi.getName();
            address = netIfApi.getAddress();
            if (netIfApi.isUsableAsSatelliteConnection())
            {
                stlt_encryption_type = netIfApi.getSatelliteConnectionEncryptionType();
                stlt_port = netIfApi.getSatelliteConnectionPort();
            }
        }

        public NetInterface.NetInterfaceApi toApi()
        {
            return new NetInterfacePojo(null, name, address, stlt_port, stlt_encryption_type);
        }
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class NodeData
    {
        public String name;
        public String type;
        public Map<String, String> props;
        public List<String> flags;
        public List<NetInterfaceData> net_interfaces = new ArrayList<>();
        public String connection_status;
    }

    public static class NodeModifyData
    {
        public String node_type;
        public Map<String, String> override_props = Collections.emptyMap();
        public Set<String> delete_props = Collections.emptySet();
        public Set<String> delete_namespaces = Collections.emptySet();
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class StorPoolData
    {
        public String node_name;
        public String storage_pool_name;
        public String provider_kind;
        public Map<String, String> props = new HashMap<>();
        // Volumes are for now not reported, maybe later via flag
        public Map<String, String> static_traits;
        public Long free_capacity;
        public Long total_capacity;
        public String free_space_mgr_name;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class StorPoolModifyData
    {
        public Map<String, String> override_props = Collections.emptyMap();
        public Set<String> delete_props = Collections.emptySet();
        public Set<String> delete_namespaces = Collections.emptySet();
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class DrbdResourceDefinitionLayerData
    {
        public String resource_name_suffix;
        public Short peer_slots;
        public Integer al_stripes;
        public Long al_size;
        public Integer port;
        public String transport_type;
        public String secret;
        public Boolean down;

        public DrbdResourceDefinitionLayerData()
        {
        }

        public DrbdResourceDefinitionLayerData(DrbdRscPojo.DrbdRscDfnPojo drbdRscDfnPojo)
        {
            resource_name_suffix = drbdRscDfnPojo.getRscNameSuffix();
            peer_slots = drbdRscDfnPojo.getPeerSlots();
            al_stripes = drbdRscDfnPojo.getAlStripes();
            al_size = drbdRscDfnPojo.getAlStripeSize();
            port = drbdRscDfnPojo.getPort();
            transport_type = drbdRscDfnPojo.getTransportType();
            secret = drbdRscDfnPojo.getSecret();
            down = drbdRscDfnPojo.isDown();
        }
    }

    public static class ResourceDefinitionLayerData
    {
        public String type;
        public Object data;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class ResourceDefinitionData
    {
        public String name;
        public Map<String, String> props = Collections.emptyMap();
        public List<String> flags = Collections.emptyList();
        public List<VolumeDefinitionData> volume_definitions; // do not use for now

        public List<ResourceDefinitionLayerData> layer_data = Collections.emptyList();

        public ResourceDefinitionData()
        {
        }

        public ResourceDefinitionData(ResourceDefinition.RscDfnApi rscDfnApi)
        {
            name = rscDfnApi.getResourceName();
            flags = ResourceDefinition.RscDfnFlags.toStringList(rscDfnApi.getFlags());
            props = rscDfnApi.getProps();

            layer_data = new ArrayList<>();

            for (Pair<String, RscDfnLayerDataApi> layer : rscDfnApi.getLayerData())
            {
                ResourceDefinitionLayerData rscDfnLayerData = new ResourceDefinitionLayerData();
                rscDfnLayerData.type = layer.objA.equals(DeviceLayerKind.LUKS.name()) ? "LUKS" : layer.objA;

                if (layer.objB != null)
                {
                    switch (layer.objB.getLayerKind())
                    {
                        case DRBD:
                            DrbdRscPojo.DrbdRscDfnPojo drbdRscDfnPojo = (DrbdRscPojo.DrbdRscDfnPojo) layer.objB;
                            rscDfnLayerData.data = new DrbdResourceDefinitionLayerData(drbdRscDfnPojo);
                            break;
                        default:
                    }
                }
                layer_data.add(rscDfnLayerData);
            }
        }
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class ResourceDefinitionCreateData
    {
        public Integer drbd_port;
        public String drbd_secret;
        public String drbd_transport_type = "IP";

        public ResourceDefinitionData resource_definition;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class ResourceDefinitionModifyData
    {
        public Map<String, String> override_props = Collections.emptyMap();
        public Set<String> delete_props = Collections.emptySet();
        public Set<String> delete_namespaces = Collections.emptySet();

        public List<String> layer_stack = Collections.emptyList();
        public Integer drbd_port;
        // do not use for now
//        public String drbd_secret;
//        public String drbd_transport_type;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class DrbdVolumeDefinitionLayerData
    {
        public String resource_name_suffix;
        public Integer volume_number;
        public Integer minor_number;

        public DrbdVolumeDefinitionLayerData()
        {
        }

        public DrbdVolumeDefinitionLayerData(DrbdRscPojo.DrbdVlmDfnPojo drbdVlmDfnPojo)
        {
            resource_name_suffix = drbdVlmDfnPojo.getRscNameSuffix();
            volume_number = drbdVlmDfnPojo.getVlmNr();
            minor_number = drbdVlmDfnPojo.getMinorNr();
        }
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class VolumeDefinitionLayerData
    {
        public String type;
        public Object data;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class VolumeDefinitionData
    {
        public Integer volume_number;
        public Long size;
        public Map<String, String> props = Collections.emptyMap();
        public List<String> flags = Collections.emptyList();

        public List<VolumeDefinitionLayerData> layer_data = Collections.emptyList();

        public VolumeDefinitionData()
        {
        }

        public VolumeDefinitionData(VolumeDefinition.VlmDfnApi vlmDfnApi)
        {
            volume_number = vlmDfnApi.getVolumeNr();
            size = vlmDfnApi.getSize();
            props = vlmDfnApi.getProps();
            flags = FlagsHelper.toStringList(
                VolumeDefinition.VlmDfnFlags.class,
                vlmDfnApi.getFlags()
            );

            layer_data = new ArrayList<>();
            for (Pair<String, VlmDfnLayerDataApi> layer : vlmDfnApi.getVlmDfnLayerData())
            {
                VolumeDefinitionLayerData volumeDefinitionLayerData = new VolumeDefinitionLayerData();
                volumeDefinitionLayerData.type =
                    layer.objA.equals(DeviceLayerKind.LUKS.name()) ? "LUKS" : layer.objA;

                if (layer.objB != null)
                {
                    switch (layer.objB.getLayerKind())
                    {
                        case DRBD:
                            DrbdRscPojo.DrbdVlmDfnPojo drbdVlmDfnPojo = (DrbdRscPojo.DrbdVlmDfnPojo) layer.objB;
                            volumeDefinitionLayerData.data = new DrbdVolumeDefinitionLayerData(drbdVlmDfnPojo);
                            break;
                        default:
                    }
                }
                layer_data.add(volumeDefinitionLayerData);
            }
        }

        public VolumeDefinition.VlmDfnApi toVlmDfnApi()
        {
            ArrayList<Pair<String, VlmDfnLayerDataApi>> layerData = new ArrayList<>(); // empty will be ignored
            return new VlmDfnPojo(
                null,
                volume_number,
                size,
                FlagsHelper.fromStringList(VolumeDefinition.VlmDfnFlags.class, flags),
                props,
                layerData
            );
        }
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class VolumeDefinitionCreateData
    {
        public VolumeDefinitionData volume_definition;
        public Integer drbd_minor_number;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class VolumeDefinitionModifyData
    {
        public Long size;
        public Map<String, String> override_props = Collections.emptyMap();
        public Set<String> delete_props = Collections.emptySet();
        public Set<String> delete_namespaces = Collections.emptySet();
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class DrbdResourceData
    {
        public DrbdResourceDefinitionLayerData drbd_resource_definition;
        public Integer node_id;
        public Short peer_slots;
        public Integer al_stripes;
        public Long al_size;
        public List<String> flags;
        public List<DrbdVolumeData> drbd_volumes;

        public DrbdResourceData()
        {
        }

        public DrbdResourceData(DrbdRscPojo drbdRscPojo)
        {
            drbd_resource_definition = new DrbdResourceDefinitionLayerData(drbdRscPojo.getDrbdRscDfn());
            node_id = drbdRscPojo.getNodeId();
            peer_slots = drbdRscPojo.getPeerSlots();
            al_stripes = drbdRscPojo.getAlStripes();
            al_size = drbdRscPojo.getAlStripeSize();
            flags = FlagsHelper.toStringList(DrbdRscObject.DrbdRscFlags.class, drbdRscPojo.getFlags());
            drbd_volumes = drbdRscPojo.getVolumeList().stream().map(DrbdVolumeData::new).collect(Collectors.toList());
        }
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class StorageResourceData
    {
        public List<StorageVolume> storage_volumes;

        public StorageResourceData()
        {
        }

        public StorageResourceData(StorageRscPojo storageRscPojo)
        {
            storage_volumes = storageRscPojo.getVolumeList().stream()
                .map(StorageVolume::new)
                .collect(Collectors.toList());
        }
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class LUKSResourceData
    {
        public List<LUKSVolume> luks_volumes;

        public LUKSResourceData()
        {
        }

        public LUKSResourceData(LuksRscPojo luksRscPojo)
        {
            luks_volumes = luksRscPojo.getVolumeList().stream()
                .map(LUKSVolume::new)
                .collect(Collectors.toList());
        }
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class ResourceLayerData
    {
        public List<ResourceLayerData> children = Collections.emptyList();
        public String resource_name_suffix;
        public String type;
        public DrbdResourceData drbd;
        public LUKSResourceData luks;
        public StorageResourceData storage;

        public ResourceLayerData()
        {
        }

        public ResourceLayerData(RscLayerDataApi rscLayerDataApi)
        {
            resource_name_suffix = rscLayerDataApi.getRscNameSuffix();
            children = rscLayerDataApi.getChildren().stream().map(ResourceLayerData::new).collect(Collectors.toList());
            type = getLayerTypeString(rscLayerDataApi.getLayerKind());
            switch (rscLayerDataApi.getLayerKind())
            {
                case DRBD:
                    DrbdRscPojo drbdRscPojo = (DrbdRscPojo) rscLayerDataApi;
                    drbd = new DrbdResourceData(drbdRscPojo);
                    break;
                case STORAGE:
                    StorageRscPojo storageRscPojo = (StorageRscPojo) rscLayerDataApi;
                    storage = new StorageResourceData(storageRscPojo);
                    break;
                case LUKS:
                    LuksRscPojo luksRscPojo = (LuksRscPojo) rscLayerDataApi;
                    luks = new LUKSResourceData(luksRscPojo);
                    break;
                default:
            }
        }
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class ResourceData
    {
        public String name;
        public String node_name;
        public Map<String, String> props = Collections.emptyMap();
        public List<String> flags = Collections.emptyList();
        public ResourceLayerData layer_object;
        public ResourceStateData state;

        public ResourceData()
        {
        }

        public ResourceData(Resource.RscApi rscApi, Map<NodeName, SatelliteState> satelliteStates)
        {
            name = rscApi.getName();
            node_name = rscApi.getNodeName();
            flags = FlagsHelper.toStringList(Resource.RscFlags.class, rscApi.getFlags());
            props = rscApi.getProps();
            layer_object = new ResourceLayerData(rscApi.getLayerData());

            try
            {
                final ResourceName rscNameRes = new ResourceName(rscApi.getName());
                final NodeName linNodeName = new NodeName(rscApi.getNodeName());
                if (satelliteStates.containsKey(linNodeName) &&
                    satelliteStates.get(linNodeName)
                        .getResourceStates().containsKey(rscNameRes))
                {
                    state = new ResourceStateData();
                    state.in_use = satelliteStates.get(linNodeName)
                        .getResourceStates().get(rscNameRes).isInUse();
                }
            }
            catch (InvalidNameException ignored)
            {
            }
        }

        public Resource.RscApi toRscApi()
        {
            return new RscPojo(
                name,
                node_name,
                FlagsHelper.fromStringList(Resource.RscFlags.class, flags),
                props
            );
        }
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class ResourceStateData
    {
        public Boolean in_use;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class ResourceCreateData
    {
        public ResourceData resource;
        public List<String> layer_list = Collections.emptyList();
        public Integer drbd_node_id;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class ResourceModifyData
    {
        public Map<String, String> override_props = Collections.emptyMap();
        public Set<String> delete_props = Collections.emptySet();
        public Set<String> delete_namespaces = Collections.emptySet();
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class DrbdVolumeDefinition
    {
        public String resource_name_suffix;
        public Integer volume_number;
        public Integer minor_number;

        public DrbdVolumeDefinition()
        {
        }

        public DrbdVolumeDefinition(DrbdRscPojo.DrbdVlmDfnPojo drbdVlmDfnPojo)
        {
            resource_name_suffix = drbdVlmDfnPojo.getRscNameSuffix();
            volume_number = drbdVlmDfnPojo.getVlmNr();
            minor_number = drbdVlmDfnPojo.getMinorNr();
        }
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class DrbdVolumeData
    {
        public DrbdVolumeDefinition drbd_volume_definition;
        public String device_path;
        public String backing_device;
        public String meta_disk;
        public Long allocated_size;
        public Long usable_size;
        public String disk_state;

        public DrbdVolumeData()
        {
        }

        public DrbdVolumeData(DrbdRscPojo.DrbdVlmPojo drbdVlmPojo)
        {
            drbd_volume_definition = new DrbdVolumeDefinition(drbdVlmPojo.getDrbdVlmDfn());
            device_path = drbdVlmPojo.getDevicePath();
            backing_device = drbdVlmPojo.getBackingDisk();
            meta_disk = drbdVlmPojo.getMetaDisk();
            allocated_size = drbdVlmPojo.getAllocatedSize();
            usable_size = drbdVlmPojo.getUsableSize();
            disk_state = drbdVlmPojo.getDiskState();
        }
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class LUKSVolume
    {
        public Integer volume_number;
        public String device_path;
        public String backing_device;
        public Long allocated_size;
        public Long usable_size;
        public Boolean opened;
        public String disk_state;

        public LUKSVolume()
        {
        }

        public LUKSVolume(LuksRscPojo.LuksVlmPojo luksVlmPojo)
        {
            volume_number = luksVlmPojo.getVlmNr();
            device_path = luksVlmPojo.getDevicePath();
            backing_device = luksVlmPojo.getBackingDevice();
            allocated_size = luksVlmPojo.getAllocatedSize();
            usable_size = luksVlmPojo.getUsableSize();
            opened = luksVlmPojo.isOpened();
            disk_state = luksVlmPojo.getDiskState();
        }
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class StorageVolume
    {
        public Integer volume_number;
        public String device_path;
        public Long allocated_size;
        public Long usable_size;
        public String disk_state;

        // ignore provider for now

        public StorageVolume()
        {
        }

        public StorageVolume(VlmLayerDataApi vlmLayerDataApi)
        {
            volume_number = vlmLayerDataApi.getVlmNr();
            device_path = vlmLayerDataApi.getDevicePath();
            allocated_size = vlmLayerDataApi.getAllocatedSize();
            usable_size = vlmLayerDataApi.getUsableSize();
            disk_state = vlmLayerDataApi.getDiskState();
        }
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class VolumeLayerData
    {
        public String type;
        public Object data;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class VolumeData
    {
        public Integer volume_number;
        public String storage_pool_name;
        public String provider_kind;
        public String device_path;
        public Long allocated_size;
        public Long usable_size;
        public Map<String, String> props = Collections.emptyMap();
        public List<String> flags = Collections.emptyList();

        public VolumeStateData state;
        public List<VolumeLayerData> layer_data_list = Collections.emptyList();

        public VolumeData()
        {
        }

        public VolumeData(Volume.VlmApi vlmApi)
        {
            volume_number = vlmApi.getVlmNr();
            storage_pool_name = vlmApi.getStorPoolName();
            provider_kind = deviceProviderKindAsString(vlmApi.getStorPoolDeviceProviderKind());

            device_path = vlmApi.getDevicePath();
            allocated_size = vlmApi.getAllocatedSize().orElse(null);

            props = vlmApi.getVlmProps();
            flags = FlagsHelper.toStringList(Volume.VlmFlags.class, vlmApi.getFlags());

            layer_data_list = new ArrayList<>();

            for (Pair<String, VlmLayerDataApi> layerData : vlmApi.getVlmLayerData())
            {
                VolumeLayerData volumeLayerData = new VolumeLayerData();
                volumeLayerData.type = getLayerTypeString(layerData.objB.getLayerKind());

                switch (layerData.objB.getLayerKind())
                {
                    case DRBD:
                        DrbdRscPojo.DrbdVlmPojo drbdVlm = (DrbdRscPojo.DrbdVlmPojo) layerData.objB;
                        volumeLayerData.data = new DrbdVolumeData(drbdVlm);
                        break;
                    case STORAGE:
                        volumeLayerData.data = new StorageVolume(layerData.objB);
                        break;
                    case LUKS:
                        LuksRscPojo.LuksVlmPojo luksVlmPojo = (LuksRscPojo.LuksVlmPojo) layerData.objB;
                        volumeLayerData.data = new LUKSVolume(luksVlmPojo);
                        break;
                    default:
                }

                layer_data_list.add(volumeLayerData);
            }
        }
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class VolumeStateData
    {
        public String disk_state;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class AutoSelectFilterData implements AutoSelectFilterApi
    {
        public int place_count = 2;
        public String storage_pool;
        public List<String> not_place_with_rsc = new ArrayList<>();
        public String not_place_with_rsc_regex;
        public List<String> replicas_on_same = new ArrayList<>();
        public List<String> replicas_on_different = new ArrayList<>();

        @Override
        public int getPlaceCount()
        {
            return place_count;
        }

        @Override
        public String getStorPoolNameStr()
        {
            return storage_pool;
        }

        @Override
        public List<String> getNotPlaceWithRscList()
        {
            return not_place_with_rsc;
        }

        @Override
        public String getNotPlaceWithRscRegex()
        {
            return not_place_with_rsc_regex;
        }

        @Override
        public List<String> getReplicasOnSameList()
        {
            return replicas_on_same;
        }

        @Override
        public List<String> getReplicasOnDifferentList()
        {
            return replicas_on_different;
        }
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class AutoPlaceRequest
    {
        public boolean diskless_on_remaining = false;
        public AutoSelectFilterData select_filter = new AutoSelectFilterData();
        public List<String> layer_list = Collections.emptyList();
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class ErrorReport
    {
        public String node_name;
        public long error_time;
        public String filename;
        public String text;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class ControllerPropsModify
    {
        public Map<String, String> override_props = Collections.emptyMap();
        public Set<String> delete_props = Collections.emptySet();
        public Set<String> delete_namespaces = Collections.emptySet();
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class ControllerVersion
    {
        public String version;
        public String git_hash;
        public String build_time;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class PassPhraseCreate
    {
        public String new_passphrase;
        public String old_passphrase;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class SnapshotVolumeDfnData
    {
        public Integer volume_number;
        public long size;

        public SnapshotVolumeDfnData()
        {
        }

        public SnapshotVolumeDfnData(SnapshotVolumeDefinition.SnapshotVlmDfnApi snapshotVlmDfnApi)
        {
            volume_number = snapshotVlmDfnApi.getVolumeNr();
            size = snapshotVlmDfnApi.getSize();
        }
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class SnapshotData
    {
        public String name;
        public String resource_name;
        public List<String> nodes = Collections.emptyList();
        public Map<String, String> props = Collections.emptyMap();
        public List<String> flags = Collections.emptyList();
        public List<SnapshotVolumeDfnData> volume_definitions = Collections.emptyList();

        public SnapshotData()
        {
        }

        public SnapshotData(SnapshotDefinition.SnapshotDfnListItemApi snapshotDfnListItemApi)
        {
            name = snapshotDfnListItemApi.getSnapshotName();
            resource_name = snapshotDfnListItemApi.getRscDfn().getResourceName();
            nodes = snapshotDfnListItemApi.getNodeNames();
            props = snapshotDfnListItemApi.getProps();
            flags = FlagsHelper.toStringList(
                SnapshotDefinition.SnapshotDfnFlags.class, snapshotDfnListItemApi.getFlags()
            );
            volume_definitions = snapshotDfnListItemApi.getSnapshotVlmDfnList().stream()
                .map(SnapshotVolumeDfnData::new)
                .collect(Collectors.toList());
        }
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class SnapshotRestore
    {
        public List<String> nodes = Collections.emptyList();  //optional
        public String to_resource;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class ResourceConnectionData
    {
        public String node_a;
        public String node_b;
        public Map<String, String> props = Collections.emptyMap();
        public List<String> flags = Collections.emptyList();
        public Integer port;

        public ResourceConnectionData()
        {
        }

        public ResourceConnectionData(ResourceConnection.RscConnApi rscConnApi)
        {
            node_a = rscConnApi.getSourceNodeName();
            node_b = rscConnApi.getTargetNodeName();
            props = rscConnApi.getProps();
            flags = FlagsHelper.toStringList(ResourceConnection.RscConnFlags.class, rscConnApi.getFlags());
            port = rscConnApi.getPort();
        }
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class ResourceConnectionModify
    {
        public Map<String, String> override_props = Collections.emptyMap();
        public Set<String> delete_props = Collections.emptySet();
        public Set<String> delete_namespaces = Collections.emptySet();
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class DrbdProxyEnable
    {
        public Integer port;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class DrbdProxyModify
    {
        public Map<String, String> override_props = Collections.emptyMap();
        public Set<String> delete_props = Collections.emptySet();
        public Set<String> delete_namespaces = Collections.emptySet();
        public String compression_type;
        public Map<String, String> compression_props = Collections.emptyMap();
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class Candidate
    {
        public String storage_pool;
        public Long max_volume_size;
        public List<String> node_names;
        public boolean all_thin;

        public Candidate()
        {
        }

        public Candidate(MaxVlmSizeCandidatePojo pojo)
        {
            storage_pool = pojo.getStorPoolDfnApi().getName();
            max_volume_size = pojo.getMaxVlmSize();
            node_names = pojo.getNodeNames();
            all_thin = pojo.areAllThin();
        }
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class MaxVolumeSizesData
    {
        public List<Candidate> candidates;
        public Double default_max_oversubscription_ratio;

        public MaxVolumeSizesData(List<MaxVlmSizeCandidatePojo> candidatePojos)
        {
            candidates = candidatePojos.stream().map(Candidate::new).collect(Collectors.toList());
        }
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class KeyValueStore
    {
        public String name;
        public Map<String, String> props;

        public KeyValueStore()
        {
        }

        public KeyValueStore(com.linbit.linstor.KeyValueStore.KvsApi kvsApi)
        {
            name = kvsApi.getName();
            props = kvsApi.getProps();
        }
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class ModifyKeyValueStore
    {
        public Map<String, String> override_props = Collections.emptyMap();
        public Set<String> delete_props = Collections.emptySet();
        public Set<String> delete_namespaces = Collections.emptySet();
    }
}
