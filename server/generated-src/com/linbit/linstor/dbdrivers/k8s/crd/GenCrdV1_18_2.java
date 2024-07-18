package com.linbit.linstor.dbdrivers.k8s.crd;

import com.linbit.ImplementationError;
import com.linbit.linstor.dbdrivers.DatabaseTable;
import com.linbit.linstor.dbdrivers.DatabaseTable.Column;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.transaction.BaseControllerK8sCrdTransactionMgrContext;
import com.linbit.linstor.transaction.K8sCrdMigrationContext;
import com.linbit.linstor.transaction.K8sCrdSchemaUpdateContext;
import com.linbit.linstor.utils.ByteUtils;
import com.linbit.utils.ExceptionThrowingFunction;
import com.linbit.utils.TimeUtils;

import java.nio.charset.StandardCharsets;
import java.sql.Types;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;
import com.fasterxml.jackson.databind.DatabindContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.jsontype.impl.TypeIdResolverBase;
import com.fasterxml.jackson.databind.type.TypeFactory;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.model.annotation.Group;
import io.fabric8.kubernetes.model.annotation.Plural;
import io.fabric8.kubernetes.model.annotation.Singular;
import io.fabric8.kubernetes.model.annotation.Version;

public class GenCrdV1_18_2
{
    public static final String VERSION = "v1-18-2";
    public static final String GROUP = "internal.linstor.linbit.com";
    private static final DateTimeFormatter RFC3339 = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssXXX");
    private static final Map<String, String> KEY_LUT = new HashMap<>();
    private static final HashSet<String> USED_K8S_KEYS = new HashSet<>();
    private static final AtomicLong NEXT_ID = new AtomicLong();
    private static final HashMap<String, Class<?>> JSON_ID_TO_TYPE_CLASS_LUT = new HashMap<>();

    static
    {
        JSON_ID_TO_TYPE_CLASS_LUT.put("Files", Files.class);
        JSON_ID_TO_TYPE_CLASS_LUT.put("FilesSpec", FilesSpec.class);
        JSON_ID_TO_TYPE_CLASS_LUT.put("KeyValueStore", KeyValueStore.class);
        JSON_ID_TO_TYPE_CLASS_LUT.put("KeyValueStoreSpec", KeyValueStoreSpec.class);
        JSON_ID_TO_TYPE_CLASS_LUT.put("LayerBcacheVolumes", LayerBcacheVolumes.class);
        JSON_ID_TO_TYPE_CLASS_LUT.put("LayerBcacheVolumesSpec", LayerBcacheVolumesSpec.class);
        JSON_ID_TO_TYPE_CLASS_LUT.put("LayerCacheVolumes", LayerCacheVolumes.class);
        JSON_ID_TO_TYPE_CLASS_LUT.put("LayerCacheVolumesSpec", LayerCacheVolumesSpec.class);
        JSON_ID_TO_TYPE_CLASS_LUT.put("LayerDrbdResources", LayerDrbdResources.class);
        JSON_ID_TO_TYPE_CLASS_LUT.put("LayerDrbdResourcesSpec", LayerDrbdResourcesSpec.class);
        JSON_ID_TO_TYPE_CLASS_LUT.put("LayerDrbdResourceDefinitions", LayerDrbdResourceDefinitions.class);
        JSON_ID_TO_TYPE_CLASS_LUT.put("LayerDrbdResourceDefinitionsSpec", LayerDrbdResourceDefinitionsSpec.class);
        JSON_ID_TO_TYPE_CLASS_LUT.put("LayerDrbdVolumes", LayerDrbdVolumes.class);
        JSON_ID_TO_TYPE_CLASS_LUT.put("LayerDrbdVolumesSpec", LayerDrbdVolumesSpec.class);
        JSON_ID_TO_TYPE_CLASS_LUT.put("LayerDrbdVolumeDefinitions", LayerDrbdVolumeDefinitions.class);
        JSON_ID_TO_TYPE_CLASS_LUT.put("LayerDrbdVolumeDefinitionsSpec", LayerDrbdVolumeDefinitionsSpec.class);
        JSON_ID_TO_TYPE_CLASS_LUT.put("LayerLuksVolumes", LayerLuksVolumes.class);
        JSON_ID_TO_TYPE_CLASS_LUT.put("LayerLuksVolumesSpec", LayerLuksVolumesSpec.class);
        JSON_ID_TO_TYPE_CLASS_LUT.put("LayerOpenflexResourceDefinitions", LayerOpenflexResourceDefinitions.class);
        JSON_ID_TO_TYPE_CLASS_LUT.put("LayerOpenflexResourceDefinitionsSpec", LayerOpenflexResourceDefinitionsSpec.class);
        JSON_ID_TO_TYPE_CLASS_LUT.put("LayerOpenflexVolumes", LayerOpenflexVolumes.class);
        JSON_ID_TO_TYPE_CLASS_LUT.put("LayerOpenflexVolumesSpec", LayerOpenflexVolumesSpec.class);
        JSON_ID_TO_TYPE_CLASS_LUT.put("LayerResourceIds", LayerResourceIds.class);
        JSON_ID_TO_TYPE_CLASS_LUT.put("LayerResourceIdsSpec", LayerResourceIdsSpec.class);
        JSON_ID_TO_TYPE_CLASS_LUT.put("LayerStorageVolumes", LayerStorageVolumes.class);
        JSON_ID_TO_TYPE_CLASS_LUT.put("LayerStorageVolumesSpec", LayerStorageVolumesSpec.class);
        JSON_ID_TO_TYPE_CLASS_LUT.put("LayerWritecacheVolumes", LayerWritecacheVolumes.class);
        JSON_ID_TO_TYPE_CLASS_LUT.put("LayerWritecacheVolumesSpec", LayerWritecacheVolumesSpec.class);
        JSON_ID_TO_TYPE_CLASS_LUT.put("LinstorRemotes", LinstorRemotes.class);
        JSON_ID_TO_TYPE_CLASS_LUT.put("LinstorRemotesSpec", LinstorRemotesSpec.class);
        JSON_ID_TO_TYPE_CLASS_LUT.put("Nodes", Nodes.class);
        JSON_ID_TO_TYPE_CLASS_LUT.put("NodesSpec", NodesSpec.class);
        JSON_ID_TO_TYPE_CLASS_LUT.put("NodeConnections", NodeConnections.class);
        JSON_ID_TO_TYPE_CLASS_LUT.put("NodeConnectionsSpec", NodeConnectionsSpec.class);
        JSON_ID_TO_TYPE_CLASS_LUT.put("NodeNetInterfaces", NodeNetInterfaces.class);
        JSON_ID_TO_TYPE_CLASS_LUT.put("NodeNetInterfacesSpec", NodeNetInterfacesSpec.class);
        JSON_ID_TO_TYPE_CLASS_LUT.put("NodeStorPool", NodeStorPool.class);
        JSON_ID_TO_TYPE_CLASS_LUT.put("NodeStorPoolSpec", NodeStorPoolSpec.class);
        JSON_ID_TO_TYPE_CLASS_LUT.put("PropsContainers", PropsContainers.class);
        JSON_ID_TO_TYPE_CLASS_LUT.put("PropsContainersSpec", PropsContainersSpec.class);
        JSON_ID_TO_TYPE_CLASS_LUT.put("Resources", Resources.class);
        JSON_ID_TO_TYPE_CLASS_LUT.put("ResourcesSpec", ResourcesSpec.class);
        JSON_ID_TO_TYPE_CLASS_LUT.put("ResourceConnections", ResourceConnections.class);
        JSON_ID_TO_TYPE_CLASS_LUT.put("ResourceConnectionsSpec", ResourceConnectionsSpec.class);
        JSON_ID_TO_TYPE_CLASS_LUT.put("ResourceDefinitions", ResourceDefinitions.class);
        JSON_ID_TO_TYPE_CLASS_LUT.put("ResourceDefinitionsSpec", ResourceDefinitionsSpec.class);
        JSON_ID_TO_TYPE_CLASS_LUT.put("ResourceGroups", ResourceGroups.class);
        JSON_ID_TO_TYPE_CLASS_LUT.put("ResourceGroupsSpec", ResourceGroupsSpec.class);
        JSON_ID_TO_TYPE_CLASS_LUT.put("S3Remotes", S3Remotes.class);
        JSON_ID_TO_TYPE_CLASS_LUT.put("S3RemotesSpec", S3RemotesSpec.class);
        JSON_ID_TO_TYPE_CLASS_LUT.put("SatellitesCapacity", SatellitesCapacity.class);
        JSON_ID_TO_TYPE_CLASS_LUT.put("SatellitesCapacitySpec", SatellitesCapacitySpec.class);
        JSON_ID_TO_TYPE_CLASS_LUT.put("Schedules", Schedules.class);
        JSON_ID_TO_TYPE_CLASS_LUT.put("SchedulesSpec", SchedulesSpec.class);
        JSON_ID_TO_TYPE_CLASS_LUT.put("SecAccessTypes", SecAccessTypes.class);
        JSON_ID_TO_TYPE_CLASS_LUT.put("SecAccessTypesSpec", SecAccessTypesSpec.class);
        JSON_ID_TO_TYPE_CLASS_LUT.put("SecAclMap", SecAclMap.class);
        JSON_ID_TO_TYPE_CLASS_LUT.put("SecAclMapSpec", SecAclMapSpec.class);
        JSON_ID_TO_TYPE_CLASS_LUT.put("SecConfiguration", SecConfiguration.class);
        JSON_ID_TO_TYPE_CLASS_LUT.put("SecConfigurationSpec", SecConfigurationSpec.class);
        JSON_ID_TO_TYPE_CLASS_LUT.put("SecDfltRoles", SecDfltRoles.class);
        JSON_ID_TO_TYPE_CLASS_LUT.put("SecDfltRolesSpec", SecDfltRolesSpec.class);
        JSON_ID_TO_TYPE_CLASS_LUT.put("SecIdentities", SecIdentities.class);
        JSON_ID_TO_TYPE_CLASS_LUT.put("SecIdentitiesSpec", SecIdentitiesSpec.class);
        JSON_ID_TO_TYPE_CLASS_LUT.put("SecIdRoleMap", SecIdRoleMap.class);
        JSON_ID_TO_TYPE_CLASS_LUT.put("SecIdRoleMapSpec", SecIdRoleMapSpec.class);
        JSON_ID_TO_TYPE_CLASS_LUT.put("SecObjectProtection", SecObjectProtection.class);
        JSON_ID_TO_TYPE_CLASS_LUT.put("SecObjectProtectionSpec", SecObjectProtectionSpec.class);
        JSON_ID_TO_TYPE_CLASS_LUT.put("SecRoles", SecRoles.class);
        JSON_ID_TO_TYPE_CLASS_LUT.put("SecRolesSpec", SecRolesSpec.class);
        JSON_ID_TO_TYPE_CLASS_LUT.put("SecTypes", SecTypes.class);
        JSON_ID_TO_TYPE_CLASS_LUT.put("SecTypesSpec", SecTypesSpec.class);
        JSON_ID_TO_TYPE_CLASS_LUT.put("SecTypeRules", SecTypeRules.class);
        JSON_ID_TO_TYPE_CLASS_LUT.put("SecTypeRulesSpec", SecTypeRulesSpec.class);
        JSON_ID_TO_TYPE_CLASS_LUT.put("SpaceHistory", SpaceHistory.class);
        JSON_ID_TO_TYPE_CLASS_LUT.put("SpaceHistorySpec", SpaceHistorySpec.class);
        JSON_ID_TO_TYPE_CLASS_LUT.put("StorPoolDefinitions", StorPoolDefinitions.class);
        JSON_ID_TO_TYPE_CLASS_LUT.put("StorPoolDefinitionsSpec", StorPoolDefinitionsSpec.class);
        JSON_ID_TO_TYPE_CLASS_LUT.put("TrackingDate", TrackingDate.class);
        JSON_ID_TO_TYPE_CLASS_LUT.put("TrackingDateSpec", TrackingDateSpec.class);
        JSON_ID_TO_TYPE_CLASS_LUT.put("Volumes", Volumes.class);
        JSON_ID_TO_TYPE_CLASS_LUT.put("VolumesSpec", VolumesSpec.class);
        JSON_ID_TO_TYPE_CLASS_LUT.put("VolumeConnections", VolumeConnections.class);
        JSON_ID_TO_TYPE_CLASS_LUT.put("VolumeConnectionsSpec", VolumeConnectionsSpec.class);
        JSON_ID_TO_TYPE_CLASS_LUT.put("VolumeDefinitions", VolumeDefinitions.class);
        JSON_ID_TO_TYPE_CLASS_LUT.put("VolumeDefinitionsSpec", VolumeDefinitionsSpec.class);
        JSON_ID_TO_TYPE_CLASS_LUT.put("VolumeGroups", VolumeGroups.class);
        JSON_ID_TO_TYPE_CLASS_LUT.put("VolumeGroupsSpec", VolumeGroupsSpec.class);
    }

    private GenCrdV1_18_2()
    {
    }

    @SuppressWarnings("unchecked")
    public static <CRD extends LinstorCrd<SPEC>, SPEC extends LinstorSpec<CRD, SPEC>> Class<? extends LinstorCrd<SPEC>> databaseTableToCustomResourceClass(
        DatabaseTable table
    )
    {
        switch(table.getName())
        {
            case "FILES":
                return (Class<CRD>) Files.class;
            case "KEY_VALUE_STORE":
                return (Class<CRD>) KeyValueStore.class;
            case "LAYER_BCACHE_VOLUMES":
                return (Class<CRD>) LayerBcacheVolumes.class;
            case "LAYER_CACHE_VOLUMES":
                return (Class<CRD>) LayerCacheVolumes.class;
            case "LAYER_DRBD_RESOURCES":
                return (Class<CRD>) LayerDrbdResources.class;
            case "LAYER_DRBD_RESOURCE_DEFINITIONS":
                return (Class<CRD>) LayerDrbdResourceDefinitions.class;
            case "LAYER_DRBD_VOLUMES":
                return (Class<CRD>) LayerDrbdVolumes.class;
            case "LAYER_DRBD_VOLUME_DEFINITIONS":
                return (Class<CRD>) LayerDrbdVolumeDefinitions.class;
            case "LAYER_LUKS_VOLUMES":
                return (Class<CRD>) LayerLuksVolumes.class;
            case "LAYER_OPENFLEX_RESOURCE_DEFINITIONS":
                return (Class<CRD>) LayerOpenflexResourceDefinitions.class;
            case "LAYER_OPENFLEX_VOLUMES":
                return (Class<CRD>) LayerOpenflexVolumes.class;
            case "LAYER_RESOURCE_IDS":
                return (Class<CRD>) LayerResourceIds.class;
            case "LAYER_STORAGE_VOLUMES":
                return (Class<CRD>) LayerStorageVolumes.class;
            case "LAYER_WRITECACHE_VOLUMES":
                return (Class<CRD>) LayerWritecacheVolumes.class;
            case "LINSTOR_REMOTES":
                return (Class<CRD>) LinstorRemotes.class;
            case "NODES":
                return (Class<CRD>) Nodes.class;
            case "NODE_CONNECTIONS":
                return (Class<CRD>) NodeConnections.class;
            case "NODE_NET_INTERFACES":
                return (Class<CRD>) NodeNetInterfaces.class;
            case "NODE_STOR_POOL":
                return (Class<CRD>) NodeStorPool.class;
            case "PROPS_CONTAINERS":
                return (Class<CRD>) PropsContainers.class;
            case "RESOURCES":
                return (Class<CRD>) Resources.class;
            case "RESOURCE_CONNECTIONS":
                return (Class<CRD>) ResourceConnections.class;
            case "RESOURCE_DEFINITIONS":
                return (Class<CRD>) ResourceDefinitions.class;
            case "RESOURCE_GROUPS":
                return (Class<CRD>) ResourceGroups.class;
            case "S3_REMOTES":
                return (Class<CRD>) S3Remotes.class;
            case "SATELLITES_CAPACITY":
                return (Class<CRD>) SatellitesCapacity.class;
            case "SCHEDULES":
                return (Class<CRD>) Schedules.class;
            case "SEC_ACCESS_TYPES":
                return (Class<CRD>) SecAccessTypes.class;
            case "SEC_ACL_MAP":
                return (Class<CRD>) SecAclMap.class;
            case "SEC_CONFIGURATION":
                return (Class<CRD>) SecConfiguration.class;
            case "SEC_DFLT_ROLES":
                return (Class<CRD>) SecDfltRoles.class;
            case "SEC_IDENTITIES":
                return (Class<CRD>) SecIdentities.class;
            case "SEC_ID_ROLE_MAP":
                return (Class<CRD>) SecIdRoleMap.class;
            case "SEC_OBJECT_PROTECTION":
                return (Class<CRD>) SecObjectProtection.class;
            case "SEC_ROLES":
                return (Class<CRD>) SecRoles.class;
            case "SEC_TYPES":
                return (Class<CRD>) SecTypes.class;
            case "SEC_TYPE_RULES":
                return (Class<CRD>) SecTypeRules.class;
            case "SPACE_HISTORY":
                return (Class<CRD>) SpaceHistory.class;
            case "STOR_POOL_DEFINITIONS":
                return (Class<CRD>) StorPoolDefinitions.class;
            case "TRACKING_DATE":
                return (Class<CRD>) TrackingDate.class;
            case "VOLUMES":
                return (Class<CRD>) Volumes.class;
            case "VOLUME_CONNECTIONS":
                return (Class<CRD>) VolumeConnections.class;
            case "VOLUME_DEFINITIONS":
                return (Class<CRD>) VolumeDefinitions.class;
            case "VOLUME_GROUPS":
                return (Class<CRD>) VolumeGroups.class;
            default:
                // we are most likely iterating tables the current version does not know about.
                return null;
        }
    }

    @SuppressWarnings("unchecked")
    public static <SPEC extends LinstorSpec> Class<SPEC> databaseTableToSpecClass(
        DatabaseTable table
    )
    {
        switch(table.getName())
        {
            case "FILES":
                return (Class<SPEC>) FilesSpec.class;
            case "KEY_VALUE_STORE":
                return (Class<SPEC>) KeyValueStoreSpec.class;
            case "LAYER_BCACHE_VOLUMES":
                return (Class<SPEC>) LayerBcacheVolumesSpec.class;
            case "LAYER_CACHE_VOLUMES":
                return (Class<SPEC>) LayerCacheVolumesSpec.class;
            case "LAYER_DRBD_RESOURCES":
                return (Class<SPEC>) LayerDrbdResourcesSpec.class;
            case "LAYER_DRBD_RESOURCE_DEFINITIONS":
                return (Class<SPEC>) LayerDrbdResourceDefinitionsSpec.class;
            case "LAYER_DRBD_VOLUMES":
                return (Class<SPEC>) LayerDrbdVolumesSpec.class;
            case "LAYER_DRBD_VOLUME_DEFINITIONS":
                return (Class<SPEC>) LayerDrbdVolumeDefinitionsSpec.class;
            case "LAYER_LUKS_VOLUMES":
                return (Class<SPEC>) LayerLuksVolumesSpec.class;
            case "LAYER_OPENFLEX_RESOURCE_DEFINITIONS":
                return (Class<SPEC>) LayerOpenflexResourceDefinitionsSpec.class;
            case "LAYER_OPENFLEX_VOLUMES":
                return (Class<SPEC>) LayerOpenflexVolumesSpec.class;
            case "LAYER_RESOURCE_IDS":
                return (Class<SPEC>) LayerResourceIdsSpec.class;
            case "LAYER_STORAGE_VOLUMES":
                return (Class<SPEC>) LayerStorageVolumesSpec.class;
            case "LAYER_WRITECACHE_VOLUMES":
                return (Class<SPEC>) LayerWritecacheVolumesSpec.class;
            case "LINSTOR_REMOTES":
                return (Class<SPEC>) LinstorRemotesSpec.class;
            case "NODES":
                return (Class<SPEC>) NodesSpec.class;
            case "NODE_CONNECTIONS":
                return (Class<SPEC>) NodeConnectionsSpec.class;
            case "NODE_NET_INTERFACES":
                return (Class<SPEC>) NodeNetInterfacesSpec.class;
            case "NODE_STOR_POOL":
                return (Class<SPEC>) NodeStorPoolSpec.class;
            case "PROPS_CONTAINERS":
                return (Class<SPEC>) PropsContainersSpec.class;
            case "RESOURCES":
                return (Class<SPEC>) ResourcesSpec.class;
            case "RESOURCE_CONNECTIONS":
                return (Class<SPEC>) ResourceConnectionsSpec.class;
            case "RESOURCE_DEFINITIONS":
                return (Class<SPEC>) ResourceDefinitionsSpec.class;
            case "RESOURCE_GROUPS":
                return (Class<SPEC>) ResourceGroupsSpec.class;
            case "S3_REMOTES":
                return (Class<SPEC>) S3RemotesSpec.class;
            case "SATELLITES_CAPACITY":
                return (Class<SPEC>) SatellitesCapacitySpec.class;
            case "SCHEDULES":
                return (Class<SPEC>) SchedulesSpec.class;
            case "SEC_ACCESS_TYPES":
                return (Class<SPEC>) SecAccessTypesSpec.class;
            case "SEC_ACL_MAP":
                return (Class<SPEC>) SecAclMapSpec.class;
            case "SEC_CONFIGURATION":
                return (Class<SPEC>) SecConfigurationSpec.class;
            case "SEC_DFLT_ROLES":
                return (Class<SPEC>) SecDfltRolesSpec.class;
            case "SEC_IDENTITIES":
                return (Class<SPEC>) SecIdentitiesSpec.class;
            case "SEC_ID_ROLE_MAP":
                return (Class<SPEC>) SecIdRoleMapSpec.class;
            case "SEC_OBJECT_PROTECTION":
                return (Class<SPEC>) SecObjectProtectionSpec.class;
            case "SEC_ROLES":
                return (Class<SPEC>) SecRolesSpec.class;
            case "SEC_TYPES":
                return (Class<SPEC>) SecTypesSpec.class;
            case "SEC_TYPE_RULES":
                return (Class<SPEC>) SecTypeRulesSpec.class;
            case "SPACE_HISTORY":
                return (Class<SPEC>) SpaceHistorySpec.class;
            case "STOR_POOL_DEFINITIONS":
                return (Class<SPEC>) StorPoolDefinitionsSpec.class;
            case "TRACKING_DATE":
                return (Class<SPEC>) TrackingDateSpec.class;
            case "VOLUMES":
                return (Class<SPEC>) VolumesSpec.class;
            case "VOLUME_CONNECTIONS":
                return (Class<SPEC>) VolumeConnectionsSpec.class;
            case "VOLUME_DEFINITIONS":
                return (Class<SPEC>) VolumeDefinitionsSpec.class;
            case "VOLUME_GROUPS":
                return (Class<SPEC>) VolumeGroupsSpec.class;
            default:
                // we are most likely iterating tables the current version does not know about.
                return null;
        }
    }

    @SuppressWarnings("unchecked")
    public static <SPEC extends LinstorSpec> LinstorCrd<SPEC> specToCrd(SPEC spec)
    {
        switch(spec.getDatabaseTable().getName())
        {
            case "FILES":
                return (LinstorCrd<SPEC>) new Files((FilesSpec) spec);
            case "KEY_VALUE_STORE":
                return (LinstorCrd<SPEC>) new KeyValueStore((KeyValueStoreSpec) spec);
            case "LAYER_BCACHE_VOLUMES":
                return (LinstorCrd<SPEC>) new LayerBcacheVolumes((LayerBcacheVolumesSpec) spec);
            case "LAYER_CACHE_VOLUMES":
                return (LinstorCrd<SPEC>) new LayerCacheVolumes((LayerCacheVolumesSpec) spec);
            case "LAYER_DRBD_RESOURCES":
                return (LinstorCrd<SPEC>) new LayerDrbdResources((LayerDrbdResourcesSpec) spec);
            case "LAYER_DRBD_RESOURCE_DEFINITIONS":
                return (LinstorCrd<SPEC>) new LayerDrbdResourceDefinitions((LayerDrbdResourceDefinitionsSpec) spec);
            case "LAYER_DRBD_VOLUMES":
                return (LinstorCrd<SPEC>) new LayerDrbdVolumes((LayerDrbdVolumesSpec) spec);
            case "LAYER_DRBD_VOLUME_DEFINITIONS":
                return (LinstorCrd<SPEC>) new LayerDrbdVolumeDefinitions((LayerDrbdVolumeDefinitionsSpec) spec);
            case "LAYER_LUKS_VOLUMES":
                return (LinstorCrd<SPEC>) new LayerLuksVolumes((LayerLuksVolumesSpec) spec);
            case "LAYER_OPENFLEX_RESOURCE_DEFINITIONS":
                return (LinstorCrd<SPEC>) new LayerOpenflexResourceDefinitions((LayerOpenflexResourceDefinitionsSpec) spec);
            case "LAYER_OPENFLEX_VOLUMES":
                return (LinstorCrd<SPEC>) new LayerOpenflexVolumes((LayerOpenflexVolumesSpec) spec);
            case "LAYER_RESOURCE_IDS":
                return (LinstorCrd<SPEC>) new LayerResourceIds((LayerResourceIdsSpec) spec);
            case "LAYER_STORAGE_VOLUMES":
                return (LinstorCrd<SPEC>) new LayerStorageVolumes((LayerStorageVolumesSpec) spec);
            case "LAYER_WRITECACHE_VOLUMES":
                return (LinstorCrd<SPEC>) new LayerWritecacheVolumes((LayerWritecacheVolumesSpec) spec);
            case "LINSTOR_REMOTES":
                return (LinstorCrd<SPEC>) new LinstorRemotes((LinstorRemotesSpec) spec);
            case "NODES":
                return (LinstorCrd<SPEC>) new Nodes((NodesSpec) spec);
            case "NODE_CONNECTIONS":
                return (LinstorCrd<SPEC>) new NodeConnections((NodeConnectionsSpec) spec);
            case "NODE_NET_INTERFACES":
                return (LinstorCrd<SPEC>) new NodeNetInterfaces((NodeNetInterfacesSpec) spec);
            case "NODE_STOR_POOL":
                return (LinstorCrd<SPEC>) new NodeStorPool((NodeStorPoolSpec) spec);
            case "PROPS_CONTAINERS":
                return (LinstorCrd<SPEC>) new PropsContainers((PropsContainersSpec) spec);
            case "RESOURCES":
                return (LinstorCrd<SPEC>) new Resources((ResourcesSpec) spec);
            case "RESOURCE_CONNECTIONS":
                return (LinstorCrd<SPEC>) new ResourceConnections((ResourceConnectionsSpec) spec);
            case "RESOURCE_DEFINITIONS":
                return (LinstorCrd<SPEC>) new ResourceDefinitions((ResourceDefinitionsSpec) spec);
            case "RESOURCE_GROUPS":
                return (LinstorCrd<SPEC>) new ResourceGroups((ResourceGroupsSpec) spec);
            case "S3_REMOTES":
                return (LinstorCrd<SPEC>) new S3Remotes((S3RemotesSpec) spec);
            case "SATELLITES_CAPACITY":
                return (LinstorCrd<SPEC>) new SatellitesCapacity((SatellitesCapacitySpec) spec);
            case "SCHEDULES":
                return (LinstorCrd<SPEC>) new Schedules((SchedulesSpec) spec);
            case "SEC_ACCESS_TYPES":
                return (LinstorCrd<SPEC>) new SecAccessTypes((SecAccessTypesSpec) spec);
            case "SEC_ACL_MAP":
                return (LinstorCrd<SPEC>) new SecAclMap((SecAclMapSpec) spec);
            case "SEC_CONFIGURATION":
                return (LinstorCrd<SPEC>) new SecConfiguration((SecConfigurationSpec) spec);
            case "SEC_DFLT_ROLES":
                return (LinstorCrd<SPEC>) new SecDfltRoles((SecDfltRolesSpec) spec);
            case "SEC_IDENTITIES":
                return (LinstorCrd<SPEC>) new SecIdentities((SecIdentitiesSpec) spec);
            case "SEC_ID_ROLE_MAP":
                return (LinstorCrd<SPEC>) new SecIdRoleMap((SecIdRoleMapSpec) spec);
            case "SEC_OBJECT_PROTECTION":
                return (LinstorCrd<SPEC>) new SecObjectProtection((SecObjectProtectionSpec) spec);
            case "SEC_ROLES":
                return (LinstorCrd<SPEC>) new SecRoles((SecRolesSpec) spec);
            case "SEC_TYPES":
                return (LinstorCrd<SPEC>) new SecTypes((SecTypesSpec) spec);
            case "SEC_TYPE_RULES":
                return (LinstorCrd<SPEC>) new SecTypeRules((SecTypeRulesSpec) spec);
            case "SPACE_HISTORY":
                return (LinstorCrd<SPEC>) new SpaceHistory((SpaceHistorySpec) spec);
            case "STOR_POOL_DEFINITIONS":
                return (LinstorCrd<SPEC>) new StorPoolDefinitions((StorPoolDefinitionsSpec) spec);
            case "TRACKING_DATE":
                return (LinstorCrd<SPEC>) new TrackingDate((TrackingDateSpec) spec);
            case "VOLUMES":
                return (LinstorCrd<SPEC>) new Volumes((VolumesSpec) spec);
            case "VOLUME_CONNECTIONS":
                return (LinstorCrd<SPEC>) new VolumeConnections((VolumeConnectionsSpec) spec);
            case "VOLUME_DEFINITIONS":
                return (LinstorCrd<SPEC>) new VolumeDefinitions((VolumeDefinitionsSpec) spec);
            case "VOLUME_GROUPS":
                return (LinstorCrd<SPEC>) new VolumeGroups((VolumeGroupsSpec) spec);
            default:
                // we are most likely iterating tables the current version does not know about.
                return null;
        }
    }

    public static <DATA> LinstorCrd<?> dataToCrd(
        DatabaseTable table,
        Map<Column, ExceptionThrowingFunction<DATA, Object, AccessDeniedException>> setters,
        DATA data
    )
        throws AccessDeniedException
    {
        switch(table.getName())
        {
            case "FILES":
            {
                return new Files(
                    new FilesSpec(
                        (String) setters.get(GeneratedDatabaseTables.Files.UUID).accept(data),
                        (String) setters.get(GeneratedDatabaseTables.Files.PATH).accept(data),
                        (long) setters.get(GeneratedDatabaseTables.Files.FLAGS).accept(data),
                        (byte[]) setters.get(GeneratedDatabaseTables.Files.CONTENT).accept(data),
                        (String) setters.get(GeneratedDatabaseTables.Files.CONTENT_CHECKSUM).accept(data)
                    )
                );
            }
            case "KEY_VALUE_STORE":
            {
                return new KeyValueStore(
                    new KeyValueStoreSpec(
                        (String) setters.get(GeneratedDatabaseTables.KeyValueStore.UUID).accept(data),
                        (String) setters.get(GeneratedDatabaseTables.KeyValueStore.KVS_NAME).accept(data),
                        (String) setters.get(GeneratedDatabaseTables.KeyValueStore.KVS_DSP_NAME).accept(data)
                    )
                );
            }
            case "LAYER_BCACHE_VOLUMES":
            {
                return new LayerBcacheVolumes(
                    new LayerBcacheVolumesSpec(
                        (int) setters.get(GeneratedDatabaseTables.LayerBcacheVolumes.LAYER_RESOURCE_ID).accept(data),
                        (int) setters.get(GeneratedDatabaseTables.LayerBcacheVolumes.VLM_NR).accept(data),
                        (String) setters.get(GeneratedDatabaseTables.LayerBcacheVolumes.NODE_NAME).accept(data),
                        (String) setters.get(GeneratedDatabaseTables.LayerBcacheVolumes.POOL_NAME).accept(data),
                        (String) setters.get(GeneratedDatabaseTables.LayerBcacheVolumes.DEV_UUID).accept(data)
                    )
                );
            }
            case "LAYER_CACHE_VOLUMES":
            {
                return new LayerCacheVolumes(
                    new LayerCacheVolumesSpec(
                        (int) setters.get(GeneratedDatabaseTables.LayerCacheVolumes.LAYER_RESOURCE_ID).accept(data),
                        (int) setters.get(GeneratedDatabaseTables.LayerCacheVolumes.VLM_NR).accept(data),
                        (String) setters.get(GeneratedDatabaseTables.LayerCacheVolumes.NODE_NAME).accept(data),
                        (String) setters.get(GeneratedDatabaseTables.LayerCacheVolumes.POOL_NAME_CACHE).accept(data),
                        (String) setters.get(GeneratedDatabaseTables.LayerCacheVolumes.POOL_NAME_META).accept(data)
                    )
                );
            }
            case "LAYER_DRBD_RESOURCES":
            {
                return new LayerDrbdResources(
                    new LayerDrbdResourcesSpec(
                        (int) setters.get(GeneratedDatabaseTables.LayerDrbdResources.LAYER_RESOURCE_ID).accept(data),
                        (int) setters.get(GeneratedDatabaseTables.LayerDrbdResources.PEER_SLOTS).accept(data),
                        (int) setters.get(GeneratedDatabaseTables.LayerDrbdResources.AL_STRIPES).accept(data),
                        (long) setters.get(GeneratedDatabaseTables.LayerDrbdResources.AL_STRIPE_SIZE).accept(data),
                        (long) setters.get(GeneratedDatabaseTables.LayerDrbdResources.FLAGS).accept(data),
                        (int) setters.get(GeneratedDatabaseTables.LayerDrbdResources.NODE_ID).accept(data)
                    )
                );
            }
            case "LAYER_DRBD_RESOURCE_DEFINITIONS":
            {
                return new LayerDrbdResourceDefinitions(
                    new LayerDrbdResourceDefinitionsSpec(
                        (String) setters.get(GeneratedDatabaseTables.LayerDrbdResourceDefinitions.RESOURCE_NAME).accept(data),
                        (String) setters.get(GeneratedDatabaseTables.LayerDrbdResourceDefinitions.RESOURCE_NAME_SUFFIX).accept(data),
                        (String) setters.get(GeneratedDatabaseTables.LayerDrbdResourceDefinitions.SNAPSHOT_NAME).accept(data),
                        (int) setters.get(GeneratedDatabaseTables.LayerDrbdResourceDefinitions.PEER_SLOTS).accept(data),
                        (int) setters.get(GeneratedDatabaseTables.LayerDrbdResourceDefinitions.AL_STRIPES).accept(data),
                        (long) setters.get(GeneratedDatabaseTables.LayerDrbdResourceDefinitions.AL_STRIPE_SIZE).accept(data),
                        (Integer) setters.get(GeneratedDatabaseTables.LayerDrbdResourceDefinitions.TCP_PORT).accept(data),
                        (String) setters.get(GeneratedDatabaseTables.LayerDrbdResourceDefinitions.TRANSPORT_TYPE).accept(data),
                        (String) setters.get(GeneratedDatabaseTables.LayerDrbdResourceDefinitions.SECRET).accept(data)
                    )
                );
            }
            case "LAYER_DRBD_VOLUMES":
            {
                return new LayerDrbdVolumes(
                    new LayerDrbdVolumesSpec(
                        (int) setters.get(GeneratedDatabaseTables.LayerDrbdVolumes.LAYER_RESOURCE_ID).accept(data),
                        (int) setters.get(GeneratedDatabaseTables.LayerDrbdVolumes.VLM_NR).accept(data),
                        (String) setters.get(GeneratedDatabaseTables.LayerDrbdVolumes.NODE_NAME).accept(data),
                        (String) setters.get(GeneratedDatabaseTables.LayerDrbdVolumes.POOL_NAME).accept(data)
                    )
                );
            }
            case "LAYER_DRBD_VOLUME_DEFINITIONS":
            {
                return new LayerDrbdVolumeDefinitions(
                    new LayerDrbdVolumeDefinitionsSpec(
                        (String) setters.get(GeneratedDatabaseTables.LayerDrbdVolumeDefinitions.RESOURCE_NAME).accept(data),
                        (String) setters.get(GeneratedDatabaseTables.LayerDrbdVolumeDefinitions.RESOURCE_NAME_SUFFIX).accept(data),
                        (String) setters.get(GeneratedDatabaseTables.LayerDrbdVolumeDefinitions.SNAPSHOT_NAME).accept(data),
                        (int) setters.get(GeneratedDatabaseTables.LayerDrbdVolumeDefinitions.VLM_NR).accept(data),
                        (Integer) setters.get(GeneratedDatabaseTables.LayerDrbdVolumeDefinitions.VLM_MINOR_NR).accept(data)
                    )
                );
            }
            case "LAYER_LUKS_VOLUMES":
            {
                return new LayerLuksVolumes(
                    new LayerLuksVolumesSpec(
                        (int) setters.get(GeneratedDatabaseTables.LayerLuksVolumes.LAYER_RESOURCE_ID).accept(data),
                        (int) setters.get(GeneratedDatabaseTables.LayerLuksVolumes.VLM_NR).accept(data),
                        (String) setters.get(GeneratedDatabaseTables.LayerLuksVolumes.ENCRYPTED_PASSWORD).accept(data)
                    )
                );
            }
            case "LAYER_OPENFLEX_RESOURCE_DEFINITIONS":
            {
                return new LayerOpenflexResourceDefinitions(
                    new LayerOpenflexResourceDefinitionsSpec(
                        (String) setters.get(GeneratedDatabaseTables.LayerOpenflexResourceDefinitions.RESOURCE_NAME).accept(data),
                        (String) setters.get(GeneratedDatabaseTables.LayerOpenflexResourceDefinitions.SNAPSHOT_NAME).accept(data),
                        (String) setters.get(GeneratedDatabaseTables.LayerOpenflexResourceDefinitions.RESOURCE_NAME_SUFFIX).accept(data),
                        (String) setters.get(GeneratedDatabaseTables.LayerOpenflexResourceDefinitions.NQN).accept(data)
                    )
                );
            }
            case "LAYER_OPENFLEX_VOLUMES":
            {
                return new LayerOpenflexVolumes(
                    new LayerOpenflexVolumesSpec(
                        (int) setters.get(GeneratedDatabaseTables.LayerOpenflexVolumes.LAYER_RESOURCE_ID).accept(data),
                        (int) setters.get(GeneratedDatabaseTables.LayerOpenflexVolumes.VLM_NR).accept(data),
                        (String) setters.get(GeneratedDatabaseTables.LayerOpenflexVolumes.NODE_NAME).accept(data),
                        (String) setters.get(GeneratedDatabaseTables.LayerOpenflexVolumes.POOL_NAME).accept(data)
                    )
                );
            }
            case "LAYER_RESOURCE_IDS":
            {
                return new LayerResourceIds(
                    new LayerResourceIdsSpec(
                        (int) setters.get(GeneratedDatabaseTables.LayerResourceIds.LAYER_RESOURCE_ID).accept(data),
                        (String) setters.get(GeneratedDatabaseTables.LayerResourceIds.NODE_NAME).accept(data),
                        (String) setters.get(GeneratedDatabaseTables.LayerResourceIds.RESOURCE_NAME).accept(data),
                        (String) setters.get(GeneratedDatabaseTables.LayerResourceIds.SNAPSHOT_NAME).accept(data),
                        (String) setters.get(GeneratedDatabaseTables.LayerResourceIds.LAYER_RESOURCE_KIND).accept(data),
                        (Integer) setters.get(GeneratedDatabaseTables.LayerResourceIds.LAYER_RESOURCE_PARENT_ID).accept(data),
                        (String) setters.get(GeneratedDatabaseTables.LayerResourceIds.LAYER_RESOURCE_SUFFIX).accept(data),
                        (boolean) setters.get(GeneratedDatabaseTables.LayerResourceIds.LAYER_RESOURCE_SUSPENDED).accept(data)
                    )
                );
            }
            case "LAYER_STORAGE_VOLUMES":
            {
                return new LayerStorageVolumes(
                    new LayerStorageVolumesSpec(
                        (int) setters.get(GeneratedDatabaseTables.LayerStorageVolumes.LAYER_RESOURCE_ID).accept(data),
                        (int) setters.get(GeneratedDatabaseTables.LayerStorageVolumes.VLM_NR).accept(data),
                        (String) setters.get(GeneratedDatabaseTables.LayerStorageVolumes.PROVIDER_KIND).accept(data),
                        (String) setters.get(GeneratedDatabaseTables.LayerStorageVolumes.NODE_NAME).accept(data),
                        (String) setters.get(GeneratedDatabaseTables.LayerStorageVolumes.STOR_POOL_NAME).accept(data)
                    )
                );
            }
            case "LAYER_WRITECACHE_VOLUMES":
            {
                return new LayerWritecacheVolumes(
                    new LayerWritecacheVolumesSpec(
                        (int) setters.get(GeneratedDatabaseTables.LayerWritecacheVolumes.LAYER_RESOURCE_ID).accept(data),
                        (int) setters.get(GeneratedDatabaseTables.LayerWritecacheVolumes.VLM_NR).accept(data),
                        (String) setters.get(GeneratedDatabaseTables.LayerWritecacheVolumes.NODE_NAME).accept(data),
                        (String) setters.get(GeneratedDatabaseTables.LayerWritecacheVolumes.POOL_NAME).accept(data)
                    )
                );
            }
            case "LINSTOR_REMOTES":
            {
                return new LinstorRemotes(
                    new LinstorRemotesSpec(
                        (String) setters.get(GeneratedDatabaseTables.LinstorRemotes.UUID).accept(data),
                        (String) setters.get(GeneratedDatabaseTables.LinstorRemotes.NAME).accept(data),
                        (String) setters.get(GeneratedDatabaseTables.LinstorRemotes.DSP_NAME).accept(data),
                        (long) setters.get(GeneratedDatabaseTables.LinstorRemotes.FLAGS).accept(data),
                        (String) setters.get(GeneratedDatabaseTables.LinstorRemotes.URL).accept(data),
                        (byte[]) setters.get(GeneratedDatabaseTables.LinstorRemotes.ENCRYPTED_PASSPHRASE).accept(data),
                        (String) setters.get(GeneratedDatabaseTables.LinstorRemotes.CLUSTER_ID).accept(data)
                    )
                );
            }
            case "NODES":
            {
                return new Nodes(
                    new NodesSpec(
                        (String) setters.get(GeneratedDatabaseTables.Nodes.UUID).accept(data),
                        (String) setters.get(GeneratedDatabaseTables.Nodes.NODE_NAME).accept(data),
                        (String) setters.get(GeneratedDatabaseTables.Nodes.NODE_DSP_NAME).accept(data),
                        (long) setters.get(GeneratedDatabaseTables.Nodes.NODE_FLAGS).accept(data),
                        (int) setters.get(GeneratedDatabaseTables.Nodes.NODE_TYPE).accept(data)
                    )
                );
            }
            case "NODE_CONNECTIONS":
            {
                return new NodeConnections(
                    new NodeConnectionsSpec(
                        (String) setters.get(GeneratedDatabaseTables.NodeConnections.UUID).accept(data),
                        (String) setters.get(GeneratedDatabaseTables.NodeConnections.NODE_NAME_SRC).accept(data),
                        (String) setters.get(GeneratedDatabaseTables.NodeConnections.NODE_NAME_DST).accept(data)
                    )
                );
            }
            case "NODE_NET_INTERFACES":
            {
                return new NodeNetInterfaces(
                    new NodeNetInterfacesSpec(
                        (String) setters.get(GeneratedDatabaseTables.NodeNetInterfaces.UUID).accept(data),
                        (String) setters.get(GeneratedDatabaseTables.NodeNetInterfaces.NODE_NAME).accept(data),
                        (String) setters.get(GeneratedDatabaseTables.NodeNetInterfaces.NODE_NET_NAME).accept(data),
                        (String) setters.get(GeneratedDatabaseTables.NodeNetInterfaces.NODE_NET_DSP_NAME).accept(data),
                        (String) setters.get(GeneratedDatabaseTables.NodeNetInterfaces.INET_ADDRESS).accept(data),
                        (Short) setters.get(GeneratedDatabaseTables.NodeNetInterfaces.STLT_CONN_PORT).accept(data),
                        (String) setters.get(GeneratedDatabaseTables.NodeNetInterfaces.STLT_CONN_ENCR_TYPE).accept(data)
                    )
                );
            }
            case "NODE_STOR_POOL":
            {
                return new NodeStorPool(
                    new NodeStorPoolSpec(
                        (String) setters.get(GeneratedDatabaseTables.NodeStorPool.UUID).accept(data),
                        (String) setters.get(GeneratedDatabaseTables.NodeStorPool.NODE_NAME).accept(data),
                        (String) setters.get(GeneratedDatabaseTables.NodeStorPool.POOL_NAME).accept(data),
                        (String) setters.get(GeneratedDatabaseTables.NodeStorPool.DRIVER_NAME).accept(data),
                        (String) setters.get(GeneratedDatabaseTables.NodeStorPool.FREE_SPACE_MGR_NAME).accept(data),
                        (String) setters.get(GeneratedDatabaseTables.NodeStorPool.FREE_SPACE_MGR_DSP_NAME).accept(data),
                        (boolean) setters.get(GeneratedDatabaseTables.NodeStorPool.EXTERNAL_LOCKING).accept(data)
                    )
                );
            }
            case "PROPS_CONTAINERS":
            {
                return new PropsContainers(
                    new PropsContainersSpec(
                        (String) setters.get(GeneratedDatabaseTables.PropsContainers.PROPS_INSTANCE).accept(data),
                        (String) setters.get(GeneratedDatabaseTables.PropsContainers.PROP_KEY).accept(data),
                        (String) setters.get(GeneratedDatabaseTables.PropsContainers.PROP_VALUE).accept(data)
                    )
                );
            }
            case "RESOURCES":
            {
                return new Resources(
                    new ResourcesSpec(
                        (String) setters.get(GeneratedDatabaseTables.Resources.UUID).accept(data),
                        (String) setters.get(GeneratedDatabaseTables.Resources.NODE_NAME).accept(data),
                        (String) setters.get(GeneratedDatabaseTables.Resources.RESOURCE_NAME).accept(data),
                        (String) setters.get(GeneratedDatabaseTables.Resources.SNAPSHOT_NAME).accept(data),
                        (long) setters.get(GeneratedDatabaseTables.Resources.RESOURCE_FLAGS).accept(data),
                        (Long) setters.get(GeneratedDatabaseTables.Resources.CREATE_TIMESTAMP).accept(data)
                    )
                );
            }
            case "RESOURCE_CONNECTIONS":
            {
                return new ResourceConnections(
                    new ResourceConnectionsSpec(
                        (String) setters.get(GeneratedDatabaseTables.ResourceConnections.UUID).accept(data),
                        (String) setters.get(GeneratedDatabaseTables.ResourceConnections.NODE_NAME_SRC).accept(data),
                        (String) setters.get(GeneratedDatabaseTables.ResourceConnections.NODE_NAME_DST).accept(data),
                        (String) setters.get(GeneratedDatabaseTables.ResourceConnections.RESOURCE_NAME).accept(data),
                        (String) setters.get(GeneratedDatabaseTables.ResourceConnections.SNAPSHOT_NAME).accept(data),
                        (long) setters.get(GeneratedDatabaseTables.ResourceConnections.FLAGS).accept(data),
                        (Integer) setters.get(GeneratedDatabaseTables.ResourceConnections.TCP_PORT).accept(data)
                    )
                );
            }
            case "RESOURCE_DEFINITIONS":
            {
                return new ResourceDefinitions(
                    new ResourceDefinitionsSpec(
                        (String) setters.get(GeneratedDatabaseTables.ResourceDefinitions.UUID).accept(data),
                        (String) setters.get(GeneratedDatabaseTables.ResourceDefinitions.RESOURCE_NAME).accept(data),
                        (String) setters.get(GeneratedDatabaseTables.ResourceDefinitions.SNAPSHOT_NAME).accept(data),
                        (String) setters.get(GeneratedDatabaseTables.ResourceDefinitions.RESOURCE_DSP_NAME).accept(data),
                        (String) setters.get(GeneratedDatabaseTables.ResourceDefinitions.SNAPSHOT_DSP_NAME).accept(data),
                        (long) setters.get(GeneratedDatabaseTables.ResourceDefinitions.RESOURCE_FLAGS).accept(data),
                        (String) setters.get(GeneratedDatabaseTables.ResourceDefinitions.LAYER_STACK).accept(data),
                        (byte[]) setters.get(GeneratedDatabaseTables.ResourceDefinitions.RESOURCE_EXTERNAL_NAME).accept(data),
                        (String) setters.get(GeneratedDatabaseTables.ResourceDefinitions.RESOURCE_GROUP_NAME).accept(data),
                        (String) setters.get(GeneratedDatabaseTables.ResourceDefinitions.PARENT_UUID).accept(data)
                    )
                );
            }
            case "RESOURCE_GROUPS":
            {
                return new ResourceGroups(
                    new ResourceGroupsSpec(
                        (String) setters.get(GeneratedDatabaseTables.ResourceGroups.UUID).accept(data),
                        (String) setters.get(GeneratedDatabaseTables.ResourceGroups.RESOURCE_GROUP_NAME).accept(data),
                        (String) setters.get(GeneratedDatabaseTables.ResourceGroups.RESOURCE_GROUP_DSP_NAME).accept(data),
                        (String) setters.get(GeneratedDatabaseTables.ResourceGroups.DESCRIPTION).accept(data),
                        (String) setters.get(GeneratedDatabaseTables.ResourceGroups.LAYER_STACK).accept(data),
                        (int) setters.get(GeneratedDatabaseTables.ResourceGroups.REPLICA_COUNT).accept(data),
                        (String) setters.get(GeneratedDatabaseTables.ResourceGroups.NODE_NAME_LIST).accept(data),
                        (String) setters.get(GeneratedDatabaseTables.ResourceGroups.POOL_NAME).accept(data),
                        (String) setters.get(GeneratedDatabaseTables.ResourceGroups.POOL_NAME_DISKLESS).accept(data),
                        (String) setters.get(GeneratedDatabaseTables.ResourceGroups.DO_NOT_PLACE_WITH_RSC_REGEX).accept(data),
                        (String) setters.get(GeneratedDatabaseTables.ResourceGroups.DO_NOT_PLACE_WITH_RSC_LIST).accept(data),
                        (String) setters.get(GeneratedDatabaseTables.ResourceGroups.REPLICAS_ON_SAME).accept(data),
                        (String) setters.get(GeneratedDatabaseTables.ResourceGroups.REPLICAS_ON_DIFFERENT).accept(data),
                        (String) setters.get(GeneratedDatabaseTables.ResourceGroups.ALLOWED_PROVIDER_LIST).accept(data),
                        (Boolean) setters.get(GeneratedDatabaseTables.ResourceGroups.DISKLESS_ON_REMAINING).accept(data)
                    )
                );
            }
            case "S3_REMOTES":
            {
                return new S3Remotes(
                    new S3RemotesSpec(
                        (String) setters.get(GeneratedDatabaseTables.S3Remotes.UUID).accept(data),
                        (String) setters.get(GeneratedDatabaseTables.S3Remotes.NAME).accept(data),
                        (String) setters.get(GeneratedDatabaseTables.S3Remotes.DSP_NAME).accept(data),
                        (long) setters.get(GeneratedDatabaseTables.S3Remotes.FLAGS).accept(data),
                        (String) setters.get(GeneratedDatabaseTables.S3Remotes.ENDPOINT).accept(data),
                        (String) setters.get(GeneratedDatabaseTables.S3Remotes.BUCKET).accept(data),
                        (String) setters.get(GeneratedDatabaseTables.S3Remotes.REGION).accept(data),
                        (byte[]) setters.get(GeneratedDatabaseTables.S3Remotes.ACCESS_KEY).accept(data),
                        (byte[]) setters.get(GeneratedDatabaseTables.S3Remotes.SECRET_KEY).accept(data)
                    )
                );
            }
            case "SATELLITES_CAPACITY":
            {
                return new SatellitesCapacity(
                    new SatellitesCapacitySpec(
                        (String) setters.get(GeneratedDatabaseTables.SatellitesCapacity.NODE_NAME).accept(data),
                        (byte[]) setters.get(GeneratedDatabaseTables.SatellitesCapacity.CAPACITY).accept(data),
                        (boolean) setters.get(GeneratedDatabaseTables.SatellitesCapacity.FAIL_FLAG).accept(data),
                        (byte[]) setters.get(GeneratedDatabaseTables.SatellitesCapacity.ALLOCATED).accept(data),
                        (byte[]) setters.get(GeneratedDatabaseTables.SatellitesCapacity.USABLE).accept(data)
                    )
                );
            }
            case "SCHEDULES":
            {
                return new Schedules(
                    new SchedulesSpec(
                        (String) setters.get(GeneratedDatabaseTables.Schedules.UUID).accept(data),
                        (String) setters.get(GeneratedDatabaseTables.Schedules.NAME).accept(data),
                        (String) setters.get(GeneratedDatabaseTables.Schedules.DSP_NAME).accept(data),
                        (long) setters.get(GeneratedDatabaseTables.Schedules.FLAGS).accept(data),
                        (String) setters.get(GeneratedDatabaseTables.Schedules.FULL_CRON).accept(data),
                        (String) setters.get(GeneratedDatabaseTables.Schedules.INC_CRON).accept(data),
                        (Integer) setters.get(GeneratedDatabaseTables.Schedules.KEEP_LOCAL).accept(data),
                        (Integer) setters.get(GeneratedDatabaseTables.Schedules.KEEP_REMOTE).accept(data),
                        (long) setters.get(GeneratedDatabaseTables.Schedules.ON_FAILURE).accept(data),
                        (Integer) setters.get(GeneratedDatabaseTables.Schedules.MAX_RETRIES).accept(data)
                    )
                );
            }
            case "SEC_ACCESS_TYPES":
            {
                return new SecAccessTypes(
                    new SecAccessTypesSpec(
                        (String) setters.get(GeneratedDatabaseTables.SecAccessTypes.ACCESS_TYPE_NAME).accept(data),
                        (short) setters.get(GeneratedDatabaseTables.SecAccessTypes.ACCESS_TYPE_VALUE).accept(data)
                    )
                );
            }
            case "SEC_ACL_MAP":
            {
                return new SecAclMap(
                    new SecAclMapSpec(
                        (String) setters.get(GeneratedDatabaseTables.SecAclMap.OBJECT_PATH).accept(data),
                        (String) setters.get(GeneratedDatabaseTables.SecAclMap.ROLE_NAME).accept(data),
                        (short) setters.get(GeneratedDatabaseTables.SecAclMap.ACCESS_TYPE).accept(data)
                    )
                );
            }
            case "SEC_CONFIGURATION":
            {
                return new SecConfiguration(
                    new SecConfigurationSpec(
                        (String) setters.get(GeneratedDatabaseTables.SecConfiguration.ENTRY_KEY).accept(data),
                        (String) setters.get(GeneratedDatabaseTables.SecConfiguration.ENTRY_DSP_KEY).accept(data),
                        (String) setters.get(GeneratedDatabaseTables.SecConfiguration.ENTRY_VALUE).accept(data)
                    )
                );
            }
            case "SEC_DFLT_ROLES":
            {
                return new SecDfltRoles(
                    new SecDfltRolesSpec(
                        (String) setters.get(GeneratedDatabaseTables.SecDfltRoles.IDENTITY_NAME).accept(data),
                        (String) setters.get(GeneratedDatabaseTables.SecDfltRoles.ROLE_NAME).accept(data)
                    )
                );
            }
            case "SEC_IDENTITIES":
            {
                return new SecIdentities(
                    new SecIdentitiesSpec(
                        (String) setters.get(GeneratedDatabaseTables.SecIdentities.IDENTITY_NAME).accept(data),
                        (String) setters.get(GeneratedDatabaseTables.SecIdentities.IDENTITY_DSP_NAME).accept(data),
                        (String) setters.get(GeneratedDatabaseTables.SecIdentities.PASS_SALT).accept(data),
                        (String) setters.get(GeneratedDatabaseTables.SecIdentities.PASS_HASH).accept(data),
                        (boolean) setters.get(GeneratedDatabaseTables.SecIdentities.ID_ENABLED).accept(data),
                        (boolean) setters.get(GeneratedDatabaseTables.SecIdentities.ID_LOCKED).accept(data)
                    )
                );
            }
            case "SEC_ID_ROLE_MAP":
            {
                return new SecIdRoleMap(
                    new SecIdRoleMapSpec(
                        (String) setters.get(GeneratedDatabaseTables.SecIdRoleMap.IDENTITY_NAME).accept(data),
                        (String) setters.get(GeneratedDatabaseTables.SecIdRoleMap.ROLE_NAME).accept(data)
                    )
                );
            }
            case "SEC_OBJECT_PROTECTION":
            {
                return new SecObjectProtection(
                    new SecObjectProtectionSpec(
                        (String) setters.get(GeneratedDatabaseTables.SecObjectProtection.OBJECT_PATH).accept(data),
                        (String) setters.get(GeneratedDatabaseTables.SecObjectProtection.CREATOR_IDENTITY_NAME).accept(data),
                        (String) setters.get(GeneratedDatabaseTables.SecObjectProtection.OWNER_ROLE_NAME).accept(data),
                        (String) setters.get(GeneratedDatabaseTables.SecObjectProtection.SECURITY_TYPE_NAME).accept(data)
                    )
                );
            }
            case "SEC_ROLES":
            {
                return new SecRoles(
                    new SecRolesSpec(
                        (String) setters.get(GeneratedDatabaseTables.SecRoles.ROLE_NAME).accept(data),
                        (String) setters.get(GeneratedDatabaseTables.SecRoles.ROLE_DSP_NAME).accept(data),
                        (String) setters.get(GeneratedDatabaseTables.SecRoles.DOMAIN_NAME).accept(data),
                        (boolean) setters.get(GeneratedDatabaseTables.SecRoles.ROLE_ENABLED).accept(data),
                        (long) setters.get(GeneratedDatabaseTables.SecRoles.ROLE_PRIVILEGES).accept(data)
                    )
                );
            }
            case "SEC_TYPES":
            {
                return new SecTypes(
                    new SecTypesSpec(
                        (String) setters.get(GeneratedDatabaseTables.SecTypes.TYPE_NAME).accept(data),
                        (String) setters.get(GeneratedDatabaseTables.SecTypes.TYPE_DSP_NAME).accept(data),
                        (boolean) setters.get(GeneratedDatabaseTables.SecTypes.TYPE_ENABLED).accept(data)
                    )
                );
            }
            case "SEC_TYPE_RULES":
            {
                return new SecTypeRules(
                    new SecTypeRulesSpec(
                        (String) setters.get(GeneratedDatabaseTables.SecTypeRules.DOMAIN_NAME).accept(data),
                        (String) setters.get(GeneratedDatabaseTables.SecTypeRules.TYPE_NAME).accept(data),
                        (short) setters.get(GeneratedDatabaseTables.SecTypeRules.ACCESS_TYPE).accept(data)
                    )
                );
            }
            case "SPACE_HISTORY":
            {
                return new SpaceHistory(
                    new SpaceHistorySpec(
                        (Date) setters.get(GeneratedDatabaseTables.SpaceHistory.ENTRY_DATE).accept(data),
                        (byte[]) setters.get(GeneratedDatabaseTables.SpaceHistory.CAPACITY).accept(data)
                    )
                );
            }
            case "STOR_POOL_DEFINITIONS":
            {
                return new StorPoolDefinitions(
                    new StorPoolDefinitionsSpec(
                        (String) setters.get(GeneratedDatabaseTables.StorPoolDefinitions.UUID).accept(data),
                        (String) setters.get(GeneratedDatabaseTables.StorPoolDefinitions.POOL_NAME).accept(data),
                        (String) setters.get(GeneratedDatabaseTables.StorPoolDefinitions.POOL_DSP_NAME).accept(data)
                    )
                );
            }
            case "TRACKING_DATE":
            {
                return new TrackingDate(
                    new TrackingDateSpec(
                        (Date) setters.get(GeneratedDatabaseTables.TrackingDate.ENTRY_DATE).accept(data)
                    )
                );
            }
            case "VOLUMES":
            {
                return new Volumes(
                    new VolumesSpec(
                        (String) setters.get(GeneratedDatabaseTables.Volumes.UUID).accept(data),
                        (String) setters.get(GeneratedDatabaseTables.Volumes.NODE_NAME).accept(data),
                        (String) setters.get(GeneratedDatabaseTables.Volumes.RESOURCE_NAME).accept(data),
                        (String) setters.get(GeneratedDatabaseTables.Volumes.SNAPSHOT_NAME).accept(data),
                        (int) setters.get(GeneratedDatabaseTables.Volumes.VLM_NR).accept(data),
                        (long) setters.get(GeneratedDatabaseTables.Volumes.VLM_FLAGS).accept(data)
                    )
                );
            }
            case "VOLUME_CONNECTIONS":
            {
                return new VolumeConnections(
                    new VolumeConnectionsSpec(
                        (String) setters.get(GeneratedDatabaseTables.VolumeConnections.UUID).accept(data),
                        (String) setters.get(GeneratedDatabaseTables.VolumeConnections.NODE_NAME_SRC).accept(data),
                        (String) setters.get(GeneratedDatabaseTables.VolumeConnections.NODE_NAME_DST).accept(data),
                        (String) setters.get(GeneratedDatabaseTables.VolumeConnections.RESOURCE_NAME).accept(data),
                        (String) setters.get(GeneratedDatabaseTables.VolumeConnections.SNAPSHOT_NAME).accept(data),
                        (int) setters.get(GeneratedDatabaseTables.VolumeConnections.VLM_NR).accept(data)
                    )
                );
            }
            case "VOLUME_DEFINITIONS":
            {
                return new VolumeDefinitions(
                    new VolumeDefinitionsSpec(
                        (String) setters.get(GeneratedDatabaseTables.VolumeDefinitions.UUID).accept(data),
                        (String) setters.get(GeneratedDatabaseTables.VolumeDefinitions.RESOURCE_NAME).accept(data),
                        (String) setters.get(GeneratedDatabaseTables.VolumeDefinitions.SNAPSHOT_NAME).accept(data),
                        (int) setters.get(GeneratedDatabaseTables.VolumeDefinitions.VLM_NR).accept(data),
                        (long) setters.get(GeneratedDatabaseTables.VolumeDefinitions.VLM_SIZE).accept(data),
                        (long) setters.get(GeneratedDatabaseTables.VolumeDefinitions.VLM_FLAGS).accept(data)
                    )
                );
            }
            case "VOLUME_GROUPS":
            {
                return new VolumeGroups(
                    new VolumeGroupsSpec(
                        (String) setters.get(GeneratedDatabaseTables.VolumeGroups.UUID).accept(data),
                        (String) setters.get(GeneratedDatabaseTables.VolumeGroups.RESOURCE_GROUP_NAME).accept(data),
                        (int) setters.get(GeneratedDatabaseTables.VolumeGroups.VLM_NR).accept(data),
                        (long) setters.get(GeneratedDatabaseTables.VolumeGroups.FLAGS).accept(data)
                    )
                );
            }
            default:
                // we are most likely iterating tables the current version does not know about.
                return null;
        }
    }

    public static String databaseTableToYamlLocation(DatabaseTable dbTable)
    {
        switch(dbTable.getName())
        {
            case "FILES":
                return "/com/linbit/linstor/dbcp/k8s/crd/v1_18_2/Files.yaml";
            case "KEY_VALUE_STORE":
                return "/com/linbit/linstor/dbcp/k8s/crd/v1_18_2/KeyValueStore.yaml";
            case "LAYER_BCACHE_VOLUMES":
                return "/com/linbit/linstor/dbcp/k8s/crd/v1_18_2/LayerBcacheVolumes.yaml";
            case "LAYER_CACHE_VOLUMES":
                return "/com/linbit/linstor/dbcp/k8s/crd/v1_18_2/LayerCacheVolumes.yaml";
            case "LAYER_DRBD_RESOURCES":
                return "/com/linbit/linstor/dbcp/k8s/crd/v1_18_2/LayerDrbdResources.yaml";
            case "LAYER_DRBD_RESOURCE_DEFINITIONS":
                return "/com/linbit/linstor/dbcp/k8s/crd/v1_18_2/LayerDrbdResourceDefinitions.yaml";
            case "LAYER_DRBD_VOLUMES":
                return "/com/linbit/linstor/dbcp/k8s/crd/v1_18_2/LayerDrbdVolumes.yaml";
            case "LAYER_DRBD_VOLUME_DEFINITIONS":
                return "/com/linbit/linstor/dbcp/k8s/crd/v1_18_2/LayerDrbdVolumeDefinitions.yaml";
            case "LAYER_LUKS_VOLUMES":
                return "/com/linbit/linstor/dbcp/k8s/crd/v1_18_2/LayerLuksVolumes.yaml";
            case "LAYER_OPENFLEX_RESOURCE_DEFINITIONS":
                return "/com/linbit/linstor/dbcp/k8s/crd/v1_18_2/LayerOpenflexResourceDefinitions.yaml";
            case "LAYER_OPENFLEX_VOLUMES":
                return "/com/linbit/linstor/dbcp/k8s/crd/v1_18_2/LayerOpenflexVolumes.yaml";
            case "LAYER_RESOURCE_IDS":
                return "/com/linbit/linstor/dbcp/k8s/crd/v1_18_2/LayerResourceIds.yaml";
            case "LAYER_STORAGE_VOLUMES":
                return "/com/linbit/linstor/dbcp/k8s/crd/v1_18_2/LayerStorageVolumes.yaml";
            case "LAYER_WRITECACHE_VOLUMES":
                return "/com/linbit/linstor/dbcp/k8s/crd/v1_18_2/LayerWritecacheVolumes.yaml";
            case "LINSTOR_REMOTES":
                return "/com/linbit/linstor/dbcp/k8s/crd/v1_18_2/LinstorRemotes.yaml";
            case "NODES":
                return "/com/linbit/linstor/dbcp/k8s/crd/v1_18_2/Nodes.yaml";
            case "NODE_CONNECTIONS":
                return "/com/linbit/linstor/dbcp/k8s/crd/v1_18_2/NodeConnections.yaml";
            case "NODE_NET_INTERFACES":
                return "/com/linbit/linstor/dbcp/k8s/crd/v1_18_2/NodeNetInterfaces.yaml";
            case "NODE_STOR_POOL":
                return "/com/linbit/linstor/dbcp/k8s/crd/v1_18_2/NodeStorPool.yaml";
            case "PROPS_CONTAINERS":
                return "/com/linbit/linstor/dbcp/k8s/crd/v1_18_2/PropsContainers.yaml";
            case "RESOURCES":
                return "/com/linbit/linstor/dbcp/k8s/crd/v1_18_2/Resources.yaml";
            case "RESOURCE_CONNECTIONS":
                return "/com/linbit/linstor/dbcp/k8s/crd/v1_18_2/ResourceConnections.yaml";
            case "RESOURCE_DEFINITIONS":
                return "/com/linbit/linstor/dbcp/k8s/crd/v1_18_2/ResourceDefinitions.yaml";
            case "RESOURCE_GROUPS":
                return "/com/linbit/linstor/dbcp/k8s/crd/v1_18_2/ResourceGroups.yaml";
            case "S3_REMOTES":
                return "/com/linbit/linstor/dbcp/k8s/crd/v1_18_2/S3Remotes.yaml";
            case "SATELLITES_CAPACITY":
                return "/com/linbit/linstor/dbcp/k8s/crd/v1_18_2/SatellitesCapacity.yaml";
            case "SCHEDULES":
                return "/com/linbit/linstor/dbcp/k8s/crd/v1_18_2/Schedules.yaml";
            case "SEC_ACCESS_TYPES":
                return "/com/linbit/linstor/dbcp/k8s/crd/v1_18_2/SecAccessTypes.yaml";
            case "SEC_ACL_MAP":
                return "/com/linbit/linstor/dbcp/k8s/crd/v1_18_2/SecAclMap.yaml";
            case "SEC_CONFIGURATION":
                return "/com/linbit/linstor/dbcp/k8s/crd/v1_18_2/SecConfiguration.yaml";
            case "SEC_DFLT_ROLES":
                return "/com/linbit/linstor/dbcp/k8s/crd/v1_18_2/SecDfltRoles.yaml";
            case "SEC_IDENTITIES":
                return "/com/linbit/linstor/dbcp/k8s/crd/v1_18_2/SecIdentities.yaml";
            case "SEC_ID_ROLE_MAP":
                return "/com/linbit/linstor/dbcp/k8s/crd/v1_18_2/SecIdRoleMap.yaml";
            case "SEC_OBJECT_PROTECTION":
                return "/com/linbit/linstor/dbcp/k8s/crd/v1_18_2/SecObjectProtection.yaml";
            case "SEC_ROLES":
                return "/com/linbit/linstor/dbcp/k8s/crd/v1_18_2/SecRoles.yaml";
            case "SEC_TYPES":
                return "/com/linbit/linstor/dbcp/k8s/crd/v1_18_2/SecTypes.yaml";
            case "SEC_TYPE_RULES":
                return "/com/linbit/linstor/dbcp/k8s/crd/v1_18_2/SecTypeRules.yaml";
            case "SPACE_HISTORY":
                return "/com/linbit/linstor/dbcp/k8s/crd/v1_18_2/SpaceHistory.yaml";
            case "STOR_POOL_DEFINITIONS":
                return "/com/linbit/linstor/dbcp/k8s/crd/v1_18_2/StorPoolDefinitions.yaml";
            case "TRACKING_DATE":
                return "/com/linbit/linstor/dbcp/k8s/crd/v1_18_2/TrackingDate.yaml";
            case "VOLUMES":
                return "/com/linbit/linstor/dbcp/k8s/crd/v1_18_2/Volumes.yaml";
            case "VOLUME_CONNECTIONS":
                return "/com/linbit/linstor/dbcp/k8s/crd/v1_18_2/VolumeConnections.yaml";
            case "VOLUME_DEFINITIONS":
                return "/com/linbit/linstor/dbcp/k8s/crd/v1_18_2/VolumeDefinitions.yaml";
            case "VOLUME_GROUPS":
                return "/com/linbit/linstor/dbcp/k8s/crd/v1_18_2/VolumeGroups.yaml";
            default:
                // we are most likely iterating tables the current version does not know about.
                return null;
        }
    }

    public static String databaseTableToYamlName(DatabaseTable dbTable)
    {
        switch(dbTable.getName())
        {
            case "FILES":
                return "files";
            case "KEY_VALUE_STORE":
                return "keyvaluestore";
            case "LAYER_BCACHE_VOLUMES":
                return "layerbcachevolumes";
            case "LAYER_CACHE_VOLUMES":
                return "layercachevolumes";
            case "LAYER_DRBD_RESOURCES":
                return "layerdrbdresources";
            case "LAYER_DRBD_RESOURCE_DEFINITIONS":
                return "layerdrbdresourcedefinitions";
            case "LAYER_DRBD_VOLUMES":
                return "layerdrbdvolumes";
            case "LAYER_DRBD_VOLUME_DEFINITIONS":
                return "layerdrbdvolumedefinitions";
            case "LAYER_LUKS_VOLUMES":
                return "layerluksvolumes";
            case "LAYER_OPENFLEX_RESOURCE_DEFINITIONS":
                return "layeropenflexresourcedefinitions";
            case "LAYER_OPENFLEX_VOLUMES":
                return "layeropenflexvolumes";
            case "LAYER_RESOURCE_IDS":
                return "layerresourceids";
            case "LAYER_STORAGE_VOLUMES":
                return "layerstoragevolumes";
            case "LAYER_WRITECACHE_VOLUMES":
                return "layerwritecachevolumes";
            case "LINSTOR_REMOTES":
                return "linstorremotes";
            case "NODES":
                return "nodes";
            case "NODE_CONNECTIONS":
                return "nodeconnections";
            case "NODE_NET_INTERFACES":
                return "nodenetinterfaces";
            case "NODE_STOR_POOL":
                return "nodestorpool";
            case "PROPS_CONTAINERS":
                return "propscontainers";
            case "RESOURCES":
                return "resources";
            case "RESOURCE_CONNECTIONS":
                return "resourceconnections";
            case "RESOURCE_DEFINITIONS":
                return "resourcedefinitions";
            case "RESOURCE_GROUPS":
                return "resourcegroups";
            case "S3_REMOTES":
                return "s3remotes";
            case "SATELLITES_CAPACITY":
                return "satellitescapacity";
            case "SCHEDULES":
                return "schedules";
            case "SEC_ACCESS_TYPES":
                return "secaccesstypes";
            case "SEC_ACL_MAP":
                return "secaclmap";
            case "SEC_CONFIGURATION":
                return "secconfiguration";
            case "SEC_DFLT_ROLES":
                return "secdfltroles";
            case "SEC_IDENTITIES":
                return "secidentities";
            case "SEC_ID_ROLE_MAP":
                return "secidrolemap";
            case "SEC_OBJECT_PROTECTION":
                return "secobjectprotection";
            case "SEC_ROLES":
                return "secroles";
            case "SEC_TYPES":
                return "sectypes";
            case "SEC_TYPE_RULES":
                return "sectyperules";
            case "SPACE_HISTORY":
                return "spacehistory";
            case "STOR_POOL_DEFINITIONS":
                return "storpooldefinitions";
            case "TRACKING_DATE":
                return "trackingdate";
            case "VOLUMES":
                return "volumes";
            case "VOLUME_CONNECTIONS":
                return "volumeconnections";
            case "VOLUME_DEFINITIONS":
                return "volumedefinitions";
            case "VOLUME_GROUPS":
                return "volumegroups";
            default:
                // we are most likely iterating tables the current version does not know about.
                return null;
        }
    }

    public static BaseControllerK8sCrdTransactionMgrContext createTxMgrContext()
    {
        return new BaseControllerK8sCrdTransactionMgrContext(
            GenCrdV1_18_2::databaseTableToCustomResourceClass,
            GeneratedDatabaseTables.ALL_TABLES,
            GenCrdV1_18_2.VERSION
        );
    }

    public static K8sCrdSchemaUpdateContext createSchemaUpdateContext()
    {
        return new K8sCrdSchemaUpdateContext(
            GenCrdV1_18_2::databaseTableToYamlLocation,
            GenCrdV1_18_2::databaseTableToYamlName,
            "v1-18-2"
        );
    }

    public static K8sCrdMigrationContext createMigrationContext()
    {
        return new K8sCrdMigrationContext(createTxMgrContext(), createSchemaUpdateContext());
    }

    public static Files createFiles(
        String uuid,
        String path,
        long flags,
        byte[] content,
        String contentChecksum
    )
    {
        return new Files(
            new FilesSpec(
                uuid,
                path,
                flags,
                content,
                contentChecksum
            )
        );
    }

    @SuppressWarnings("rawtypes")
    @JsonInclude(Include.NON_NULL)
    public static class FilesSpec implements LinstorSpec
    {
        @JsonIgnore private static final long serialVersionUID = 8340579128693556176L;
        @JsonIgnore private static final String PK_FORMAT = "%s";

        @JsonIgnore private final String formattedPrimaryKey;

        @JsonProperty("uuid") public final String uuid;
        @JsonProperty("path") public final String path; // PK
        @JsonProperty("flags") public final long flags;
        @JsonProperty("content") public final byte[] content;
        @JsonProperty("content_checksum") public final String contentChecksum;

        @JsonCreator
        public FilesSpec(
            @JsonProperty("uuid") String uuidRef,
            @JsonProperty("path") String pathRef,
            @JsonProperty("flags") long flagsRef,
            @JsonProperty("content") byte[] contentRef,
            @JsonProperty("content_checksum") String contentChecksumRef
        )
        {
            uuid = uuidRef;
            path = pathRef;
            flags = flagsRef;
            content = contentRef;
            contentChecksum = contentChecksumRef;

            formattedPrimaryKey = String.format(
                FilesSpec.PK_FORMAT,
                path
            );
        }

        @JsonIgnore
        @Override
        @SuppressWarnings("rawtypes")
        public LinstorCrd getCrd()
        {
            throw new ImplementationError("Pre 1_19_1 does not support this method");
        }

        @JsonIgnore
        @Override
        public Map<String, Object> asRawParameters()
        {
            Map<String, Object> ret = new TreeMap<>();
            ret.put("UUID", uuid);
            ret.put("PATH", path);
            ret.put("FLAGS", flags);
            ret.put("CONTENT", content);
            ret.put("CONTENT_CHECKSUM", contentChecksum);
            return ret;
        }

        @JsonIgnore
        @Override
        public Object getByColumn(String clmNameStr)
        {
            switch(clmNameStr)
            {
                case "UUID":
                    return uuid;
                case "PATH":
                    return path;
                case "FLAGS":
                    return flags;
                case "CONTENT":
                    return content;
                case "CONTENT_CHECKSUM":
                    return contentChecksum;
                default:
                    throw new ImplementationError("Unknown database column. Table: FILES, Column: " + clmNameStr);
            }
        }

        @JsonIgnore
        @Override
        public final String getLinstorKey()
        {
            return formattedPrimaryKey;
        }

        @Override
        @JsonIgnore
        public DatabaseTable getDatabaseTable()
        {
            return GeneratedDatabaseTables.FILES;
        }
    }

    @Version(GenCrdV1_18_2.VERSION)
    @Group(GenCrdV1_18_2.GROUP)
    @Plural("files")
    @Singular("files")
    public static class Files extends CustomResource<FilesSpec, Void> implements LinstorCrd<FilesSpec>
    {
        private static final long serialVersionUID = 1650838928414460221L;
        String k8sKey = null;

        @JsonCreator
        public Files()
        {
        }

        public Files(FilesSpec spec)
        {
            setMetadata(new ObjectMetaBuilder().withName(deriveKey(spec.getLinstorKey())).build());
            setSpec(spec);
        }

        @Override
        public void setMetadata(ObjectMeta metadataRef)
        {
            super.setMetadata(metadataRef);
            k8sKey = metadataRef.getName();
        }

        @Override
        @JsonIgnore
        public String getLinstorKey()
        {
            return spec.getLinstorKey();
        }

        @Override
        @JsonIgnore
        public String getK8sKey()
        {
            return k8sKey;
        }
    }

    public static KeyValueStore createKeyValueStore(
        String uuid,
        String kvsName,
        String kvsDspName
    )
    {
        return new KeyValueStore(
            new KeyValueStoreSpec(
                uuid,
                kvsName,
                kvsDspName
            )
        );
    }

    @SuppressWarnings("rawtypes")
    @JsonInclude(Include.NON_NULL)
    public static class KeyValueStoreSpec implements LinstorSpec
    {
        @JsonIgnore private static final long serialVersionUID = -1756949109603969492L;
        @JsonIgnore private static final String PK_FORMAT = "%s";

        @JsonIgnore private final String formattedPrimaryKey;

        @JsonProperty("uuid") public final String uuid;
        @JsonProperty("kvs_name") public final String kvsName; // PK
        @JsonProperty("kvs_dsp_name") public final String kvsDspName;

        @JsonCreator
        public KeyValueStoreSpec(
            @JsonProperty("uuid") String uuidRef,
            @JsonProperty("kvs_name") String kvsNameRef,
            @JsonProperty("kvs_dsp_name") String kvsDspNameRef
        )
        {
            uuid = uuidRef;
            kvsName = kvsNameRef;
            kvsDspName = kvsDspNameRef;

            formattedPrimaryKey = String.format(
                KeyValueStoreSpec.PK_FORMAT,
                kvsName
            );
        }

        @JsonIgnore
        @Override
        @SuppressWarnings("rawtypes")
        public LinstorCrd getCrd()
        {
            throw new ImplementationError("Pre 1_19_1 does not support this method");
        }

        @JsonIgnore
        @Override
        public Map<String, Object> asRawParameters()
        {
            Map<String, Object> ret = new TreeMap<>();
            ret.put("UUID", uuid);
            ret.put("KVS_NAME", kvsName);
            ret.put("KVS_DSP_NAME", kvsDspName);
            return ret;
        }

        @JsonIgnore
        @Override
        public Object getByColumn(String clmNameStr)
        {
            switch(clmNameStr)
            {
                case "UUID":
                    return uuid;
                case "KVS_NAME":
                    return kvsName;
                case "KVS_DSP_NAME":
                    return kvsDspName;
                default:
                    throw new ImplementationError("Unknown database column. Table: KEY_VALUE_STORE, Column: " + clmNameStr);
            }
        }

        @JsonIgnore
        @Override
        public final String getLinstorKey()
        {
            return formattedPrimaryKey;
        }

        @Override
        @JsonIgnore
        public DatabaseTable getDatabaseTable()
        {
            return GeneratedDatabaseTables.KEY_VALUE_STORE;
        }
    }

    @Version(GenCrdV1_18_2.VERSION)
    @Group(GenCrdV1_18_2.GROUP)
    @Plural("keyvaluestore")
    @Singular("keyvaluestore")
    public static class KeyValueStore extends CustomResource<KeyValueStoreSpec, Void> implements LinstorCrd<KeyValueStoreSpec>
    {
        private static final long serialVersionUID = -8854126594385535713L;
        String k8sKey = null;

        @JsonCreator
        public KeyValueStore()
        {
        }

        public KeyValueStore(KeyValueStoreSpec spec)
        {
            setMetadata(new ObjectMetaBuilder().withName(deriveKey(spec.getLinstorKey())).build());
            setSpec(spec);
        }

        @Override
        public void setMetadata(ObjectMeta metadataRef)
        {
            super.setMetadata(metadataRef);
            k8sKey = metadataRef.getName();
        }

        @Override
        @JsonIgnore
        public String getLinstorKey()
        {
            return spec.getLinstorKey();
        }

        @Override
        @JsonIgnore
        public String getK8sKey()
        {
            return k8sKey;
        }
    }

    public static LayerBcacheVolumes createLayerBcacheVolumes(
        int layerResourceId,
        int vlmNr,
        String nodeName,
        String poolName,
        String devUuid
    )
    {
        return new LayerBcacheVolumes(
            new LayerBcacheVolumesSpec(
                layerResourceId,
                vlmNr,
                nodeName,
                poolName,
                devUuid
            )
        );
    }

    @SuppressWarnings("rawtypes")
    @JsonInclude(Include.NON_NULL)
    public static class LayerBcacheVolumesSpec implements LinstorSpec
    {
        @JsonIgnore private static final long serialVersionUID = 2405500839736954669L;
        @JsonIgnore private static final String PK_FORMAT = "%d:%d";

        @JsonIgnore private final String formattedPrimaryKey;

        @JsonProperty("layer_resource_id") public final int layerResourceId; // PK
        @JsonProperty("vlm_nr") public final int vlmNr; // PK
        @JsonProperty("node_name") public final String nodeName;
        @JsonProperty("pool_name") public final String poolName;
        @JsonProperty("dev_uuid") public final String devUuid;

        @JsonCreator
        public LayerBcacheVolumesSpec(
            @JsonProperty("layer_resource_id") int layerResourceIdRef,
            @JsonProperty("vlm_nr") int vlmNrRef,
            @JsonProperty("node_name") String nodeNameRef,
            @JsonProperty("pool_name") String poolNameRef,
            @JsonProperty("dev_uuid") String devUuidRef
        )
        {
            layerResourceId = layerResourceIdRef;
            vlmNr = vlmNrRef;
            nodeName = nodeNameRef;
            poolName = poolNameRef;
            devUuid = devUuidRef;

            formattedPrimaryKey = String.format(
                LayerBcacheVolumesSpec.PK_FORMAT,
                layerResourceId,
                vlmNr
            );
        }

        @JsonIgnore
        @Override
        @SuppressWarnings("rawtypes")
        public LinstorCrd getCrd()
        {
            throw new ImplementationError("Pre 1_19_1 does not support this method");
        }

        @JsonIgnore
        @Override
        public Map<String, Object> asRawParameters()
        {
            Map<String, Object> ret = new TreeMap<>();
            ret.put("LAYER_RESOURCE_ID", layerResourceId);
            ret.put("VLM_NR", vlmNr);
            ret.put("NODE_NAME", nodeName);
            ret.put("POOL_NAME", poolName);
            ret.put("DEV_UUID", devUuid);
            return ret;
        }

        @JsonIgnore
        @Override
        public Object getByColumn(String clmNameStr)
        {
            switch(clmNameStr)
            {
                case "LAYER_RESOURCE_ID":
                    return layerResourceId;
                case "VLM_NR":
                    return vlmNr;
                case "NODE_NAME":
                    return nodeName;
                case "POOL_NAME":
                    return poolName;
                case "DEV_UUID":
                    return devUuid;
                default:
                    throw new ImplementationError("Unknown database column. Table: LAYER_BCACHE_VOLUMES, Column: " + clmNameStr);
            }
        }

        @JsonIgnore
        @Override
        public final String getLinstorKey()
        {
            return formattedPrimaryKey;
        }

        @Override
        @JsonIgnore
        public DatabaseTable getDatabaseTable()
        {
            return GeneratedDatabaseTables.LAYER_BCACHE_VOLUMES;
        }
    }

    @Version(GenCrdV1_18_2.VERSION)
    @Group(GenCrdV1_18_2.GROUP)
    @Plural("layerbcachevolumes")
    @Singular("layerbcachevolumes")
    public static class LayerBcacheVolumes extends CustomResource<LayerBcacheVolumesSpec, Void> implements LinstorCrd<LayerBcacheVolumesSpec>
    {
        private static final long serialVersionUID = -6473594335085777517L;
        String k8sKey = null;

        @JsonCreator
        public LayerBcacheVolumes()
        {
        }

        public LayerBcacheVolumes(LayerBcacheVolumesSpec spec)
        {
            setMetadata(new ObjectMetaBuilder().withName(deriveKey(spec.getLinstorKey())).build());
            setSpec(spec);
        }

        @Override
        public void setMetadata(ObjectMeta metadataRef)
        {
            super.setMetadata(metadataRef);
            k8sKey = metadataRef.getName();
        }

        @Override
        @JsonIgnore
        public String getLinstorKey()
        {
            return spec.getLinstorKey();
        }

        @Override
        @JsonIgnore
        public String getK8sKey()
        {
            return k8sKey;
        }
    }

    public static LayerCacheVolumes createLayerCacheVolumes(
        int layerResourceId,
        int vlmNr,
        String nodeName,
        String poolNameCache,
        String poolNameMeta
    )
    {
        return new LayerCacheVolumes(
            new LayerCacheVolumesSpec(
                layerResourceId,
                vlmNr,
                nodeName,
                poolNameCache,
                poolNameMeta
            )
        );
    }

    @SuppressWarnings("rawtypes")
    @JsonInclude(Include.NON_NULL)
    public static class LayerCacheVolumesSpec implements LinstorSpec
    {
        @JsonIgnore private static final long serialVersionUID = 4154739247548074558L;
        @JsonIgnore private static final String PK_FORMAT = "%d:%d";

        @JsonIgnore private final String formattedPrimaryKey;

        @JsonProperty("layer_resource_id") public final int layerResourceId; // PK
        @JsonProperty("vlm_nr") public final int vlmNr; // PK
        @JsonProperty("node_name") public final String nodeName;
        @JsonProperty("pool_name_cache") public final String poolNameCache;
        @JsonProperty("pool_name_meta") public final String poolNameMeta;

        @JsonCreator
        public LayerCacheVolumesSpec(
            @JsonProperty("layer_resource_id") int layerResourceIdRef,
            @JsonProperty("vlm_nr") int vlmNrRef,
            @JsonProperty("node_name") String nodeNameRef,
            @JsonProperty("pool_name_cache") String poolNameCacheRef,
            @JsonProperty("pool_name_meta") String poolNameMetaRef
        )
        {
            layerResourceId = layerResourceIdRef;
            vlmNr = vlmNrRef;
            nodeName = nodeNameRef;
            poolNameCache = poolNameCacheRef;
            poolNameMeta = poolNameMetaRef;

            formattedPrimaryKey = String.format(
                LayerCacheVolumesSpec.PK_FORMAT,
                layerResourceId,
                vlmNr
            );
        }

        @JsonIgnore
        @Override
        @SuppressWarnings("rawtypes")
        public LinstorCrd getCrd()
        {
            throw new ImplementationError("Pre 1_19_1 does not support this method");
        }

        @JsonIgnore
        @Override
        public Map<String, Object> asRawParameters()
        {
            Map<String, Object> ret = new TreeMap<>();
            ret.put("LAYER_RESOURCE_ID", layerResourceId);
            ret.put("VLM_NR", vlmNr);
            ret.put("NODE_NAME", nodeName);
            ret.put("POOL_NAME_CACHE", poolNameCache);
            ret.put("POOL_NAME_META", poolNameMeta);
            return ret;
        }

        @JsonIgnore
        @Override
        public Object getByColumn(String clmNameStr)
        {
            switch(clmNameStr)
            {
                case "LAYER_RESOURCE_ID":
                    return layerResourceId;
                case "VLM_NR":
                    return vlmNr;
                case "NODE_NAME":
                    return nodeName;
                case "POOL_NAME_CACHE":
                    return poolNameCache;
                case "POOL_NAME_META":
                    return poolNameMeta;
                default:
                    throw new ImplementationError("Unknown database column. Table: LAYER_CACHE_VOLUMES, Column: " + clmNameStr);
            }
        }

        @JsonIgnore
        @Override
        public final String getLinstorKey()
        {
            return formattedPrimaryKey;
        }

        @Override
        @JsonIgnore
        public DatabaseTable getDatabaseTable()
        {
            return GeneratedDatabaseTables.LAYER_CACHE_VOLUMES;
        }
    }

    @Version(GenCrdV1_18_2.VERSION)
    @Group(GenCrdV1_18_2.GROUP)
    @Plural("layercachevolumes")
    @Singular("layercachevolumes")
    public static class LayerCacheVolumes extends CustomResource<LayerCacheVolumesSpec, Void> implements LinstorCrd<LayerCacheVolumesSpec>
    {
        private static final long serialVersionUID = 4143029160748740635L;
        String k8sKey = null;

        @JsonCreator
        public LayerCacheVolumes()
        {
        }

        public LayerCacheVolumes(LayerCacheVolumesSpec spec)
        {
            setMetadata(new ObjectMetaBuilder().withName(deriveKey(spec.getLinstorKey())).build());
            setSpec(spec);
        }

        @Override
        public void setMetadata(ObjectMeta metadataRef)
        {
            super.setMetadata(metadataRef);
            k8sKey = metadataRef.getName();
        }

        @Override
        @JsonIgnore
        public String getLinstorKey()
        {
            return spec.getLinstorKey();
        }

        @Override
        @JsonIgnore
        public String getK8sKey()
        {
            return k8sKey;
        }
    }

    public static LayerDrbdResources createLayerDrbdResources(
        int layerResourceId,
        int peerSlots,
        int alStripes,
        long alStripeSize,
        long flags,
        int nodeId
    )
    {
        return new LayerDrbdResources(
            new LayerDrbdResourcesSpec(
                layerResourceId,
                peerSlots,
                alStripes,
                alStripeSize,
                flags,
                nodeId
            )
        );
    }

    @SuppressWarnings("rawtypes")
    @JsonInclude(Include.NON_NULL)
    public static class LayerDrbdResourcesSpec implements LinstorSpec
    {
        @JsonIgnore private static final long serialVersionUID = 7325644465098771764L;
        @JsonIgnore private static final String PK_FORMAT = "%d";

        @JsonIgnore private final String formattedPrimaryKey;

        @JsonProperty("layer_resource_id") public final int layerResourceId; // PK
        @JsonProperty("peer_slots") public final int peerSlots;
        @JsonProperty("al_stripes") public final int alStripes;
        @JsonProperty("al_stripe_size") public final long alStripeSize;
        @JsonProperty("flags") public final long flags;
        @JsonProperty("node_id") public final int nodeId;

        @JsonCreator
        public LayerDrbdResourcesSpec(
            @JsonProperty("layer_resource_id") int layerResourceIdRef,
            @JsonProperty("peer_slots") int peerSlotsRef,
            @JsonProperty("al_stripes") int alStripesRef,
            @JsonProperty("al_stripe_size") long alStripeSizeRef,
            @JsonProperty("flags") long flagsRef,
            @JsonProperty("node_id") int nodeIdRef
        )
        {
            layerResourceId = layerResourceIdRef;
            peerSlots = peerSlotsRef;
            alStripes = alStripesRef;
            alStripeSize = alStripeSizeRef;
            flags = flagsRef;
            nodeId = nodeIdRef;

            formattedPrimaryKey = String.format(
                LayerDrbdResourcesSpec.PK_FORMAT,
                layerResourceId
            );
        }

        @JsonIgnore
        @Override
        @SuppressWarnings("rawtypes")
        public LinstorCrd getCrd()
        {
            throw new ImplementationError("Pre 1_19_1 does not support this method");
        }

        @JsonIgnore
        @Override
        public Map<String, Object> asRawParameters()
        {
            Map<String, Object> ret = new TreeMap<>();
            ret.put("LAYER_RESOURCE_ID", layerResourceId);
            ret.put("PEER_SLOTS", peerSlots);
            ret.put("AL_STRIPES", alStripes);
            ret.put("AL_STRIPE_SIZE", alStripeSize);
            ret.put("FLAGS", flags);
            ret.put("NODE_ID", nodeId);
            return ret;
        }

        @JsonIgnore
        @Override
        public Object getByColumn(String clmNameStr)
        {
            switch(clmNameStr)
            {
                case "LAYER_RESOURCE_ID":
                    return layerResourceId;
                case "PEER_SLOTS":
                    return peerSlots;
                case "AL_STRIPES":
                    return alStripes;
                case "AL_STRIPE_SIZE":
                    return alStripeSize;
                case "FLAGS":
                    return flags;
                case "NODE_ID":
                    return nodeId;
                default:
                    throw new ImplementationError("Unknown database column. Table: LAYER_DRBD_RESOURCES, Column: " + clmNameStr);
            }
        }

        @JsonIgnore
        @Override
        public final String getLinstorKey()
        {
            return formattedPrimaryKey;
        }

        @Override
        @JsonIgnore
        public DatabaseTable getDatabaseTable()
        {
            return GeneratedDatabaseTables.LAYER_DRBD_RESOURCES;
        }
    }

    @Version(GenCrdV1_18_2.VERSION)
    @Group(GenCrdV1_18_2.GROUP)
    @Plural("layerdrbdresources")
    @Singular("layerdrbdresources")
    public static class LayerDrbdResources extends CustomResource<LayerDrbdResourcesSpec, Void> implements LinstorCrd<LayerDrbdResourcesSpec>
    {
        private static final long serialVersionUID = 7680298641907375076L;
        String k8sKey = null;

        @JsonCreator
        public LayerDrbdResources()
        {
        }

        public LayerDrbdResources(LayerDrbdResourcesSpec spec)
        {
            setMetadata(new ObjectMetaBuilder().withName(deriveKey(spec.getLinstorKey())).build());
            setSpec(spec);
        }

        @Override
        public void setMetadata(ObjectMeta metadataRef)
        {
            super.setMetadata(metadataRef);
            k8sKey = metadataRef.getName();
        }

        @Override
        @JsonIgnore
        public String getLinstorKey()
        {
            return spec.getLinstorKey();
        }

        @Override
        @JsonIgnore
        public String getK8sKey()
        {
            return k8sKey;
        }
    }

    public static LayerDrbdResourceDefinitions createLayerDrbdResourceDefinitions(
        String resourceName,
        String resourceNameSuffix,
        String snapshotName,
        int peerSlots,
        int alStripes,
        long alStripeSize,
        Integer tcpPort,
        String transportType,
        String secret
    )
    {
        return new LayerDrbdResourceDefinitions(
            new LayerDrbdResourceDefinitionsSpec(
                resourceName,
                resourceNameSuffix,
                snapshotName,
                peerSlots,
                alStripes,
                alStripeSize,
                tcpPort,
                transportType,
                secret
            )
        );
    }

    @SuppressWarnings("rawtypes")
    @JsonInclude(Include.NON_NULL)
    public static class LayerDrbdResourceDefinitionsSpec implements LinstorSpec
    {
        @JsonIgnore private static final long serialVersionUID = -5066464125352788574L;
        @JsonIgnore private static final String PK_FORMAT = "%s:%s:%s";

        @JsonIgnore private final String formattedPrimaryKey;

        @JsonProperty("resource_name") public final String resourceName; // PK
        @JsonProperty("resource_name_suffix") public final String resourceNameSuffix; // PK
        @JsonProperty("snapshot_name") public final String snapshotName; // PK
        @JsonProperty("peer_slots") public final int peerSlots;
        @JsonProperty("al_stripes") public final int alStripes;
        @JsonProperty("al_stripe_size") public final long alStripeSize;
        @JsonProperty("tcp_port") public final Integer tcpPort;
        @JsonProperty("transport_type") public final String transportType;
        @JsonProperty("secret") public final String secret;

        @JsonCreator
        public LayerDrbdResourceDefinitionsSpec(
            @JsonProperty("resource_name") String resourceNameRef,
            @JsonProperty("resource_name_suffix") String resourceNameSuffixRef,
            @JsonProperty("snapshot_name") String snapshotNameRef,
            @JsonProperty("peer_slots") int peerSlotsRef,
            @JsonProperty("al_stripes") int alStripesRef,
            @JsonProperty("al_stripe_size") long alStripeSizeRef,
            @JsonProperty("tcp_port") Integer tcpPortRef,
            @JsonProperty("transport_type") String transportTypeRef,
            @JsonProperty("secret") String secretRef
        )
        {
            resourceName = resourceNameRef;
            resourceNameSuffix = resourceNameSuffixRef;
            snapshotName = snapshotNameRef;
            peerSlots = peerSlotsRef;
            alStripes = alStripesRef;
            alStripeSize = alStripeSizeRef;
            tcpPort = tcpPortRef;
            transportType = transportTypeRef;
            secret = secretRef;

            formattedPrimaryKey = String.format(
                LayerDrbdResourceDefinitionsSpec.PK_FORMAT,
                resourceName,
                resourceNameSuffix,
                snapshotName
            );
        }

        @JsonIgnore
        @Override
        @SuppressWarnings("rawtypes")
        public LinstorCrd getCrd()
        {
            throw new ImplementationError("Pre 1_19_1 does not support this method");
        }

        @JsonIgnore
        @Override
        public Map<String, Object> asRawParameters()
        {
            Map<String, Object> ret = new TreeMap<>();
            ret.put("RESOURCE_NAME", resourceName);
            ret.put("RESOURCE_NAME_SUFFIX", resourceNameSuffix);
            ret.put("SNAPSHOT_NAME", snapshotName);
            ret.put("PEER_SLOTS", peerSlots);
            ret.put("AL_STRIPES", alStripes);
            ret.put("AL_STRIPE_SIZE", alStripeSize);
            ret.put("TCP_PORT", tcpPort);
            ret.put("TRANSPORT_TYPE", transportType);
            ret.put("SECRET", secret);
            return ret;
        }

        @JsonIgnore
        @Override
        public Object getByColumn(String clmNameStr)
        {
            switch(clmNameStr)
            {
                case "RESOURCE_NAME":
                    return resourceName;
                case "RESOURCE_NAME_SUFFIX":
                    return resourceNameSuffix;
                case "SNAPSHOT_NAME":
                    return snapshotName;
                case "PEER_SLOTS":
                    return peerSlots;
                case "AL_STRIPES":
                    return alStripes;
                case "AL_STRIPE_SIZE":
                    return alStripeSize;
                case "TCP_PORT":
                    return tcpPort;
                case "TRANSPORT_TYPE":
                    return transportType;
                case "SECRET":
                    return secret;
                default:
                    throw new ImplementationError("Unknown database column. Table: LAYER_DRBD_RESOURCE_DEFINITIONS, Column: " + clmNameStr);
            }
        }

        @JsonIgnore
        @Override
        public final String getLinstorKey()
        {
            return formattedPrimaryKey;
        }

        @Override
        @JsonIgnore
        public DatabaseTable getDatabaseTable()
        {
            return GeneratedDatabaseTables.LAYER_DRBD_RESOURCE_DEFINITIONS;
        }
    }

    @Version(GenCrdV1_18_2.VERSION)
    @Group(GenCrdV1_18_2.GROUP)
    @Plural("layerdrbdresourcedefinitions")
    @Singular("layerdrbdresourcedefinitions")
    public static class LayerDrbdResourceDefinitions extends CustomResource<LayerDrbdResourceDefinitionsSpec, Void> implements LinstorCrd<LayerDrbdResourceDefinitionsSpec>
    {
        private static final long serialVersionUID = -3111483124421095110L;
        String k8sKey = null;

        @JsonCreator
        public LayerDrbdResourceDefinitions()
        {
        }

        public LayerDrbdResourceDefinitions(LayerDrbdResourceDefinitionsSpec spec)
        {
            setMetadata(new ObjectMetaBuilder().withName(deriveKey(spec.getLinstorKey())).build());
            setSpec(spec);
        }

        @Override
        public void setMetadata(ObjectMeta metadataRef)
        {
            super.setMetadata(metadataRef);
            k8sKey = metadataRef.getName();
        }

        @Override
        @JsonIgnore
        public String getLinstorKey()
        {
            return spec.getLinstorKey();
        }

        @Override
        @JsonIgnore
        public String getK8sKey()
        {
            return k8sKey;
        }
    }

    public static LayerDrbdVolumes createLayerDrbdVolumes(
        int layerResourceId,
        int vlmNr,
        String nodeName,
        String poolName
    )
    {
        return new LayerDrbdVolumes(
            new LayerDrbdVolumesSpec(
                layerResourceId,
                vlmNr,
                nodeName,
                poolName
            )
        );
    }

    @SuppressWarnings("rawtypes")
    @JsonInclude(Include.NON_NULL)
    public static class LayerDrbdVolumesSpec implements LinstorSpec
    {
        @JsonIgnore private static final long serialVersionUID = -1102639970546509973L;
        @JsonIgnore private static final String PK_FORMAT = "%d:%d";

        @JsonIgnore private final String formattedPrimaryKey;

        @JsonProperty("layer_resource_id") public final int layerResourceId; // PK
        @JsonProperty("vlm_nr") public final int vlmNr; // PK
        @JsonProperty("node_name") public final String nodeName;
        @JsonProperty("pool_name") public final String poolName;

        @JsonCreator
        public LayerDrbdVolumesSpec(
            @JsonProperty("layer_resource_id") int layerResourceIdRef,
            @JsonProperty("vlm_nr") int vlmNrRef,
            @JsonProperty("node_name") String nodeNameRef,
            @JsonProperty("pool_name") String poolNameRef
        )
        {
            layerResourceId = layerResourceIdRef;
            vlmNr = vlmNrRef;
            nodeName = nodeNameRef;
            poolName = poolNameRef;

            formattedPrimaryKey = String.format(
                LayerDrbdVolumesSpec.PK_FORMAT,
                layerResourceId,
                vlmNr
            );
        }

        @JsonIgnore
        @Override
        @SuppressWarnings("rawtypes")
        public LinstorCrd getCrd()
        {
            throw new ImplementationError("Pre 1_19_1 does not support this method");
        }

        @JsonIgnore
        @Override
        public Map<String, Object> asRawParameters()
        {
            Map<String, Object> ret = new TreeMap<>();
            ret.put("LAYER_RESOURCE_ID", layerResourceId);
            ret.put("VLM_NR", vlmNr);
            ret.put("NODE_NAME", nodeName);
            ret.put("POOL_NAME", poolName);
            return ret;
        }

        @JsonIgnore
        @Override
        public Object getByColumn(String clmNameStr)
        {
            switch(clmNameStr)
            {
                case "LAYER_RESOURCE_ID":
                    return layerResourceId;
                case "VLM_NR":
                    return vlmNr;
                case "NODE_NAME":
                    return nodeName;
                case "POOL_NAME":
                    return poolName;
                default:
                    throw new ImplementationError("Unknown database column. Table: LAYER_DRBD_VOLUMES, Column: " + clmNameStr);
            }
        }

        @JsonIgnore
        @Override
        public final String getLinstorKey()
        {
            return formattedPrimaryKey;
        }

        @Override
        @JsonIgnore
        public DatabaseTable getDatabaseTable()
        {
            return GeneratedDatabaseTables.LAYER_DRBD_VOLUMES;
        }
    }

    @Version(GenCrdV1_18_2.VERSION)
    @Group(GenCrdV1_18_2.GROUP)
    @Plural("layerdrbdvolumes")
    @Singular("layerdrbdvolumes")
    public static class LayerDrbdVolumes extends CustomResource<LayerDrbdVolumesSpec, Void> implements LinstorCrd<LayerDrbdVolumesSpec>
    {
        private static final long serialVersionUID = 6958642974822368317L;
        String k8sKey = null;

        @JsonCreator
        public LayerDrbdVolumes()
        {
        }

        public LayerDrbdVolumes(LayerDrbdVolumesSpec spec)
        {
            setMetadata(new ObjectMetaBuilder().withName(deriveKey(spec.getLinstorKey())).build());
            setSpec(spec);
        }

        @Override
        public void setMetadata(ObjectMeta metadataRef)
        {
            super.setMetadata(metadataRef);
            k8sKey = metadataRef.getName();
        }

        @Override
        @JsonIgnore
        public String getLinstorKey()
        {
            return spec.getLinstorKey();
        }

        @Override
        @JsonIgnore
        public String getK8sKey()
        {
            return k8sKey;
        }
    }

    public static LayerDrbdVolumeDefinitions createLayerDrbdVolumeDefinitions(
        String resourceName,
        String resourceNameSuffix,
        String snapshotName,
        int vlmNr,
        Integer vlmMinorNr
    )
    {
        return new LayerDrbdVolumeDefinitions(
            new LayerDrbdVolumeDefinitionsSpec(
                resourceName,
                resourceNameSuffix,
                snapshotName,
                vlmNr,
                vlmMinorNr
            )
        );
    }

    @SuppressWarnings("rawtypes")
    @JsonInclude(Include.NON_NULL)
    public static class LayerDrbdVolumeDefinitionsSpec implements LinstorSpec
    {
        @JsonIgnore private static final long serialVersionUID = 4749124140871497879L;
        @JsonIgnore private static final String PK_FORMAT = "%s:%s:%s:%d";

        @JsonIgnore private final String formattedPrimaryKey;

        @JsonProperty("resource_name") public final String resourceName; // PK
        @JsonProperty("resource_name_suffix") public final String resourceNameSuffix; // PK
        @JsonProperty("snapshot_name") public final String snapshotName; // PK
        @JsonProperty("vlm_nr") public final int vlmNr; // PK
        @JsonProperty("vlm_minor_nr") public final Integer vlmMinorNr;

        @JsonCreator
        public LayerDrbdVolumeDefinitionsSpec(
            @JsonProperty("resource_name") String resourceNameRef,
            @JsonProperty("resource_name_suffix") String resourceNameSuffixRef,
            @JsonProperty("snapshot_name") String snapshotNameRef,
            @JsonProperty("vlm_nr") int vlmNrRef,
            @JsonProperty("vlm_minor_nr") Integer vlmMinorNrRef
        )
        {
            resourceName = resourceNameRef;
            resourceNameSuffix = resourceNameSuffixRef;
            snapshotName = snapshotNameRef;
            vlmNr = vlmNrRef;
            vlmMinorNr = vlmMinorNrRef;

            formattedPrimaryKey = String.format(
                LayerDrbdVolumeDefinitionsSpec.PK_FORMAT,
                resourceName,
                resourceNameSuffix,
                snapshotName,
                vlmNr
            );
        }

        @JsonIgnore
        @Override
        @SuppressWarnings("rawtypes")
        public LinstorCrd getCrd()
        {
            throw new ImplementationError("Pre 1_19_1 does not support this method");
        }

        @JsonIgnore
        @Override
        public Map<String, Object> asRawParameters()
        {
            Map<String, Object> ret = new TreeMap<>();
            ret.put("RESOURCE_NAME", resourceName);
            ret.put("RESOURCE_NAME_SUFFIX", resourceNameSuffix);
            ret.put("SNAPSHOT_NAME", snapshotName);
            ret.put("VLM_NR", vlmNr);
            ret.put("VLM_MINOR_NR", vlmMinorNr);
            return ret;
        }

        @JsonIgnore
        @Override
        public Object getByColumn(String clmNameStr)
        {
            switch(clmNameStr)
            {
                case "RESOURCE_NAME":
                    return resourceName;
                case "RESOURCE_NAME_SUFFIX":
                    return resourceNameSuffix;
                case "SNAPSHOT_NAME":
                    return snapshotName;
                case "VLM_NR":
                    return vlmNr;
                case "VLM_MINOR_NR":
                    return vlmMinorNr;
                default:
                    throw new ImplementationError("Unknown database column. Table: LAYER_DRBD_VOLUME_DEFINITIONS, Column: " + clmNameStr);
            }
        }

        @JsonIgnore
        @Override
        public final String getLinstorKey()
        {
            return formattedPrimaryKey;
        }

        @Override
        @JsonIgnore
        public DatabaseTable getDatabaseTable()
        {
            return GeneratedDatabaseTables.LAYER_DRBD_VOLUME_DEFINITIONS;
        }
    }

    @Version(GenCrdV1_18_2.VERSION)
    @Group(GenCrdV1_18_2.GROUP)
    @Plural("layerdrbdvolumedefinitions")
    @Singular("layerdrbdvolumedefinitions")
    public static class LayerDrbdVolumeDefinitions extends CustomResource<LayerDrbdVolumeDefinitionsSpec, Void> implements LinstorCrd<LayerDrbdVolumeDefinitionsSpec>
    {
        private static final long serialVersionUID = 8168863591197679316L;
        String k8sKey = null;

        @JsonCreator
        public LayerDrbdVolumeDefinitions()
        {
        }

        public LayerDrbdVolumeDefinitions(LayerDrbdVolumeDefinitionsSpec spec)
        {
            setMetadata(new ObjectMetaBuilder().withName(deriveKey(spec.getLinstorKey())).build());
            setSpec(spec);
        }

        @Override
        public void setMetadata(ObjectMeta metadataRef)
        {
            super.setMetadata(metadataRef);
            k8sKey = metadataRef.getName();
        }

        @Override
        @JsonIgnore
        public String getLinstorKey()
        {
            return spec.getLinstorKey();
        }

        @Override
        @JsonIgnore
        public String getK8sKey()
        {
            return k8sKey;
        }
    }

    public static LayerLuksVolumes createLayerLuksVolumes(
        int layerResourceId,
        int vlmNr,
        String encryptedPassword
    )
    {
        return new LayerLuksVolumes(
            new LayerLuksVolumesSpec(
                layerResourceId,
                vlmNr,
                encryptedPassword
            )
        );
    }

    @SuppressWarnings("rawtypes")
    @JsonInclude(Include.NON_NULL)
    public static class LayerLuksVolumesSpec implements LinstorSpec
    {
        @JsonIgnore private static final long serialVersionUID = 5936379437818386026L;
        @JsonIgnore private static final String PK_FORMAT = "%d:%d";

        @JsonIgnore private final String formattedPrimaryKey;

        @JsonProperty("layer_resource_id") public final int layerResourceId; // PK
        @JsonProperty("vlm_nr") public final int vlmNr; // PK
        @JsonProperty("encrypted_password") public final String encryptedPassword;

        @JsonCreator
        public LayerLuksVolumesSpec(
            @JsonProperty("layer_resource_id") int layerResourceIdRef,
            @JsonProperty("vlm_nr") int vlmNrRef,
            @JsonProperty("encrypted_password") String encryptedPasswordRef
        )
        {
            layerResourceId = layerResourceIdRef;
            vlmNr = vlmNrRef;
            encryptedPassword = encryptedPasswordRef;

            formattedPrimaryKey = String.format(
                LayerLuksVolumesSpec.PK_FORMAT,
                layerResourceId,
                vlmNr
            );
        }

        @JsonIgnore
        @Override
        @SuppressWarnings("rawtypes")
        public LinstorCrd getCrd()
        {
            throw new ImplementationError("Pre 1_19_1 does not support this method");
        }

        @JsonIgnore
        @Override
        public Map<String, Object> asRawParameters()
        {
            Map<String, Object> ret = new TreeMap<>();
            ret.put("LAYER_RESOURCE_ID", layerResourceId);
            ret.put("VLM_NR", vlmNr);
            ret.put("ENCRYPTED_PASSWORD", encryptedPassword);
            return ret;
        }

        @JsonIgnore
        @Override
        public Object getByColumn(String clmNameStr)
        {
            switch(clmNameStr)
            {
                case "LAYER_RESOURCE_ID":
                    return layerResourceId;
                case "VLM_NR":
                    return vlmNr;
                case "ENCRYPTED_PASSWORD":
                    return encryptedPassword;
                default:
                    throw new ImplementationError("Unknown database column. Table: LAYER_LUKS_VOLUMES, Column: " + clmNameStr);
            }
        }

        @JsonIgnore
        @Override
        public final String getLinstorKey()
        {
            return formattedPrimaryKey;
        }

        @Override
        @JsonIgnore
        public DatabaseTable getDatabaseTable()
        {
            return GeneratedDatabaseTables.LAYER_LUKS_VOLUMES;
        }
    }

    @Version(GenCrdV1_18_2.VERSION)
    @Group(GenCrdV1_18_2.GROUP)
    @Plural("layerluksvolumes")
    @Singular("layerluksvolumes")
    public static class LayerLuksVolumes extends CustomResource<LayerLuksVolumesSpec, Void> implements LinstorCrd<LayerLuksVolumesSpec>
    {
        private static final long serialVersionUID = 2231315249660038626L;
        String k8sKey = null;

        @JsonCreator
        public LayerLuksVolumes()
        {
        }

        public LayerLuksVolumes(LayerLuksVolumesSpec spec)
        {
            setMetadata(new ObjectMetaBuilder().withName(deriveKey(spec.getLinstorKey())).build());
            setSpec(spec);
        }

        @Override
        public void setMetadata(ObjectMeta metadataRef)
        {
            super.setMetadata(metadataRef);
            k8sKey = metadataRef.getName();
        }

        @Override
        @JsonIgnore
        public String getLinstorKey()
        {
            return spec.getLinstorKey();
        }

        @Override
        @JsonIgnore
        public String getK8sKey()
        {
            return k8sKey;
        }
    }

    public static LayerOpenflexResourceDefinitions createLayerOpenflexResourceDefinitions(
        String resourceName,
        String snapshotName,
        String resourceNameSuffix,
        String nqn
    )
    {
        return new LayerOpenflexResourceDefinitions(
            new LayerOpenflexResourceDefinitionsSpec(
                resourceName,
                snapshotName,
                resourceNameSuffix,
                nqn
            )
        );
    }

    @SuppressWarnings("rawtypes")
    @JsonInclude(Include.NON_NULL)
    public static class LayerOpenflexResourceDefinitionsSpec implements LinstorSpec
    {
        @JsonIgnore private static final long serialVersionUID = -3814591743869685838L;
        @JsonIgnore private static final String PK_FORMAT = "%s:%s";

        @JsonIgnore private final String formattedPrimaryKey;

        @JsonProperty("resource_name") public final String resourceName; // PK
        @JsonProperty("snapshot_name") public final String snapshotName;
        @JsonProperty("resource_name_suffix") public final String resourceNameSuffix; // PK
        @JsonProperty("nqn") public final String nqn;

        @JsonCreator
        public LayerOpenflexResourceDefinitionsSpec(
            @JsonProperty("resource_name") String resourceNameRef,
            @JsonProperty("snapshot_name") String snapshotNameRef,
            @JsonProperty("resource_name_suffix") String resourceNameSuffixRef,
            @JsonProperty("nqn") String nqnRef
        )
        {
            resourceName = resourceNameRef;
            snapshotName = snapshotNameRef;
            resourceNameSuffix = resourceNameSuffixRef;
            nqn = nqnRef;

            formattedPrimaryKey = String.format(
                LayerOpenflexResourceDefinitionsSpec.PK_FORMAT,
                resourceName,
                resourceNameSuffix
            );
        }

        @JsonIgnore
        @Override
        @SuppressWarnings("rawtypes")
        public LinstorCrd getCrd()
        {
            throw new ImplementationError("Pre 1_19_1 does not support this method");
        }

        @JsonIgnore
        @Override
        public Map<String, Object> asRawParameters()
        {
            Map<String, Object> ret = new TreeMap<>();
            ret.put("RESOURCE_NAME", resourceName);
            ret.put("SNAPSHOT_NAME", snapshotName);
            ret.put("RESOURCE_NAME_SUFFIX", resourceNameSuffix);
            ret.put("NQN", nqn);
            return ret;
        }

        @JsonIgnore
        @Override
        public Object getByColumn(String clmNameStr)
        {
            switch(clmNameStr)
            {
                case "RESOURCE_NAME":
                    return resourceName;
                case "SNAPSHOT_NAME":
                    return snapshotName;
                case "RESOURCE_NAME_SUFFIX":
                    return resourceNameSuffix;
                case "NQN":
                    return nqn;
                default:
                    throw new ImplementationError("Unknown database column. Table: LAYER_OPENFLEX_RESOURCE_DEFINITIONS, Column: " + clmNameStr);
            }
        }

        @JsonIgnore
        @Override
        public final String getLinstorKey()
        {
            return formattedPrimaryKey;
        }

        @Override
        @JsonIgnore
        public DatabaseTable getDatabaseTable()
        {
            return GeneratedDatabaseTables.LAYER_OPENFLEX_RESOURCE_DEFINITIONS;
        }
    }

    @Version(GenCrdV1_18_2.VERSION)
    @Group(GenCrdV1_18_2.GROUP)
    @Plural("layeropenflexresourcedefinitions")
    @Singular("layeropenflexresourcedefinitions")
    public static class LayerOpenflexResourceDefinitions extends CustomResource<LayerOpenflexResourceDefinitionsSpec, Void> implements LinstorCrd<LayerOpenflexResourceDefinitionsSpec>
    {
        private static final long serialVersionUID = -1682434878532735560L;
        String k8sKey = null;

        @JsonCreator
        public LayerOpenflexResourceDefinitions()
        {
        }

        public LayerOpenflexResourceDefinitions(LayerOpenflexResourceDefinitionsSpec spec)
        {
            setMetadata(new ObjectMetaBuilder().withName(deriveKey(spec.getLinstorKey())).build());
            setSpec(spec);
        }

        @Override
        public void setMetadata(ObjectMeta metadataRef)
        {
            super.setMetadata(metadataRef);
            k8sKey = metadataRef.getName();
        }

        @Override
        @JsonIgnore
        public String getLinstorKey()
        {
            return spec.getLinstorKey();
        }

        @Override
        @JsonIgnore
        public String getK8sKey()
        {
            return k8sKey;
        }
    }

    public static LayerOpenflexVolumes createLayerOpenflexVolumes(
        int layerResourceId,
        int vlmNr,
        String nodeName,
        String poolName
    )
    {
        return new LayerOpenflexVolumes(
            new LayerOpenflexVolumesSpec(
                layerResourceId,
                vlmNr,
                nodeName,
                poolName
            )
        );
    }

    @SuppressWarnings("rawtypes")
    @JsonInclude(Include.NON_NULL)
    public static class LayerOpenflexVolumesSpec implements LinstorSpec
    {
        @JsonIgnore private static final long serialVersionUID = -5092733054867091360L;
        @JsonIgnore private static final String PK_FORMAT = "%d:%d";

        @JsonIgnore private final String formattedPrimaryKey;

        @JsonProperty("layer_resource_id") public final int layerResourceId; // PK
        @JsonProperty("vlm_nr") public final int vlmNr; // PK
        @JsonProperty("node_name") public final String nodeName;
        @JsonProperty("pool_name") public final String poolName;

        @JsonCreator
        public LayerOpenflexVolumesSpec(
            @JsonProperty("layer_resource_id") int layerResourceIdRef,
            @JsonProperty("vlm_nr") int vlmNrRef,
            @JsonProperty("node_name") String nodeNameRef,
            @JsonProperty("pool_name") String poolNameRef
        )
        {
            layerResourceId = layerResourceIdRef;
            vlmNr = vlmNrRef;
            nodeName = nodeNameRef;
            poolName = poolNameRef;

            formattedPrimaryKey = String.format(
                LayerOpenflexVolumesSpec.PK_FORMAT,
                layerResourceId,
                vlmNr
            );
        }

        @JsonIgnore
        @Override
        @SuppressWarnings("rawtypes")
        public LinstorCrd getCrd()
        {
            throw new ImplementationError("Pre 1_19_1 does not support this method");
        }

        @JsonIgnore
        @Override
        public Map<String, Object> asRawParameters()
        {
            Map<String, Object> ret = new TreeMap<>();
            ret.put("LAYER_RESOURCE_ID", layerResourceId);
            ret.put("VLM_NR", vlmNr);
            ret.put("NODE_NAME", nodeName);
            ret.put("POOL_NAME", poolName);
            return ret;
        }

        @JsonIgnore
        @Override
        public Object getByColumn(String clmNameStr)
        {
            switch(clmNameStr)
            {
                case "LAYER_RESOURCE_ID":
                    return layerResourceId;
                case "VLM_NR":
                    return vlmNr;
                case "NODE_NAME":
                    return nodeName;
                case "POOL_NAME":
                    return poolName;
                default:
                    throw new ImplementationError("Unknown database column. Table: LAYER_OPENFLEX_VOLUMES, Column: " + clmNameStr);
            }
        }

        @JsonIgnore
        @Override
        public final String getLinstorKey()
        {
            return formattedPrimaryKey;
        }

        @Override
        @JsonIgnore
        public DatabaseTable getDatabaseTable()
        {
            return GeneratedDatabaseTables.LAYER_OPENFLEX_VOLUMES;
        }
    }

    @Version(GenCrdV1_18_2.VERSION)
    @Group(GenCrdV1_18_2.GROUP)
    @Plural("layeropenflexvolumes")
    @Singular("layeropenflexvolumes")
    public static class LayerOpenflexVolumes extends CustomResource<LayerOpenflexVolumesSpec, Void> implements LinstorCrd<LayerOpenflexVolumesSpec>
    {
        private static final long serialVersionUID = 8326310777874074058L;
        String k8sKey = null;

        @JsonCreator
        public LayerOpenflexVolumes()
        {
        }

        public LayerOpenflexVolumes(LayerOpenflexVolumesSpec spec)
        {
            setMetadata(new ObjectMetaBuilder().withName(deriveKey(spec.getLinstorKey())).build());
            setSpec(spec);
        }

        @Override
        public void setMetadata(ObjectMeta metadataRef)
        {
            super.setMetadata(metadataRef);
            k8sKey = metadataRef.getName();
        }

        @Override
        @JsonIgnore
        public String getLinstorKey()
        {
            return spec.getLinstorKey();
        }

        @Override
        @JsonIgnore
        public String getK8sKey()
        {
            return k8sKey;
        }
    }

    public static LayerResourceIds createLayerResourceIds(
        int layerResourceId,
        String nodeName,
        String resourceName,
        String snapshotName,
        String layerResourceKind,
        Integer layerResourceParentId,
        String layerResourceSuffix,
        boolean layerResourceSuspended
    )
    {
        return new LayerResourceIds(
            new LayerResourceIdsSpec(
                layerResourceId,
                nodeName,
                resourceName,
                snapshotName,
                layerResourceKind,
                layerResourceParentId,
                layerResourceSuffix,
                layerResourceSuspended
            )
        );
    }

    @SuppressWarnings("rawtypes")
    @JsonInclude(Include.NON_NULL)
    public static class LayerResourceIdsSpec implements LinstorSpec
    {
        @JsonIgnore private static final long serialVersionUID = -888599128477484454L;
        @JsonIgnore private static final String PK_FORMAT = "%d";

        @JsonIgnore private final String formattedPrimaryKey;

        @JsonProperty("layer_resource_id") public final int layerResourceId; // PK
        @JsonProperty("node_name") public final String nodeName;
        @JsonProperty("resource_name") public final String resourceName;
        @JsonProperty("snapshot_name") public final String snapshotName;
        @JsonProperty("layer_resource_kind") public final String layerResourceKind;
        @JsonProperty("layer_resource_parent_id") public final Integer layerResourceParentId;
        @JsonProperty("layer_resource_suffix") public final String layerResourceSuffix;
        @JsonProperty("layer_resource_suspended") public final boolean layerResourceSuspended;

        @JsonCreator
        public LayerResourceIdsSpec(
            @JsonProperty("layer_resource_id") int layerResourceIdRef,
            @JsonProperty("node_name") String nodeNameRef,
            @JsonProperty("resource_name") String resourceNameRef,
            @JsonProperty("snapshot_name") String snapshotNameRef,
            @JsonProperty("layer_resource_kind") String layerResourceKindRef,
            @JsonProperty("layer_resource_parent_id") Integer layerResourceParentIdRef,
            @JsonProperty("layer_resource_suffix") String layerResourceSuffixRef,
            @JsonProperty("layer_resource_suspended") boolean layerResourceSuspendedRef
        )
        {
            layerResourceId = layerResourceIdRef;
            nodeName = nodeNameRef;
            resourceName = resourceNameRef;
            snapshotName = snapshotNameRef;
            layerResourceKind = layerResourceKindRef;
            layerResourceParentId = layerResourceParentIdRef;
            layerResourceSuffix = layerResourceSuffixRef;
            layerResourceSuspended = layerResourceSuspendedRef;

            formattedPrimaryKey = String.format(
                LayerResourceIdsSpec.PK_FORMAT,
                layerResourceId
            );
        }

        @JsonIgnore
        @Override
        @SuppressWarnings("rawtypes")
        public LinstorCrd getCrd()
        {
            throw new ImplementationError("Pre 1_19_1 does not support this method");
        }

        @JsonIgnore
        @Override
        public Map<String, Object> asRawParameters()
        {
            Map<String, Object> ret = new TreeMap<>();
            ret.put("LAYER_RESOURCE_ID", layerResourceId);
            ret.put("NODE_NAME", nodeName);
            ret.put("RESOURCE_NAME", resourceName);
            ret.put("SNAPSHOT_NAME", snapshotName);
            ret.put("LAYER_RESOURCE_KIND", layerResourceKind);
            ret.put("LAYER_RESOURCE_PARENT_ID", layerResourceParentId);
            ret.put("LAYER_RESOURCE_SUFFIX", layerResourceSuffix);
            ret.put("LAYER_RESOURCE_SUSPENDED", layerResourceSuspended);
            return ret;
        }

        @JsonIgnore
        @Override
        public Object getByColumn(String clmNameStr)
        {
            switch(clmNameStr)
            {
                case "LAYER_RESOURCE_ID":
                    return layerResourceId;
                case "NODE_NAME":
                    return nodeName;
                case "RESOURCE_NAME":
                    return resourceName;
                case "SNAPSHOT_NAME":
                    return snapshotName;
                case "LAYER_RESOURCE_KIND":
                    return layerResourceKind;
                case "LAYER_RESOURCE_PARENT_ID":
                    return layerResourceParentId;
                case "LAYER_RESOURCE_SUFFIX":
                    return layerResourceSuffix;
                case "LAYER_RESOURCE_SUSPENDED":
                    return layerResourceSuspended;
                default:
                    throw new ImplementationError("Unknown database column. Table: LAYER_RESOURCE_IDS, Column: " + clmNameStr);
            }
        }

        @JsonIgnore
        @Override
        public final String getLinstorKey()
        {
            return formattedPrimaryKey;
        }

        @Override
        @JsonIgnore
        public DatabaseTable getDatabaseTable()
        {
            return GeneratedDatabaseTables.LAYER_RESOURCE_IDS;
        }
    }

    @Version(GenCrdV1_18_2.VERSION)
    @Group(GenCrdV1_18_2.GROUP)
    @Plural("layerresourceids")
    @Singular("layerresourceids")
    public static class LayerResourceIds extends CustomResource<LayerResourceIdsSpec, Void> implements LinstorCrd<LayerResourceIdsSpec>
    {
        private static final long serialVersionUID = -5863101595055250524L;
        String k8sKey = null;

        @JsonCreator
        public LayerResourceIds()
        {
        }

        public LayerResourceIds(LayerResourceIdsSpec spec)
        {
            setMetadata(new ObjectMetaBuilder().withName(deriveKey(spec.getLinstorKey())).build());
            setSpec(spec);
        }

        @Override
        public void setMetadata(ObjectMeta metadataRef)
        {
            super.setMetadata(metadataRef);
            k8sKey = metadataRef.getName();
        }

        @Override
        @JsonIgnore
        public String getLinstorKey()
        {
            return spec.getLinstorKey();
        }

        @Override
        @JsonIgnore
        public String getK8sKey()
        {
            return k8sKey;
        }
    }

    public static LayerStorageVolumes createLayerStorageVolumes(
        int layerResourceId,
        int vlmNr,
        String providerKind,
        String nodeName,
        String storPoolName
    )
    {
        return new LayerStorageVolumes(
            new LayerStorageVolumesSpec(
                layerResourceId,
                vlmNr,
                providerKind,
                nodeName,
                storPoolName
            )
        );
    }

    @SuppressWarnings("rawtypes")
    @JsonInclude(Include.NON_NULL)
    public static class LayerStorageVolumesSpec implements LinstorSpec
    {
        @JsonIgnore private static final long serialVersionUID = 8881995037194422159L;
        @JsonIgnore private static final String PK_FORMAT = "%d:%d";

        @JsonIgnore private final String formattedPrimaryKey;

        @JsonProperty("layer_resource_id") public final int layerResourceId; // PK
        @JsonProperty("vlm_nr") public final int vlmNr; // PK
        @JsonProperty("provider_kind") public final String providerKind;
        @JsonProperty("node_name") public final String nodeName;
        @JsonProperty("stor_pool_name") public final String storPoolName;

        @JsonCreator
        public LayerStorageVolumesSpec(
            @JsonProperty("layer_resource_id") int layerResourceIdRef,
            @JsonProperty("vlm_nr") int vlmNrRef,
            @JsonProperty("provider_kind") String providerKindRef,
            @JsonProperty("node_name") String nodeNameRef,
            @JsonProperty("stor_pool_name") String storPoolNameRef
        )
        {
            layerResourceId = layerResourceIdRef;
            vlmNr = vlmNrRef;
            providerKind = providerKindRef;
            nodeName = nodeNameRef;
            storPoolName = storPoolNameRef;

            formattedPrimaryKey = String.format(
                LayerStorageVolumesSpec.PK_FORMAT,
                layerResourceId,
                vlmNr
            );
        }

        @JsonIgnore
        @Override
        @SuppressWarnings("rawtypes")
        public LinstorCrd getCrd()
        {
            throw new ImplementationError("Pre 1_19_1 does not support this method");
        }

        @JsonIgnore
        @Override
        public Map<String, Object> asRawParameters()
        {
            Map<String, Object> ret = new TreeMap<>();
            ret.put("LAYER_RESOURCE_ID", layerResourceId);
            ret.put("VLM_NR", vlmNr);
            ret.put("PROVIDER_KIND", providerKind);
            ret.put("NODE_NAME", nodeName);
            ret.put("STOR_POOL_NAME", storPoolName);
            return ret;
        }

        @JsonIgnore
        @Override
        public Object getByColumn(String clmNameStr)
        {
            switch(clmNameStr)
            {
                case "LAYER_RESOURCE_ID":
                    return layerResourceId;
                case "VLM_NR":
                    return vlmNr;
                case "PROVIDER_KIND":
                    return providerKind;
                case "NODE_NAME":
                    return nodeName;
                case "STOR_POOL_NAME":
                    return storPoolName;
                default:
                    throw new ImplementationError("Unknown database column. Table: LAYER_STORAGE_VOLUMES, Column: " + clmNameStr);
            }
        }

        @JsonIgnore
        @Override
        public final String getLinstorKey()
        {
            return formattedPrimaryKey;
        }

        @Override
        @JsonIgnore
        public DatabaseTable getDatabaseTable()
        {
            return GeneratedDatabaseTables.LAYER_STORAGE_VOLUMES;
        }
    }

    @Version(GenCrdV1_18_2.VERSION)
    @Group(GenCrdV1_18_2.GROUP)
    @Plural("layerstoragevolumes")
    @Singular("layerstoragevolumes")
    public static class LayerStorageVolumes extends CustomResource<LayerStorageVolumesSpec, Void> implements LinstorCrd<LayerStorageVolumesSpec>
    {
        private static final long serialVersionUID = -1634367927250488034L;
        String k8sKey = null;

        @JsonCreator
        public LayerStorageVolumes()
        {
        }

        public LayerStorageVolumes(LayerStorageVolumesSpec spec)
        {
            setMetadata(new ObjectMetaBuilder().withName(deriveKey(spec.getLinstorKey())).build());
            setSpec(spec);
        }

        @Override
        public void setMetadata(ObjectMeta metadataRef)
        {
            super.setMetadata(metadataRef);
            k8sKey = metadataRef.getName();
        }

        @Override
        @JsonIgnore
        public String getLinstorKey()
        {
            return spec.getLinstorKey();
        }

        @Override
        @JsonIgnore
        public String getK8sKey()
        {
            return k8sKey;
        }
    }

    public static LayerWritecacheVolumes createLayerWritecacheVolumes(
        int layerResourceId,
        int vlmNr,
        String nodeName,
        String poolName
    )
    {
        return new LayerWritecacheVolumes(
            new LayerWritecacheVolumesSpec(
                layerResourceId,
                vlmNr,
                nodeName,
                poolName
            )
        );
    }

    @SuppressWarnings("rawtypes")
    @JsonInclude(Include.NON_NULL)
    public static class LayerWritecacheVolumesSpec implements LinstorSpec
    {
        @JsonIgnore private static final long serialVersionUID = -7450428792424670901L;
        @JsonIgnore private static final String PK_FORMAT = "%d:%d";

        @JsonIgnore private final String formattedPrimaryKey;

        @JsonProperty("layer_resource_id") public final int layerResourceId; // PK
        @JsonProperty("vlm_nr") public final int vlmNr; // PK
        @JsonProperty("node_name") public final String nodeName;
        @JsonProperty("pool_name") public final String poolName;

        @JsonCreator
        public LayerWritecacheVolumesSpec(
            @JsonProperty("layer_resource_id") int layerResourceIdRef,
            @JsonProperty("vlm_nr") int vlmNrRef,
            @JsonProperty("node_name") String nodeNameRef,
            @JsonProperty("pool_name") String poolNameRef
        )
        {
            layerResourceId = layerResourceIdRef;
            vlmNr = vlmNrRef;
            nodeName = nodeNameRef;
            poolName = poolNameRef;

            formattedPrimaryKey = String.format(
                LayerWritecacheVolumesSpec.PK_FORMAT,
                layerResourceId,
                vlmNr
            );
        }

        @JsonIgnore
        @Override
        @SuppressWarnings("rawtypes")
        public LinstorCrd getCrd()
        {
            throw new ImplementationError("Pre 1_19_1 does not support this method");
        }

        @JsonIgnore
        @Override
        public Map<String, Object> asRawParameters()
        {
            Map<String, Object> ret = new TreeMap<>();
            ret.put("LAYER_RESOURCE_ID", layerResourceId);
            ret.put("VLM_NR", vlmNr);
            ret.put("NODE_NAME", nodeName);
            ret.put("POOL_NAME", poolName);
            return ret;
        }

        @JsonIgnore
        @Override
        public Object getByColumn(String clmNameStr)
        {
            switch(clmNameStr)
            {
                case "LAYER_RESOURCE_ID":
                    return layerResourceId;
                case "VLM_NR":
                    return vlmNr;
                case "NODE_NAME":
                    return nodeName;
                case "POOL_NAME":
                    return poolName;
                default:
                    throw new ImplementationError("Unknown database column. Table: LAYER_WRITECACHE_VOLUMES, Column: " + clmNameStr);
            }
        }

        @JsonIgnore
        @Override
        public final String getLinstorKey()
        {
            return formattedPrimaryKey;
        }

        @Override
        @JsonIgnore
        public DatabaseTable getDatabaseTable()
        {
            return GeneratedDatabaseTables.LAYER_WRITECACHE_VOLUMES;
        }
    }

    @Version(GenCrdV1_18_2.VERSION)
    @Group(GenCrdV1_18_2.GROUP)
    @Plural("layerwritecachevolumes")
    @Singular("layerwritecachevolumes")
    public static class LayerWritecacheVolumes extends CustomResource<LayerWritecacheVolumesSpec, Void> implements LinstorCrd<LayerWritecacheVolumesSpec>
    {
        private static final long serialVersionUID = -8594942747503992370L;
        String k8sKey = null;

        @JsonCreator
        public LayerWritecacheVolumes()
        {
        }

        public LayerWritecacheVolumes(LayerWritecacheVolumesSpec spec)
        {
            setMetadata(new ObjectMetaBuilder().withName(deriveKey(spec.getLinstorKey())).build());
            setSpec(spec);
        }

        @Override
        public void setMetadata(ObjectMeta metadataRef)
        {
            super.setMetadata(metadataRef);
            k8sKey = metadataRef.getName();
        }

        @Override
        @JsonIgnore
        public String getLinstorKey()
        {
            return spec.getLinstorKey();
        }

        @Override
        @JsonIgnore
        public String getK8sKey()
        {
            return k8sKey;
        }
    }

    public static LinstorRemotes createLinstorRemotes(
        String uuid,
        String name,
        String dspName,
        long flags,
        String url,
        byte[] encryptedPassphrase,
        String clusterId
    )
    {
        return new LinstorRemotes(
            new LinstorRemotesSpec(
                uuid,
                name,
                dspName,
                flags,
                url,
                encryptedPassphrase,
                clusterId
            )
        );
    }

    @SuppressWarnings("rawtypes")
    @JsonInclude(Include.NON_NULL)
    public static class LinstorRemotesSpec implements LinstorSpec
    {
        @JsonIgnore private static final long serialVersionUID = -338319431594858292L;
        @JsonIgnore private static final String PK_FORMAT = "%s";

        @JsonIgnore private final String formattedPrimaryKey;

        @JsonProperty("uuid") public final String uuid;
        @JsonProperty("name") public final String name; // PK
        @JsonProperty("dsp_name") public final String dspName;
        @JsonProperty("flags") public final long flags;
        @JsonProperty("url") public final String url;
        @JsonProperty("encrypted_passphrase") public final byte[] encryptedPassphrase;
        @JsonProperty("cluster_id") public final String clusterId;

        @JsonCreator
        public LinstorRemotesSpec(
            @JsonProperty("uuid") String uuidRef,
            @JsonProperty("name") String nameRef,
            @JsonProperty("dsp_name") String dspNameRef,
            @JsonProperty("flags") long flagsRef,
            @JsonProperty("url") String urlRef,
            @JsonProperty("encrypted_passphrase") byte[] encryptedPassphraseRef,
            @JsonProperty("cluster_id") String clusterIdRef
        )
        {
            uuid = uuidRef;
            name = nameRef;
            dspName = dspNameRef;
            flags = flagsRef;
            url = urlRef;
            encryptedPassphrase = encryptedPassphraseRef;
            clusterId = clusterIdRef;

            formattedPrimaryKey = String.format(
                LinstorRemotesSpec.PK_FORMAT,
                name
            );
        }

        @JsonIgnore
        @Override
        @SuppressWarnings("rawtypes")
        public LinstorCrd getCrd()
        {
            throw new ImplementationError("Pre 1_19_1 does not support this method");
        }

        @JsonIgnore
        @Override
        public Map<String, Object> asRawParameters()
        {
            Map<String, Object> ret = new TreeMap<>();
            ret.put("UUID", uuid);
            ret.put("NAME", name);
            ret.put("DSP_NAME", dspName);
            ret.put("FLAGS", flags);
            ret.put("URL", url);
            ret.put("ENCRYPTED_PASSPHRASE", encryptedPassphrase);
            ret.put("CLUSTER_ID", clusterId);
            return ret;
        }

        @JsonIgnore
        @Override
        public Object getByColumn(String clmNameStr)
        {
            switch(clmNameStr)
            {
                case "UUID":
                    return uuid;
                case "NAME":
                    return name;
                case "DSP_NAME":
                    return dspName;
                case "FLAGS":
                    return flags;
                case "URL":
                    return url;
                case "ENCRYPTED_PASSPHRASE":
                    return encryptedPassphrase;
                case "CLUSTER_ID":
                    return clusterId;
                default:
                    throw new ImplementationError("Unknown database column. Table: LINSTOR_REMOTES, Column: " + clmNameStr);
            }
        }

        @JsonIgnore
        @Override
        public final String getLinstorKey()
        {
            return formattedPrimaryKey;
        }

        @Override
        @JsonIgnore
        public DatabaseTable getDatabaseTable()
        {
            return GeneratedDatabaseTables.LINSTOR_REMOTES;
        }
    }

    @Version(GenCrdV1_18_2.VERSION)
    @Group(GenCrdV1_18_2.GROUP)
    @Plural("linstorremotes")
    @Singular("linstorremotes")
    public static class LinstorRemotes extends CustomResource<LinstorRemotesSpec, Void> implements LinstorCrd<LinstorRemotesSpec>
    {
        private static final long serialVersionUID = -7231324610256860226L;
        String k8sKey = null;

        @JsonCreator
        public LinstorRemotes()
        {
        }

        public LinstorRemotes(LinstorRemotesSpec spec)
        {
            setMetadata(new ObjectMetaBuilder().withName(deriveKey(spec.getLinstorKey())).build());
            setSpec(spec);
        }

        @Override
        public void setMetadata(ObjectMeta metadataRef)
        {
            super.setMetadata(metadataRef);
            k8sKey = metadataRef.getName();
        }

        @Override
        @JsonIgnore
        public String getLinstorKey()
        {
            return spec.getLinstorKey();
        }

        @Override
        @JsonIgnore
        public String getK8sKey()
        {
            return k8sKey;
        }
    }

    public static Nodes createNodes(
        String uuid,
        String nodeName,
        String nodeDspName,
        long nodeFlags,
        int nodeType
    )
    {
        return new Nodes(
            new NodesSpec(
                uuid,
                nodeName,
                nodeDspName,
                nodeFlags,
                nodeType
            )
        );
    }

    @SuppressWarnings("rawtypes")
    @JsonInclude(Include.NON_NULL)
    public static class NodesSpec implements LinstorSpec
    {
        @JsonIgnore private static final long serialVersionUID = -3065206838530355948L;
        @JsonIgnore private static final String PK_FORMAT = "%s";

        @JsonIgnore private final String formattedPrimaryKey;

        @JsonProperty("uuid") public final String uuid;
        @JsonProperty("node_name") public final String nodeName; // PK
        @JsonProperty("node_dsp_name") public final String nodeDspName;
        @JsonProperty("node_flags") public final long nodeFlags;
        @JsonProperty("node_type") public final int nodeType;

        @JsonCreator
        public NodesSpec(
            @JsonProperty("uuid") String uuidRef,
            @JsonProperty("node_name") String nodeNameRef,
            @JsonProperty("node_dsp_name") String nodeDspNameRef,
            @JsonProperty("node_flags") long nodeFlagsRef,
            @JsonProperty("node_type") int nodeTypeRef
        )
        {
            uuid = uuidRef;
            nodeName = nodeNameRef;
            nodeDspName = nodeDspNameRef;
            nodeFlags = nodeFlagsRef;
            nodeType = nodeTypeRef;

            formattedPrimaryKey = String.format(
                NodesSpec.PK_FORMAT,
                nodeName
            );
        }

        @JsonIgnore
        @Override
        @SuppressWarnings("rawtypes")
        public LinstorCrd getCrd()
        {
            throw new ImplementationError("Pre 1_19_1 does not support this method");
        }

        @JsonIgnore
        @Override
        public Map<String, Object> asRawParameters()
        {
            Map<String, Object> ret = new TreeMap<>();
            ret.put("UUID", uuid);
            ret.put("NODE_NAME", nodeName);
            ret.put("NODE_DSP_NAME", nodeDspName);
            ret.put("NODE_FLAGS", nodeFlags);
            ret.put("NODE_TYPE", nodeType);
            return ret;
        }

        @JsonIgnore
        @Override
        public Object getByColumn(String clmNameStr)
        {
            switch(clmNameStr)
            {
                case "UUID":
                    return uuid;
                case "NODE_NAME":
                    return nodeName;
                case "NODE_DSP_NAME":
                    return nodeDspName;
                case "NODE_FLAGS":
                    return nodeFlags;
                case "NODE_TYPE":
                    return nodeType;
                default:
                    throw new ImplementationError("Unknown database column. Table: NODES, Column: " + clmNameStr);
            }
        }

        @JsonIgnore
        @Override
        public final String getLinstorKey()
        {
            return formattedPrimaryKey;
        }

        @Override
        @JsonIgnore
        public DatabaseTable getDatabaseTable()
        {
            return GeneratedDatabaseTables.NODES;
        }
    }

    @Version(GenCrdV1_18_2.VERSION)
    @Group(GenCrdV1_18_2.GROUP)
    @Plural("nodes")
    @Singular("nodes")
    public static class Nodes extends CustomResource<NodesSpec, Void> implements LinstorCrd<NodesSpec>
    {
        private static final long serialVersionUID = 7120111985705336670L;
        String k8sKey = null;

        @JsonCreator
        public Nodes()
        {
        }

        public Nodes(NodesSpec spec)
        {
            setMetadata(new ObjectMetaBuilder().withName(deriveKey(spec.getLinstorKey())).build());
            setSpec(spec);
        }

        @Override
        public void setMetadata(ObjectMeta metadataRef)
        {
            super.setMetadata(metadataRef);
            k8sKey = metadataRef.getName();
        }

        @Override
        @JsonIgnore
        public String getLinstorKey()
        {
            return spec.getLinstorKey();
        }

        @Override
        @JsonIgnore
        public String getK8sKey()
        {
            return k8sKey;
        }
    }

    public static NodeConnections createNodeConnections(
        String uuid,
        String nodeNameSrc,
        String nodeNameDst
    )
    {
        return new NodeConnections(
            new NodeConnectionsSpec(
                uuid,
                nodeNameSrc,
                nodeNameDst
            )
        );
    }

    @SuppressWarnings("rawtypes")
    @JsonInclude(Include.NON_NULL)
    public static class NodeConnectionsSpec implements LinstorSpec
    {
        @JsonIgnore private static final long serialVersionUID = 607613057330317824L;
        @JsonIgnore private static final String PK_FORMAT = "%s:%s";

        @JsonIgnore private final String formattedPrimaryKey;

        @JsonProperty("uuid") public final String uuid;
        @JsonProperty("node_name_src") public final String nodeNameSrc; // PK
        @JsonProperty("node_name_dst") public final String nodeNameDst; // PK

        @JsonCreator
        public NodeConnectionsSpec(
            @JsonProperty("uuid") String uuidRef,
            @JsonProperty("node_name_src") String nodeNameSrcRef,
            @JsonProperty("node_name_dst") String nodeNameDstRef
        )
        {
            uuid = uuidRef;
            nodeNameSrc = nodeNameSrcRef;
            nodeNameDst = nodeNameDstRef;

            formattedPrimaryKey = String.format(
                NodeConnectionsSpec.PK_FORMAT,
                nodeNameSrc,
                nodeNameDst
            );
        }

        @JsonIgnore
        @Override
        @SuppressWarnings("rawtypes")
        public LinstorCrd getCrd()
        {
            throw new ImplementationError("Pre 1_19_1 does not support this method");
        }

        @JsonIgnore
        @Override
        public Map<String, Object> asRawParameters()
        {
            Map<String, Object> ret = new TreeMap<>();
            ret.put("UUID", uuid);
            ret.put("NODE_NAME_SRC", nodeNameSrc);
            ret.put("NODE_NAME_DST", nodeNameDst);
            return ret;
        }

        @JsonIgnore
        @Override
        public Object getByColumn(String clmNameStr)
        {
            switch(clmNameStr)
            {
                case "UUID":
                    return uuid;
                case "NODE_NAME_SRC":
                    return nodeNameSrc;
                case "NODE_NAME_DST":
                    return nodeNameDst;
                default:
                    throw new ImplementationError("Unknown database column. Table: NODE_CONNECTIONS, Column: " + clmNameStr);
            }
        }

        @JsonIgnore
        @Override
        public final String getLinstorKey()
        {
            return formattedPrimaryKey;
        }

        @Override
        @JsonIgnore
        public DatabaseTable getDatabaseTable()
        {
            return GeneratedDatabaseTables.NODE_CONNECTIONS;
        }
    }

    @Version(GenCrdV1_18_2.VERSION)
    @Group(GenCrdV1_18_2.GROUP)
    @Plural("nodeconnections")
    @Singular("nodeconnections")
    public static class NodeConnections extends CustomResource<NodeConnectionsSpec, Void> implements LinstorCrd<NodeConnectionsSpec>
    {
        private static final long serialVersionUID = 5308286469092567739L;
        String k8sKey = null;

        @JsonCreator
        public NodeConnections()
        {
        }

        public NodeConnections(NodeConnectionsSpec spec)
        {
            setMetadata(new ObjectMetaBuilder().withName(deriveKey(spec.getLinstorKey())).build());
            setSpec(spec);
        }

        @Override
        public void setMetadata(ObjectMeta metadataRef)
        {
            super.setMetadata(metadataRef);
            k8sKey = metadataRef.getName();
        }

        @Override
        @JsonIgnore
        public String getLinstorKey()
        {
            return spec.getLinstorKey();
        }

        @Override
        @JsonIgnore
        public String getK8sKey()
        {
            return k8sKey;
        }
    }

    public static NodeNetInterfaces createNodeNetInterfaces(
        String uuid,
        String nodeName,
        String nodeNetName,
        String nodeNetDspName,
        String inetAddress,
        Short stltConnPort,
        String stltConnEncrType
    )
    {
        return new NodeNetInterfaces(
            new NodeNetInterfacesSpec(
                uuid,
                nodeName,
                nodeNetName,
                nodeNetDspName,
                inetAddress,
                stltConnPort,
                stltConnEncrType
            )
        );
    }

    @SuppressWarnings("rawtypes")
    @JsonInclude(Include.NON_NULL)
    public static class NodeNetInterfacesSpec implements LinstorSpec
    {
        @JsonIgnore private static final long serialVersionUID = 7874610890268671129L;
        @JsonIgnore private static final String PK_FORMAT = "%s:%s";

        @JsonIgnore private final String formattedPrimaryKey;

        @JsonProperty("uuid") public final String uuid;
        @JsonProperty("node_name") public final String nodeName; // PK
        @JsonProperty("node_net_name") public final String nodeNetName; // PK
        @JsonProperty("node_net_dsp_name") public final String nodeNetDspName;
        @JsonProperty("inet_address") public final String inetAddress;
        @JsonProperty("stlt_conn_port") public final Short stltConnPort;
        @JsonProperty("stlt_conn_encr_type") public final String stltConnEncrType;

        @JsonCreator
        public NodeNetInterfacesSpec(
            @JsonProperty("uuid") String uuidRef,
            @JsonProperty("node_name") String nodeNameRef,
            @JsonProperty("node_net_name") String nodeNetNameRef,
            @JsonProperty("node_net_dsp_name") String nodeNetDspNameRef,
            @JsonProperty("inet_address") String inetAddressRef,
            @JsonProperty("stlt_conn_port") Short stltConnPortRef,
            @JsonProperty("stlt_conn_encr_type") String stltConnEncrTypeRef
        )
        {
            uuid = uuidRef;
            nodeName = nodeNameRef;
            nodeNetName = nodeNetNameRef;
            nodeNetDspName = nodeNetDspNameRef;
            inetAddress = inetAddressRef;
            stltConnPort = stltConnPortRef;
            stltConnEncrType = stltConnEncrTypeRef;

            formattedPrimaryKey = String.format(
                NodeNetInterfacesSpec.PK_FORMAT,
                nodeName,
                nodeNetName
            );
        }

        @JsonIgnore
        @Override
        @SuppressWarnings("rawtypes")
        public LinstorCrd getCrd()
        {
            throw new ImplementationError("Pre 1_19_1 does not support this method");
        }

        @JsonIgnore
        @Override
        public Map<String, Object> asRawParameters()
        {
            Map<String, Object> ret = new TreeMap<>();
            ret.put("UUID", uuid);
            ret.put("NODE_NAME", nodeName);
            ret.put("NODE_NET_NAME", nodeNetName);
            ret.put("NODE_NET_DSP_NAME", nodeNetDspName);
            ret.put("INET_ADDRESS", inetAddress);
            ret.put("STLT_CONN_PORT", stltConnPort);
            ret.put("STLT_CONN_ENCR_TYPE", stltConnEncrType);
            return ret;
        }

        @JsonIgnore
        @Override
        public Object getByColumn(String clmNameStr)
        {
            switch(clmNameStr)
            {
                case "UUID":
                    return uuid;
                case "NODE_NAME":
                    return nodeName;
                case "NODE_NET_NAME":
                    return nodeNetName;
                case "NODE_NET_DSP_NAME":
                    return nodeNetDspName;
                case "INET_ADDRESS":
                    return inetAddress;
                case "STLT_CONN_PORT":
                    return stltConnPort;
                case "STLT_CONN_ENCR_TYPE":
                    return stltConnEncrType;
                default:
                    throw new ImplementationError("Unknown database column. Table: NODE_NET_INTERFACES, Column: " + clmNameStr);
            }
        }

        @JsonIgnore
        @Override
        public final String getLinstorKey()
        {
            return formattedPrimaryKey;
        }

        @Override
        @JsonIgnore
        public DatabaseTable getDatabaseTable()
        {
            return GeneratedDatabaseTables.NODE_NET_INTERFACES;
        }
    }

    @Version(GenCrdV1_18_2.VERSION)
    @Group(GenCrdV1_18_2.GROUP)
    @Plural("nodenetinterfaces")
    @Singular("nodenetinterfaces")
    public static class NodeNetInterfaces extends CustomResource<NodeNetInterfacesSpec, Void> implements LinstorCrd<NodeNetInterfacesSpec>
    {
        private static final long serialVersionUID = -2826384915232238607L;
        String k8sKey = null;

        @JsonCreator
        public NodeNetInterfaces()
        {
        }

        public NodeNetInterfaces(NodeNetInterfacesSpec spec)
        {
            setMetadata(new ObjectMetaBuilder().withName(deriveKey(spec.getLinstorKey())).build());
            setSpec(spec);
        }

        @Override
        public void setMetadata(ObjectMeta metadataRef)
        {
            super.setMetadata(metadataRef);
            k8sKey = metadataRef.getName();
        }

        @Override
        @JsonIgnore
        public String getLinstorKey()
        {
            return spec.getLinstorKey();
        }

        @Override
        @JsonIgnore
        public String getK8sKey()
        {
            return k8sKey;
        }
    }

    public static NodeStorPool createNodeStorPool(
        String uuid,
        String nodeName,
        String poolName,
        String driverName,
        String freeSpaceMgrName,
        String freeSpaceMgrDspName,
        boolean externalLocking
    )
    {
        return new NodeStorPool(
            new NodeStorPoolSpec(
                uuid,
                nodeName,
                poolName,
                driverName,
                freeSpaceMgrName,
                freeSpaceMgrDspName,
                externalLocking
            )
        );
    }

    @SuppressWarnings("rawtypes")
    @JsonInclude(Include.NON_NULL)
    public static class NodeStorPoolSpec implements LinstorSpec
    {
        @JsonIgnore private static final long serialVersionUID = -2517557697476265815L;
        @JsonIgnore private static final String PK_FORMAT = "%s:%s";

        @JsonIgnore private final String formattedPrimaryKey;

        @JsonProperty("uuid") public final String uuid;
        @JsonProperty("node_name") public final String nodeName; // PK
        @JsonProperty("pool_name") public final String poolName; // PK
        @JsonProperty("driver_name") public final String driverName;
        @JsonProperty("free_space_mgr_name") public final String freeSpaceMgrName;
        @JsonProperty("free_space_mgr_dsp_name") public final String freeSpaceMgrDspName;
        @JsonProperty("external_locking") public final boolean externalLocking;

        @JsonCreator
        public NodeStorPoolSpec(
            @JsonProperty("uuid") String uuidRef,
            @JsonProperty("node_name") String nodeNameRef,
            @JsonProperty("pool_name") String poolNameRef,
            @JsonProperty("driver_name") String driverNameRef,
            @JsonProperty("free_space_mgr_name") String freeSpaceMgrNameRef,
            @JsonProperty("free_space_mgr_dsp_name") String freeSpaceMgrDspNameRef,
            @JsonProperty("external_locking") boolean externalLockingRef
        )
        {
            uuid = uuidRef;
            nodeName = nodeNameRef;
            poolName = poolNameRef;
            driverName = driverNameRef;
            freeSpaceMgrName = freeSpaceMgrNameRef;
            freeSpaceMgrDspName = freeSpaceMgrDspNameRef;
            externalLocking = externalLockingRef;

            formattedPrimaryKey = String.format(
                NodeStorPoolSpec.PK_FORMAT,
                nodeName,
                poolName
            );
        }

        @JsonIgnore
        @Override
        @SuppressWarnings("rawtypes")
        public LinstorCrd getCrd()
        {
            throw new ImplementationError("Pre 1_19_1 does not support this method");
        }

        @JsonIgnore
        @Override
        public Map<String, Object> asRawParameters()
        {
            Map<String, Object> ret = new TreeMap<>();
            ret.put("UUID", uuid);
            ret.put("NODE_NAME", nodeName);
            ret.put("POOL_NAME", poolName);
            ret.put("DRIVER_NAME", driverName);
            ret.put("FREE_SPACE_MGR_NAME", freeSpaceMgrName);
            ret.put("FREE_SPACE_MGR_DSP_NAME", freeSpaceMgrDspName);
            ret.put("EXTERNAL_LOCKING", externalLocking);
            return ret;
        }

        @JsonIgnore
        @Override
        public Object getByColumn(String clmNameStr)
        {
            switch(clmNameStr)
            {
                case "UUID":
                    return uuid;
                case "NODE_NAME":
                    return nodeName;
                case "POOL_NAME":
                    return poolName;
                case "DRIVER_NAME":
                    return driverName;
                case "FREE_SPACE_MGR_NAME":
                    return freeSpaceMgrName;
                case "FREE_SPACE_MGR_DSP_NAME":
                    return freeSpaceMgrDspName;
                case "EXTERNAL_LOCKING":
                    return externalLocking;
                default:
                    throw new ImplementationError("Unknown database column. Table: NODE_STOR_POOL, Column: " + clmNameStr);
            }
        }

        @JsonIgnore
        @Override
        public final String getLinstorKey()
        {
            return formattedPrimaryKey;
        }

        @Override
        @JsonIgnore
        public DatabaseTable getDatabaseTable()
        {
            return GeneratedDatabaseTables.NODE_STOR_POOL;
        }
    }

    @Version(GenCrdV1_18_2.VERSION)
    @Group(GenCrdV1_18_2.GROUP)
    @Plural("nodestorpool")
    @Singular("nodestorpool")
    public static class NodeStorPool extends CustomResource<NodeStorPoolSpec, Void> implements LinstorCrd<NodeStorPoolSpec>
    {
        private static final long serialVersionUID = -761147063049076002L;
        String k8sKey = null;

        @JsonCreator
        public NodeStorPool()
        {
        }

        public NodeStorPool(NodeStorPoolSpec spec)
        {
            setMetadata(new ObjectMetaBuilder().withName(deriveKey(spec.getLinstorKey())).build());
            setSpec(spec);
        }

        @Override
        public void setMetadata(ObjectMeta metadataRef)
        {
            super.setMetadata(metadataRef);
            k8sKey = metadataRef.getName();
        }

        @Override
        @JsonIgnore
        public String getLinstorKey()
        {
            return spec.getLinstorKey();
        }

        @Override
        @JsonIgnore
        public String getK8sKey()
        {
            return k8sKey;
        }
    }

    public static PropsContainers createPropsContainers(
        String propsInstance,
        String propKey,
        String propValue
    )
    {
        return new PropsContainers(
            new PropsContainersSpec(
                propsInstance,
                propKey,
                propValue
            )
        );
    }

    @SuppressWarnings("rawtypes")
    @JsonInclude(Include.NON_NULL)
    public static class PropsContainersSpec implements LinstorSpec
    {
        @JsonIgnore private static final long serialVersionUID = 5661615817587305361L;
        @JsonIgnore private static final String PK_FORMAT = "%s:%s";

        @JsonIgnore private final String formattedPrimaryKey;

        @JsonProperty("props_instance") public final String propsInstance; // PK
        @JsonProperty("prop_key") public final String propKey; // PK
        @JsonProperty("prop_value") public final String propValue;

        @JsonCreator
        public PropsContainersSpec(
            @JsonProperty("props_instance") String propsInstanceRef,
            @JsonProperty("prop_key") String propKeyRef,
            @JsonProperty("prop_value") String propValueRef
        )
        {
            propsInstance = propsInstanceRef;
            propKey = propKeyRef;
            propValue = propValueRef;

            formattedPrimaryKey = String.format(
                PropsContainersSpec.PK_FORMAT,
                propsInstance,
                propKey
            );
        }

        @JsonIgnore
        @Override
        @SuppressWarnings("rawtypes")
        public LinstorCrd getCrd()
        {
            throw new ImplementationError("Pre 1_19_1 does not support this method");
        }

        @JsonIgnore
        @Override
        public Map<String, Object> asRawParameters()
        {
            Map<String, Object> ret = new TreeMap<>();
            ret.put("PROPS_INSTANCE", propsInstance);
            ret.put("PROP_KEY", propKey);
            ret.put("PROP_VALUE", propValue);
            return ret;
        }

        @JsonIgnore
        @Override
        public Object getByColumn(String clmNameStr)
        {
            switch(clmNameStr)
            {
                case "PROPS_INSTANCE":
                    return propsInstance;
                case "PROP_KEY":
                    return propKey;
                case "PROP_VALUE":
                    return propValue;
                default:
                    throw new ImplementationError("Unknown database column. Table: PROPS_CONTAINERS, Column: " + clmNameStr);
            }
        }

        @JsonIgnore
        @Override
        public final String getLinstorKey()
        {
            return formattedPrimaryKey;
        }

        @Override
        @JsonIgnore
        public DatabaseTable getDatabaseTable()
        {
            return GeneratedDatabaseTables.PROPS_CONTAINERS;
        }
    }

    @Version(GenCrdV1_18_2.VERSION)
    @Group(GenCrdV1_18_2.GROUP)
    @Plural("propscontainers")
    @Singular("propscontainers")
    public static class PropsContainers extends CustomResource<PropsContainersSpec, Void> implements LinstorCrd<PropsContainersSpec>
    {
        private static final long serialVersionUID = -6405027598215409128L;
        String k8sKey = null;

        @JsonCreator
        public PropsContainers()
        {
        }

        public PropsContainers(PropsContainersSpec spec)
        {
            setMetadata(new ObjectMetaBuilder().withName(deriveKey(spec.getLinstorKey())).build());
            setSpec(spec);
        }

        @Override
        public void setMetadata(ObjectMeta metadataRef)
        {
            super.setMetadata(metadataRef);
            k8sKey = metadataRef.getName();
        }

        @Override
        @JsonIgnore
        public String getLinstorKey()
        {
            return spec.getLinstorKey();
        }

        @Override
        @JsonIgnore
        public String getK8sKey()
        {
            return k8sKey;
        }
    }

    public static Resources createResources(
        String uuid,
        String nodeName,
        String resourceName,
        String snapshotName,
        long resourceFlags,
        Long createTimestamp
    )
    {
        return new Resources(
            new ResourcesSpec(
                uuid,
                nodeName,
                resourceName,
                snapshotName,
                resourceFlags,
                createTimestamp
            )
        );
    }

    @SuppressWarnings("rawtypes")
    @JsonInclude(Include.NON_NULL)
    public static class ResourcesSpec implements LinstorSpec
    {
        @JsonIgnore private static final long serialVersionUID = 9127178926668879961L;
        @JsonIgnore private static final String PK_FORMAT = "%s:%s:%s";

        @JsonIgnore private final String formattedPrimaryKey;

        @JsonProperty("uuid") public final String uuid;
        @JsonProperty("node_name") public final String nodeName; // PK
        @JsonProperty("resource_name") public final String resourceName; // PK
        @JsonProperty("snapshot_name") public final String snapshotName; // PK
        @JsonProperty("resource_flags") public final long resourceFlags;
        @JsonProperty("create_timestamp") public final Long createTimestamp;

        @JsonCreator
        public ResourcesSpec(
            @JsonProperty("uuid") String uuidRef,
            @JsonProperty("node_name") String nodeNameRef,
            @JsonProperty("resource_name") String resourceNameRef,
            @JsonProperty("snapshot_name") String snapshotNameRef,
            @JsonProperty("resource_flags") long resourceFlagsRef,
            @JsonProperty("create_timestamp") Long createTimestampRef
        )
        {
            uuid = uuidRef;
            nodeName = nodeNameRef;
            resourceName = resourceNameRef;
            snapshotName = snapshotNameRef;
            resourceFlags = resourceFlagsRef;
            createTimestamp = createTimestampRef;

            formattedPrimaryKey = String.format(
                ResourcesSpec.PK_FORMAT,
                nodeName,
                resourceName,
                snapshotName
            );
        }

        @JsonIgnore
        @Override
        @SuppressWarnings("rawtypes")
        public LinstorCrd getCrd()
        {
            throw new ImplementationError("Pre 1_19_1 does not support this method");
        }

        @JsonIgnore
        @Override
        public Map<String, Object> asRawParameters()
        {
            Map<String, Object> ret = new TreeMap<>();
            ret.put("UUID", uuid);
            ret.put("NODE_NAME", nodeName);
            ret.put("RESOURCE_NAME", resourceName);
            ret.put("SNAPSHOT_NAME", snapshotName);
            ret.put("RESOURCE_FLAGS", resourceFlags);
            ret.put("CREATE_TIMESTAMP", createTimestamp);
            return ret;
        }

        @JsonIgnore
        @Override
        public Object getByColumn(String clmNameStr)
        {
            switch(clmNameStr)
            {
                case "UUID":
                    return uuid;
                case "NODE_NAME":
                    return nodeName;
                case "RESOURCE_NAME":
                    return resourceName;
                case "SNAPSHOT_NAME":
                    return snapshotName;
                case "RESOURCE_FLAGS":
                    return resourceFlags;
                case "CREATE_TIMESTAMP":
                    return createTimestamp;
                default:
                    throw new ImplementationError("Unknown database column. Table: RESOURCES, Column: " + clmNameStr);
            }
        }

        @JsonIgnore
        @Override
        public final String getLinstorKey()
        {
            return formattedPrimaryKey;
        }

        @Override
        @JsonIgnore
        public DatabaseTable getDatabaseTable()
        {
            return GeneratedDatabaseTables.RESOURCES;
        }
    }

    @Version(GenCrdV1_18_2.VERSION)
    @Group(GenCrdV1_18_2.GROUP)
    @Plural("resources")
    @Singular("resources")
    public static class Resources extends CustomResource<ResourcesSpec, Void> implements LinstorCrd<ResourcesSpec>
    {
        private static final long serialVersionUID = 8839658324510628640L;
        String k8sKey = null;

        @JsonCreator
        public Resources()
        {
        }

        public Resources(ResourcesSpec spec)
        {
            setMetadata(new ObjectMetaBuilder().withName(deriveKey(spec.getLinstorKey())).build());
            setSpec(spec);
        }

        @Override
        public void setMetadata(ObjectMeta metadataRef)
        {
            super.setMetadata(metadataRef);
            k8sKey = metadataRef.getName();
        }

        @Override
        @JsonIgnore
        public String getLinstorKey()
        {
            return spec.getLinstorKey();
        }

        @Override
        @JsonIgnore
        public String getK8sKey()
        {
            return k8sKey;
        }
    }

    public static ResourceConnections createResourceConnections(
        String uuid,
        String nodeNameSrc,
        String nodeNameDst,
        String resourceName,
        String snapshotName,
        long flags,
        Integer tcpPort
    )
    {
        return new ResourceConnections(
            new ResourceConnectionsSpec(
                uuid,
                nodeNameSrc,
                nodeNameDst,
                resourceName,
                snapshotName,
                flags,
                tcpPort
            )
        );
    }

    @SuppressWarnings("rawtypes")
    @JsonInclude(Include.NON_NULL)
    public static class ResourceConnectionsSpec implements LinstorSpec
    {
        @JsonIgnore private static final long serialVersionUID = -6938449210781187832L;
        @JsonIgnore private static final String PK_FORMAT = "%s:%s:%s:%s";

        @JsonIgnore private final String formattedPrimaryKey;

        @JsonProperty("uuid") public final String uuid;
        @JsonProperty("node_name_src") public final String nodeNameSrc; // PK
        @JsonProperty("node_name_dst") public final String nodeNameDst; // PK
        @JsonProperty("resource_name") public final String resourceName; // PK
        @JsonProperty("snapshot_name") public final String snapshotName; // PK
        @JsonProperty("flags") public final long flags;
        @JsonProperty("tcp_port") public final Integer tcpPort;

        @JsonCreator
        public ResourceConnectionsSpec(
            @JsonProperty("uuid") String uuidRef,
            @JsonProperty("node_name_src") String nodeNameSrcRef,
            @JsonProperty("node_name_dst") String nodeNameDstRef,
            @JsonProperty("resource_name") String resourceNameRef,
            @JsonProperty("snapshot_name") String snapshotNameRef,
            @JsonProperty("flags") long flagsRef,
            @JsonProperty("tcp_port") Integer tcpPortRef
        )
        {
            uuid = uuidRef;
            nodeNameSrc = nodeNameSrcRef;
            nodeNameDst = nodeNameDstRef;
            resourceName = resourceNameRef;
            snapshotName = snapshotNameRef;
            flags = flagsRef;
            tcpPort = tcpPortRef;

            formattedPrimaryKey = String.format(
                ResourceConnectionsSpec.PK_FORMAT,
                nodeNameSrc,
                nodeNameDst,
                resourceName,
                snapshotName
            );
        }

        @JsonIgnore
        @Override
        @SuppressWarnings("rawtypes")
        public LinstorCrd getCrd()
        {
            throw new ImplementationError("Pre 1_19_1 does not support this method");
        }

        @JsonIgnore
        @Override
        public Map<String, Object> asRawParameters()
        {
            Map<String, Object> ret = new TreeMap<>();
            ret.put("UUID", uuid);
            ret.put("NODE_NAME_SRC", nodeNameSrc);
            ret.put("NODE_NAME_DST", nodeNameDst);
            ret.put("RESOURCE_NAME", resourceName);
            ret.put("SNAPSHOT_NAME", snapshotName);
            ret.put("FLAGS", flags);
            ret.put("TCP_PORT", tcpPort);
            return ret;
        }

        @JsonIgnore
        @Override
        public Object getByColumn(String clmNameStr)
        {
            switch(clmNameStr)
            {
                case "UUID":
                    return uuid;
                case "NODE_NAME_SRC":
                    return nodeNameSrc;
                case "NODE_NAME_DST":
                    return nodeNameDst;
                case "RESOURCE_NAME":
                    return resourceName;
                case "SNAPSHOT_NAME":
                    return snapshotName;
                case "FLAGS":
                    return flags;
                case "TCP_PORT":
                    return tcpPort;
                default:
                    throw new ImplementationError("Unknown database column. Table: RESOURCE_CONNECTIONS, Column: " + clmNameStr);
            }
        }

        @JsonIgnore
        @Override
        public final String getLinstorKey()
        {
            return formattedPrimaryKey;
        }

        @Override
        @JsonIgnore
        public DatabaseTable getDatabaseTable()
        {
            return GeneratedDatabaseTables.RESOURCE_CONNECTIONS;
        }
    }

    @Version(GenCrdV1_18_2.VERSION)
    @Group(GenCrdV1_18_2.GROUP)
    @Plural("resourceconnections")
    @Singular("resourceconnections")
    public static class ResourceConnections extends CustomResource<ResourceConnectionsSpec, Void> implements LinstorCrd<ResourceConnectionsSpec>
    {
        private static final long serialVersionUID = 1851362248183525847L;
        String k8sKey = null;

        @JsonCreator
        public ResourceConnections()
        {
        }

        public ResourceConnections(ResourceConnectionsSpec spec)
        {
            setMetadata(new ObjectMetaBuilder().withName(deriveKey(spec.getLinstorKey())).build());
            setSpec(spec);
        }

        @Override
        public void setMetadata(ObjectMeta metadataRef)
        {
            super.setMetadata(metadataRef);
            k8sKey = metadataRef.getName();
        }

        @Override
        @JsonIgnore
        public String getLinstorKey()
        {
            return spec.getLinstorKey();
        }

        @Override
        @JsonIgnore
        public String getK8sKey()
        {
            return k8sKey;
        }
    }

    public static ResourceDefinitions createResourceDefinitions(
        String uuid,
        String resourceName,
        String snapshotName,
        String resourceDspName,
        String snapshotDspName,
        long resourceFlags,
        String layerStack,
        byte[] resourceExternalName,
        String resourceGroupName,
        String parentUuid
    )
    {
        return new ResourceDefinitions(
            new ResourceDefinitionsSpec(
                uuid,
                resourceName,
                snapshotName,
                resourceDspName,
                snapshotDspName,
                resourceFlags,
                layerStack,
                resourceExternalName,
                resourceGroupName,
                parentUuid
            )
        );
    }

    @SuppressWarnings("rawtypes")
    @JsonInclude(Include.NON_NULL)
    public static class ResourceDefinitionsSpec implements LinstorSpec
    {
        @JsonIgnore private static final long serialVersionUID = 6535687732282691894L;
        @JsonIgnore private static final String PK_FORMAT = "%s:%s";

        @JsonIgnore private final String formattedPrimaryKey;

        @JsonProperty("uuid") public final String uuid;
        @JsonProperty("resource_name") public final String resourceName; // PK
        @JsonProperty("snapshot_name") public final String snapshotName; // PK
        @JsonProperty("resource_dsp_name") public final String resourceDspName;
        @JsonProperty("snapshot_dsp_name") public final String snapshotDspName;
        @JsonProperty("resource_flags") public final long resourceFlags;
        @JsonProperty("layer_stack") public final String layerStack;
        @JsonProperty("resource_external_name") public final byte[] resourceExternalName;
        @JsonProperty("resource_group_name") public final String resourceGroupName;
        @JsonProperty("parent_uuid") public final String parentUuid;

        @JsonCreator
        public ResourceDefinitionsSpec(
            @JsonProperty("uuid") String uuidRef,
            @JsonProperty("resource_name") String resourceNameRef,
            @JsonProperty("snapshot_name") String snapshotNameRef,
            @JsonProperty("resource_dsp_name") String resourceDspNameRef,
            @JsonProperty("snapshot_dsp_name") String snapshotDspNameRef,
            @JsonProperty("resource_flags") long resourceFlagsRef,
            @JsonProperty("layer_stack") String layerStackRef,
            @JsonProperty("resource_external_name") byte[] resourceExternalNameRef,
            @JsonProperty("resource_group_name") String resourceGroupNameRef,
            @JsonProperty("parent_uuid") String parentUuidRef
        )
        {
            uuid = uuidRef;
            resourceName = resourceNameRef;
            snapshotName = snapshotNameRef;
            resourceDspName = resourceDspNameRef;
            snapshotDspName = snapshotDspNameRef;
            resourceFlags = resourceFlagsRef;
            layerStack = layerStackRef;
            resourceExternalName = resourceExternalNameRef;
            resourceGroupName = resourceGroupNameRef;
            parentUuid = parentUuidRef;

            formattedPrimaryKey = String.format(
                ResourceDefinitionsSpec.PK_FORMAT,
                resourceName,
                snapshotName
            );
        }

        @JsonIgnore
        @Override
        @SuppressWarnings("rawtypes")
        public LinstorCrd getCrd()
        {
            throw new ImplementationError("Pre 1_19_1 does not support this method");
        }

        @JsonIgnore
        @Override
        public Map<String, Object> asRawParameters()
        {
            Map<String, Object> ret = new TreeMap<>();
            ret.put("UUID", uuid);
            ret.put("RESOURCE_NAME", resourceName);
            ret.put("SNAPSHOT_NAME", snapshotName);
            ret.put("RESOURCE_DSP_NAME", resourceDspName);
            ret.put("SNAPSHOT_DSP_NAME", snapshotDspName);
            ret.put("RESOURCE_FLAGS", resourceFlags);
            ret.put("LAYER_STACK", layerStack);
            ret.put("RESOURCE_EXTERNAL_NAME", resourceExternalName);
            ret.put("RESOURCE_GROUP_NAME", resourceGroupName);
            ret.put("PARENT_UUID", parentUuid);
            return ret;
        }

        @JsonIgnore
        @Override
        public Object getByColumn(String clmNameStr)
        {
            switch(clmNameStr)
            {
                case "UUID":
                    return uuid;
                case "RESOURCE_NAME":
                    return resourceName;
                case "SNAPSHOT_NAME":
                    return snapshotName;
                case "RESOURCE_DSP_NAME":
                    return resourceDspName;
                case "SNAPSHOT_DSP_NAME":
                    return snapshotDspName;
                case "RESOURCE_FLAGS":
                    return resourceFlags;
                case "LAYER_STACK":
                    return layerStack;
                case "RESOURCE_EXTERNAL_NAME":
                    return resourceExternalName;
                case "RESOURCE_GROUP_NAME":
                    return resourceGroupName;
                case "PARENT_UUID":
                    return parentUuid;
                default:
                    throw new ImplementationError("Unknown database column. Table: RESOURCE_DEFINITIONS, Column: " + clmNameStr);
            }
        }

        @JsonIgnore
        @Override
        public final String getLinstorKey()
        {
            return formattedPrimaryKey;
        }

        @Override
        @JsonIgnore
        public DatabaseTable getDatabaseTable()
        {
            return GeneratedDatabaseTables.RESOURCE_DEFINITIONS;
        }
    }

    @Version(GenCrdV1_18_2.VERSION)
    @Group(GenCrdV1_18_2.GROUP)
    @Plural("resourcedefinitions")
    @Singular("resourcedefinitions")
    public static class ResourceDefinitions extends CustomResource<ResourceDefinitionsSpec, Void> implements LinstorCrd<ResourceDefinitionsSpec>
    {
        private static final long serialVersionUID = 8156288402588869424L;
        String k8sKey = null;

        @JsonCreator
        public ResourceDefinitions()
        {
        }

        public ResourceDefinitions(ResourceDefinitionsSpec spec)
        {
            setMetadata(new ObjectMetaBuilder().withName(deriveKey(spec.getLinstorKey())).build());
            setSpec(spec);
        }

        @Override
        public void setMetadata(ObjectMeta metadataRef)
        {
            super.setMetadata(metadataRef);
            k8sKey = metadataRef.getName();
        }

        @Override
        @JsonIgnore
        public String getLinstorKey()
        {
            return spec.getLinstorKey();
        }

        @Override
        @JsonIgnore
        public String getK8sKey()
        {
            return k8sKey;
        }
    }

    public static ResourceGroups createResourceGroups(
        String uuid,
        String resourceGroupName,
        String resourceGroupDspName,
        String description,
        String layerStack,
        int replicaCount,
        String nodeNameList,
        String poolName,
        String poolNameDiskless,
        String doNotPlaceWithRscRegex,
        String doNotPlaceWithRscList,
        String replicasOnSame,
        String replicasOnDifferent,
        String allowedProviderList,
        Boolean disklessOnRemaining
    )
    {
        return new ResourceGroups(
            new ResourceGroupsSpec(
                uuid,
                resourceGroupName,
                resourceGroupDspName,
                description,
                layerStack,
                replicaCount,
                nodeNameList,
                poolName,
                poolNameDiskless,
                doNotPlaceWithRscRegex,
                doNotPlaceWithRscList,
                replicasOnSame,
                replicasOnDifferent,
                allowedProviderList,
                disklessOnRemaining
            )
        );
    }

    @SuppressWarnings("rawtypes")
    @JsonInclude(Include.NON_NULL)
    public static class ResourceGroupsSpec implements LinstorSpec
    {
        @JsonIgnore private static final long serialVersionUID = -8995543517386875425L;
        @JsonIgnore private static final String PK_FORMAT = "%s";

        @JsonIgnore private final String formattedPrimaryKey;

        @JsonProperty("uuid") public final String uuid;
        @JsonProperty("resource_group_name") public final String resourceGroupName; // PK
        @JsonProperty("resource_group_dsp_name") public final String resourceGroupDspName;
        @JsonProperty("description") public final String description;
        @JsonProperty("layer_stack") public final String layerStack;
        @JsonProperty("replica_count") public final int replicaCount;
        @JsonProperty("node_name_list") public final String nodeNameList;
        @JsonProperty("pool_name") public final String poolName;
        @JsonProperty("pool_name_diskless") public final String poolNameDiskless;
        @JsonProperty("do_not_place_with_rsc_regex") public final String doNotPlaceWithRscRegex;
        @JsonProperty("do_not_place_with_rsc_list") public final String doNotPlaceWithRscList;
        @JsonProperty("replicas_on_same") public final String replicasOnSame;
        @JsonProperty("replicas_on_different") public final String replicasOnDifferent;
        @JsonProperty("allowed_provider_list") public final String allowedProviderList;
        @JsonProperty("diskless_on_remaining") public final Boolean disklessOnRemaining;

        @JsonCreator
        public ResourceGroupsSpec(
            @JsonProperty("uuid") String uuidRef,
            @JsonProperty("resource_group_name") String resourceGroupNameRef,
            @JsonProperty("resource_group_dsp_name") String resourceGroupDspNameRef,
            @JsonProperty("description") String descriptionRef,
            @JsonProperty("layer_stack") String layerStackRef,
            @JsonProperty("replica_count") int replicaCountRef,
            @JsonProperty("node_name_list") String nodeNameListRef,
            @JsonProperty("pool_name") String poolNameRef,
            @JsonProperty("pool_name_diskless") String poolNameDisklessRef,
            @JsonProperty("do_not_place_with_rsc_regex") String doNotPlaceWithRscRegexRef,
            @JsonProperty("do_not_place_with_rsc_list") String doNotPlaceWithRscListRef,
            @JsonProperty("replicas_on_same") String replicasOnSameRef,
            @JsonProperty("replicas_on_different") String replicasOnDifferentRef,
            @JsonProperty("allowed_provider_list") String allowedProviderListRef,
            @JsonProperty("diskless_on_remaining") Boolean disklessOnRemainingRef
        )
        {
            uuid = uuidRef;
            resourceGroupName = resourceGroupNameRef;
            resourceGroupDspName = resourceGroupDspNameRef;
            description = descriptionRef;
            layerStack = layerStackRef;
            replicaCount = replicaCountRef;
            nodeNameList = nodeNameListRef;
            poolName = poolNameRef;
            poolNameDiskless = poolNameDisklessRef;
            doNotPlaceWithRscRegex = doNotPlaceWithRscRegexRef;
            doNotPlaceWithRscList = doNotPlaceWithRscListRef;
            replicasOnSame = replicasOnSameRef;
            replicasOnDifferent = replicasOnDifferentRef;
            allowedProviderList = allowedProviderListRef;
            disklessOnRemaining = disklessOnRemainingRef;

            formattedPrimaryKey = String.format(
                ResourceGroupsSpec.PK_FORMAT,
                resourceGroupName
            );
        }

        @JsonIgnore
        @Override
        @SuppressWarnings("rawtypes")
        public LinstorCrd getCrd()
        {
            throw new ImplementationError("Pre 1_19_1 does not support this method");
        }

        @JsonIgnore
        @Override
        public Map<String, Object> asRawParameters()
        {
            Map<String, Object> ret = new TreeMap<>();
            ret.put("UUID", uuid);
            ret.put("RESOURCE_GROUP_NAME", resourceGroupName);
            ret.put("RESOURCE_GROUP_DSP_NAME", resourceGroupDspName);
            ret.put("DESCRIPTION", description);
            ret.put("LAYER_STACK", layerStack);
            ret.put("REPLICA_COUNT", replicaCount);
            ret.put("NODE_NAME_LIST", nodeNameList);
            ret.put("POOL_NAME", poolName);
            ret.put("POOL_NAME_DISKLESS", poolNameDiskless);
            ret.put("DO_NOT_PLACE_WITH_RSC_REGEX", doNotPlaceWithRscRegex);
            ret.put("DO_NOT_PLACE_WITH_RSC_LIST", doNotPlaceWithRscList);
            ret.put("REPLICAS_ON_SAME", replicasOnSame);
            ret.put("REPLICAS_ON_DIFFERENT", replicasOnDifferent);
            ret.put("ALLOWED_PROVIDER_LIST", allowedProviderList);
            ret.put("DISKLESS_ON_REMAINING", disklessOnRemaining);
            return ret;
        }

        @JsonIgnore
        @Override
        public Object getByColumn(String clmNameStr)
        {
            switch(clmNameStr)
            {
                case "UUID":
                    return uuid;
                case "RESOURCE_GROUP_NAME":
                    return resourceGroupName;
                case "RESOURCE_GROUP_DSP_NAME":
                    return resourceGroupDspName;
                case "DESCRIPTION":
                    return description;
                case "LAYER_STACK":
                    return layerStack;
                case "REPLICA_COUNT":
                    return replicaCount;
                case "NODE_NAME_LIST":
                    return nodeNameList;
                case "POOL_NAME":
                    return poolName;
                case "POOL_NAME_DISKLESS":
                    return poolNameDiskless;
                case "DO_NOT_PLACE_WITH_RSC_REGEX":
                    return doNotPlaceWithRscRegex;
                case "DO_NOT_PLACE_WITH_RSC_LIST":
                    return doNotPlaceWithRscList;
                case "REPLICAS_ON_SAME":
                    return replicasOnSame;
                case "REPLICAS_ON_DIFFERENT":
                    return replicasOnDifferent;
                case "ALLOWED_PROVIDER_LIST":
                    return allowedProviderList;
                case "DISKLESS_ON_REMAINING":
                    return disklessOnRemaining;
                default:
                    throw new ImplementationError("Unknown database column. Table: RESOURCE_GROUPS, Column: " + clmNameStr);
            }
        }

        @JsonIgnore
        @Override
        public final String getLinstorKey()
        {
            return formattedPrimaryKey;
        }

        @Override
        @JsonIgnore
        public DatabaseTable getDatabaseTable()
        {
            return GeneratedDatabaseTables.RESOURCE_GROUPS;
        }
    }

    @Version(GenCrdV1_18_2.VERSION)
    @Group(GenCrdV1_18_2.GROUP)
    @Plural("resourcegroups")
    @Singular("resourcegroups")
    public static class ResourceGroups extends CustomResource<ResourceGroupsSpec, Void> implements LinstorCrd<ResourceGroupsSpec>
    {
        private static final long serialVersionUID = -4913471919920321754L;
        String k8sKey = null;

        @JsonCreator
        public ResourceGroups()
        {
        }

        public ResourceGroups(ResourceGroupsSpec spec)
        {
            setMetadata(new ObjectMetaBuilder().withName(deriveKey(spec.getLinstorKey())).build());
            setSpec(spec);
        }

        @Override
        public void setMetadata(ObjectMeta metadataRef)
        {
            super.setMetadata(metadataRef);
            k8sKey = metadataRef.getName();
        }

        @Override
        @JsonIgnore
        public String getLinstorKey()
        {
            return spec.getLinstorKey();
        }

        @Override
        @JsonIgnore
        public String getK8sKey()
        {
            return k8sKey;
        }
    }

    public static S3Remotes createS3Remotes(
        String uuid,
        String name,
        String dspName,
        long flags,
        String endpoint,
        String bucket,
        String region,
        byte[] accessKey,
        byte[] secretKey
    )
    {
        return new S3Remotes(
            new S3RemotesSpec(
                uuid,
                name,
                dspName,
                flags,
                endpoint,
                bucket,
                region,
                accessKey,
                secretKey
            )
        );
    }

    @SuppressWarnings("rawtypes")
    @JsonInclude(Include.NON_NULL)
    public static class S3RemotesSpec implements LinstorSpec
    {
        @JsonIgnore private static final long serialVersionUID = -6602540188360831367L;
        @JsonIgnore private static final String PK_FORMAT = "%s";

        @JsonIgnore private final String formattedPrimaryKey;

        @JsonProperty("uuid") public final String uuid;
        @JsonProperty("name") public final String name; // PK
        @JsonProperty("dsp_name") public final String dspName;
        @JsonProperty("flags") public final long flags;
        @JsonProperty("endpoint") public final String endpoint;
        @JsonProperty("bucket") public final String bucket;
        @JsonProperty("region") public final String region;
        @JsonProperty("access_key") public final byte[] accessKey;
        @JsonProperty("secret_key") public final byte[] secretKey;

        @JsonCreator
        public S3RemotesSpec(
            @JsonProperty("uuid") String uuidRef,
            @JsonProperty("name") String nameRef,
            @JsonProperty("dsp_name") String dspNameRef,
            @JsonProperty("flags") long flagsRef,
            @JsonProperty("endpoint") String endpointRef,
            @JsonProperty("bucket") String bucketRef,
            @JsonProperty("region") String regionRef,
            @JsonProperty("access_key") byte[] accessKeyRef,
            @JsonProperty("secret_key") byte[] secretKeyRef
        )
        {
            uuid = uuidRef;
            name = nameRef;
            dspName = dspNameRef;
            flags = flagsRef;
            endpoint = endpointRef;
            bucket = bucketRef;
            region = regionRef;
            accessKey = accessKeyRef;
            secretKey = secretKeyRef;

            formattedPrimaryKey = String.format(
                S3RemotesSpec.PK_FORMAT,
                name
            );
        }

        @JsonIgnore
        @Override
        @SuppressWarnings("rawtypes")
        public LinstorCrd getCrd()
        {
            throw new ImplementationError("Pre 1_19_1 does not support this method");
        }

        @JsonIgnore
        @Override
        public Map<String, Object> asRawParameters()
        {
            Map<String, Object> ret = new TreeMap<>();
            ret.put("UUID", uuid);
            ret.put("NAME", name);
            ret.put("DSP_NAME", dspName);
            ret.put("FLAGS", flags);
            ret.put("ENDPOINT", endpoint);
            ret.put("BUCKET", bucket);
            ret.put("REGION", region);
            ret.put("ACCESS_KEY", accessKey);
            ret.put("SECRET_KEY", secretKey);
            return ret;
        }

        @JsonIgnore
        @Override
        public Object getByColumn(String clmNameStr)
        {
            switch(clmNameStr)
            {
                case "UUID":
                    return uuid;
                case "NAME":
                    return name;
                case "DSP_NAME":
                    return dspName;
                case "FLAGS":
                    return flags;
                case "ENDPOINT":
                    return endpoint;
                case "BUCKET":
                    return bucket;
                case "REGION":
                    return region;
                case "ACCESS_KEY":
                    return accessKey;
                case "SECRET_KEY":
                    return secretKey;
                default:
                    throw new ImplementationError("Unknown database column. Table: S3_REMOTES, Column: " + clmNameStr);
            }
        }

        @JsonIgnore
        @Override
        public final String getLinstorKey()
        {
            return formattedPrimaryKey;
        }

        @Override
        @JsonIgnore
        public DatabaseTable getDatabaseTable()
        {
            return GeneratedDatabaseTables.S3_REMOTES;
        }
    }

    @Version(GenCrdV1_18_2.VERSION)
    @Group(GenCrdV1_18_2.GROUP)
    @Plural("s3remotes")
    @Singular("s3remotes")
    public static class S3Remotes extends CustomResource<S3RemotesSpec, Void> implements LinstorCrd<S3RemotesSpec>
    {
        private static final long serialVersionUID = 6866747096036316656L;
        String k8sKey = null;

        @JsonCreator
        public S3Remotes()
        {
        }

        public S3Remotes(S3RemotesSpec spec)
        {
            setMetadata(new ObjectMetaBuilder().withName(deriveKey(spec.getLinstorKey())).build());
            setSpec(spec);
        }

        @Override
        public void setMetadata(ObjectMeta metadataRef)
        {
            super.setMetadata(metadataRef);
            k8sKey = metadataRef.getName();
        }

        @Override
        @JsonIgnore
        public String getLinstorKey()
        {
            return spec.getLinstorKey();
        }

        @Override
        @JsonIgnore
        public String getK8sKey()
        {
            return k8sKey;
        }
    }

    public static SatellitesCapacity createSatellitesCapacity(
        String nodeName,
        byte[] capacity,
        boolean failFlag,
        byte[] allocated,
        byte[] usable
    )
    {
        return new SatellitesCapacity(
            new SatellitesCapacitySpec(
                nodeName,
                capacity,
                failFlag,
                allocated,
                usable
            )
        );
    }

    @SuppressWarnings("rawtypes")
    @JsonInclude(Include.NON_NULL)
    public static class SatellitesCapacitySpec implements LinstorSpec
    {
        @JsonIgnore private static final long serialVersionUID = -8279944099586981049L;
        @JsonIgnore private static final String PK_FORMAT = "%s";

        @JsonIgnore private final String formattedPrimaryKey;

        @JsonProperty("node_name") public final String nodeName; // PK
        @JsonProperty("capacity") public final byte[] capacity;
        @JsonProperty("fail_flag") public final boolean failFlag;
        @JsonProperty("allocated") public final byte[] allocated;
        @JsonProperty("usable") public final byte[] usable;

        @JsonCreator
        public SatellitesCapacitySpec(
            @JsonProperty("node_name") String nodeNameRef,
            @JsonProperty("capacity") byte[] capacityRef,
            @JsonProperty("fail_flag") boolean failFlagRef,
            @JsonProperty("allocated") byte[] allocatedRef,
            @JsonProperty("usable") byte[] usableRef
        )
        {
            nodeName = nodeNameRef;
            capacity = capacityRef;
            failFlag = failFlagRef;
            allocated = allocatedRef;
            usable = usableRef;

            formattedPrimaryKey = String.format(
                SatellitesCapacitySpec.PK_FORMAT,
                nodeName
            );
        }

        @JsonIgnore
        @Override
        @SuppressWarnings("rawtypes")
        public LinstorCrd getCrd()
        {
            throw new ImplementationError("Pre 1_19_1 does not support this method");
        }

        @JsonIgnore
        @Override
        public Map<String, Object> asRawParameters()
        {
            Map<String, Object> ret = new TreeMap<>();
            ret.put("NODE_NAME", nodeName);
            ret.put("CAPACITY", capacity);
            ret.put("FAIL_FLAG", failFlag);
            ret.put("ALLOCATED", allocated);
            ret.put("USABLE", usable);
            return ret;
        }

        @JsonIgnore
        @Override
        public Object getByColumn(String clmNameStr)
        {
            switch(clmNameStr)
            {
                case "NODE_NAME":
                    return nodeName;
                case "CAPACITY":
                    return capacity;
                case "FAIL_FLAG":
                    return failFlag;
                case "ALLOCATED":
                    return allocated;
                case "USABLE":
                    return usable;
                default:
                    throw new ImplementationError("Unknown database column. Table: SATELLITES_CAPACITY, Column: " + clmNameStr);
            }
        }

        @JsonIgnore
        @Override
        public final String getLinstorKey()
        {
            return formattedPrimaryKey;
        }

        @Override
        @JsonIgnore
        public DatabaseTable getDatabaseTable()
        {
            return GeneratedDatabaseTables.SATELLITES_CAPACITY;
        }
    }

    @Version(GenCrdV1_18_2.VERSION)
    @Group(GenCrdV1_18_2.GROUP)
    @Plural("satellitescapacity")
    @Singular("satellitescapacity")
    public static class SatellitesCapacity extends CustomResource<SatellitesCapacitySpec, Void> implements LinstorCrd<SatellitesCapacitySpec>
    {
        private static final long serialVersionUID = 70328678096896115L;
        String k8sKey = null;

        @JsonCreator
        public SatellitesCapacity()
        {
        }

        public SatellitesCapacity(SatellitesCapacitySpec spec)
        {
            setMetadata(new ObjectMetaBuilder().withName(deriveKey(spec.getLinstorKey())).build());
            setSpec(spec);
        }

        @Override
        public void setMetadata(ObjectMeta metadataRef)
        {
            super.setMetadata(metadataRef);
            k8sKey = metadataRef.getName();
        }

        @Override
        @JsonIgnore
        public String getLinstorKey()
        {
            return spec.getLinstorKey();
        }

        @Override
        @JsonIgnore
        public String getK8sKey()
        {
            return k8sKey;
        }
    }

    public static Schedules createSchedules(
        String uuid,
        String name,
        String dspName,
        long flags,
        String fullCron,
        String incCron,
        Integer keepLocal,
        Integer keepRemote,
        long onFailure,
        Integer maxRetries
    )
    {
        return new Schedules(
            new SchedulesSpec(
                uuid,
                name,
                dspName,
                flags,
                fullCron,
                incCron,
                keepLocal,
                keepRemote,
                onFailure,
                maxRetries
            )
        );
    }

    @SuppressWarnings("rawtypes")
    @JsonInclude(Include.NON_NULL)
    public static class SchedulesSpec implements LinstorSpec
    {
        @JsonIgnore private static final long serialVersionUID = 2663046669100453103L;
        @JsonIgnore private static final String PK_FORMAT = "%s";

        @JsonIgnore private final String formattedPrimaryKey;

        @JsonProperty("uuid") public final String uuid;
        @JsonProperty("name") public final String name; // PK
        @JsonProperty("dsp_name") public final String dspName;
        @JsonProperty("flags") public final long flags;
        @JsonProperty("full_cron") public final String fullCron;
        @JsonProperty("inc_cron") public final String incCron;
        @JsonProperty("keep_local") public final Integer keepLocal;
        @JsonProperty("keep_remote") public final Integer keepRemote;
        @JsonProperty("on_failure") public final long onFailure;
        @JsonProperty("max_retries") public final Integer maxRetries;

        @JsonCreator
        public SchedulesSpec(
            @JsonProperty("uuid") String uuidRef,
            @JsonProperty("name") String nameRef,
            @JsonProperty("dsp_name") String dspNameRef,
            @JsonProperty("flags") long flagsRef,
            @JsonProperty("full_cron") String fullCronRef,
            @JsonProperty("inc_cron") String incCronRef,
            @JsonProperty("keep_local") Integer keepLocalRef,
            @JsonProperty("keep_remote") Integer keepRemoteRef,
            @JsonProperty("on_failure") long onFailureRef,
            @JsonProperty("max_retries") Integer maxRetriesRef
        )
        {
            uuid = uuidRef;
            name = nameRef;
            dspName = dspNameRef;
            flags = flagsRef;
            fullCron = fullCronRef;
            incCron = incCronRef;
            keepLocal = keepLocalRef;
            keepRemote = keepRemoteRef;
            onFailure = onFailureRef;
            maxRetries = maxRetriesRef;

            formattedPrimaryKey = String.format(
                SchedulesSpec.PK_FORMAT,
                name
            );
        }

        @JsonIgnore
        @Override
        @SuppressWarnings("rawtypes")
        public LinstorCrd getCrd()
        {
            throw new ImplementationError("Pre 1_19_1 does not support this method");
        }

        @JsonIgnore
        @Override
        public Map<String, Object> asRawParameters()
        {
            Map<String, Object> ret = new TreeMap<>();
            ret.put("UUID", uuid);
            ret.put("NAME", name);
            ret.put("DSP_NAME", dspName);
            ret.put("FLAGS", flags);
            ret.put("FULL_CRON", fullCron);
            ret.put("INC_CRON", incCron);
            ret.put("KEEP_LOCAL", keepLocal);
            ret.put("KEEP_REMOTE", keepRemote);
            ret.put("ON_FAILURE", onFailure);
            ret.put("MAX_RETRIES", maxRetries);
            return ret;
        }

        @JsonIgnore
        @Override
        public Object getByColumn(String clmNameStr)
        {
            switch(clmNameStr)
            {
                case "UUID":
                    return uuid;
                case "NAME":
                    return name;
                case "DSP_NAME":
                    return dspName;
                case "FLAGS":
                    return flags;
                case "FULL_CRON":
                    return fullCron;
                case "INC_CRON":
                    return incCron;
                case "KEEP_LOCAL":
                    return keepLocal;
                case "KEEP_REMOTE":
                    return keepRemote;
                case "ON_FAILURE":
                    return onFailure;
                case "MAX_RETRIES":
                    return maxRetries;
                default:
                    throw new ImplementationError("Unknown database column. Table: SCHEDULES, Column: " + clmNameStr);
            }
        }

        @JsonIgnore
        @Override
        public final String getLinstorKey()
        {
            return formattedPrimaryKey;
        }

        @Override
        @JsonIgnore
        public DatabaseTable getDatabaseTable()
        {
            return GeneratedDatabaseTables.SCHEDULES;
        }
    }

    @Version(GenCrdV1_18_2.VERSION)
    @Group(GenCrdV1_18_2.GROUP)
    @Plural("schedules")
    @Singular("schedules")
    public static class Schedules extends CustomResource<SchedulesSpec, Void> implements LinstorCrd<SchedulesSpec>
    {
        private static final long serialVersionUID = -5133106596079703048L;
        String k8sKey = null;

        @JsonCreator
        public Schedules()
        {
        }

        public Schedules(SchedulesSpec spec)
        {
            setMetadata(new ObjectMetaBuilder().withName(deriveKey(spec.getLinstorKey())).build());
            setSpec(spec);
        }

        @Override
        public void setMetadata(ObjectMeta metadataRef)
        {
            super.setMetadata(metadataRef);
            k8sKey = metadataRef.getName();
        }

        @Override
        @JsonIgnore
        public String getLinstorKey()
        {
            return spec.getLinstorKey();
        }

        @Override
        @JsonIgnore
        public String getK8sKey()
        {
            return k8sKey;
        }
    }

    public static SecAccessTypes createSecAccessTypes(
        String accessTypeName,
        short accessTypeValue
    )
    {
        return new SecAccessTypes(
            new SecAccessTypesSpec(
                accessTypeName,
                accessTypeValue
            )
        );
    }

    @SuppressWarnings("rawtypes")
    @JsonInclude(Include.NON_NULL)
    public static class SecAccessTypesSpec implements LinstorSpec
    {
        @JsonIgnore private static final long serialVersionUID = -5310853667285563062L;
        @JsonIgnore private static final String PK_FORMAT = "%s";

        @JsonIgnore private final String formattedPrimaryKey;

        @JsonProperty("access_type_name") public final String accessTypeName; // PK
        @JsonProperty("access_type_value") public final short accessTypeValue;

        @JsonCreator
        public SecAccessTypesSpec(
            @JsonProperty("access_type_name") String accessTypeNameRef,
            @JsonProperty("access_type_value") short accessTypeValueRef
        )
        {
            accessTypeName = accessTypeNameRef;
            accessTypeValue = accessTypeValueRef;

            formattedPrimaryKey = String.format(
                SecAccessTypesSpec.PK_FORMAT,
                accessTypeName
            );
        }

        @JsonIgnore
        @Override
        @SuppressWarnings("rawtypes")
        public LinstorCrd getCrd()
        {
            throw new ImplementationError("Pre 1_19_1 does not support this method");
        }

        @JsonIgnore
        @Override
        public Map<String, Object> asRawParameters()
        {
            Map<String, Object> ret = new TreeMap<>();
            ret.put("ACCESS_TYPE_NAME", accessTypeName);
            ret.put("ACCESS_TYPE_VALUE", accessTypeValue);
            return ret;
        }

        @JsonIgnore
        @Override
        public Object getByColumn(String clmNameStr)
        {
            switch(clmNameStr)
            {
                case "ACCESS_TYPE_NAME":
                    return accessTypeName;
                case "ACCESS_TYPE_VALUE":
                    return accessTypeValue;
                default:
                    throw new ImplementationError("Unknown database column. Table: SEC_ACCESS_TYPES, Column: " + clmNameStr);
            }
        }

        @JsonIgnore
        @Override
        public final String getLinstorKey()
        {
            return formattedPrimaryKey;
        }

        @Override
        @JsonIgnore
        public DatabaseTable getDatabaseTable()
        {
            return GeneratedDatabaseTables.SEC_ACCESS_TYPES;
        }
    }

    @Version(GenCrdV1_18_2.VERSION)
    @Group(GenCrdV1_18_2.GROUP)
    @Plural("secaccesstypes")
    @Singular("secaccesstypes")
    public static class SecAccessTypes extends CustomResource<SecAccessTypesSpec, Void> implements LinstorCrd<SecAccessTypesSpec>
    {
        private static final long serialVersionUID = 6494610534057646367L;
        String k8sKey = null;

        @JsonCreator
        public SecAccessTypes()
        {
        }

        public SecAccessTypes(SecAccessTypesSpec spec)
        {
            setMetadata(new ObjectMetaBuilder().withName(deriveKey(spec.getLinstorKey())).build());
            setSpec(spec);
        }

        @Override
        public void setMetadata(ObjectMeta metadataRef)
        {
            super.setMetadata(metadataRef);
            k8sKey = metadataRef.getName();
        }

        @Override
        @JsonIgnore
        public String getLinstorKey()
        {
            return spec.getLinstorKey();
        }

        @Override
        @JsonIgnore
        public String getK8sKey()
        {
            return k8sKey;
        }
    }

    public static SecAclMap createSecAclMap(
        String objectPath,
        String roleName,
        short accessType
    )
    {
        return new SecAclMap(
            new SecAclMapSpec(
                objectPath,
                roleName,
                accessType
            )
        );
    }

    @SuppressWarnings("rawtypes")
    @JsonInclude(Include.NON_NULL)
    public static class SecAclMapSpec implements LinstorSpec
    {
        @JsonIgnore private static final long serialVersionUID = -1820035107865495173L;
        @JsonIgnore private static final String PK_FORMAT = "%s:%s";

        @JsonIgnore private final String formattedPrimaryKey;

        @JsonProperty("object_path") public final String objectPath; // PK
        @JsonProperty("role_name") public final String roleName; // PK
        @JsonProperty("access_type") public final short accessType;

        @JsonCreator
        public SecAclMapSpec(
            @JsonProperty("object_path") String objectPathRef,
            @JsonProperty("role_name") String roleNameRef,
            @JsonProperty("access_type") short accessTypeRef
        )
        {
            objectPath = objectPathRef;
            roleName = roleNameRef;
            accessType = accessTypeRef;

            formattedPrimaryKey = String.format(
                SecAclMapSpec.PK_FORMAT,
                objectPath,
                roleName
            );
        }

        @JsonIgnore
        @Override
        @SuppressWarnings("rawtypes")
        public LinstorCrd getCrd()
        {
            throw new ImplementationError("Pre 1_19_1 does not support this method");
        }

        @JsonIgnore
        @Override
        public Map<String, Object> asRawParameters()
        {
            Map<String, Object> ret = new TreeMap<>();
            ret.put("OBJECT_PATH", objectPath);
            ret.put("ROLE_NAME", roleName);
            ret.put("ACCESS_TYPE", accessType);
            return ret;
        }

        @JsonIgnore
        @Override
        public Object getByColumn(String clmNameStr)
        {
            switch(clmNameStr)
            {
                case "OBJECT_PATH":
                    return objectPath;
                case "ROLE_NAME":
                    return roleName;
                case "ACCESS_TYPE":
                    return accessType;
                default:
                    throw new ImplementationError("Unknown database column. Table: SEC_ACL_MAP, Column: " + clmNameStr);
            }
        }

        @JsonIgnore
        @Override
        public final String getLinstorKey()
        {
            return formattedPrimaryKey;
        }

        @Override
        @JsonIgnore
        public DatabaseTable getDatabaseTable()
        {
            return GeneratedDatabaseTables.SEC_ACL_MAP;
        }
    }

    @Version(GenCrdV1_18_2.VERSION)
    @Group(GenCrdV1_18_2.GROUP)
    @Plural("secaclmap")
    @Singular("secaclmap")
    public static class SecAclMap extends CustomResource<SecAclMapSpec, Void> implements LinstorCrd<SecAclMapSpec>
    {
        private static final long serialVersionUID = 7435553859257409042L;
        String k8sKey = null;

        @JsonCreator
        public SecAclMap()
        {
        }

        public SecAclMap(SecAclMapSpec spec)
        {
            setMetadata(new ObjectMetaBuilder().withName(deriveKey(spec.getLinstorKey())).build());
            setSpec(spec);
        }

        @Override
        public void setMetadata(ObjectMeta metadataRef)
        {
            super.setMetadata(metadataRef);
            k8sKey = metadataRef.getName();
        }

        @Override
        @JsonIgnore
        public String getLinstorKey()
        {
            return spec.getLinstorKey();
        }

        @Override
        @JsonIgnore
        public String getK8sKey()
        {
            return k8sKey;
        }
    }

    public static SecConfiguration createSecConfiguration(
        String entryKey,
        String entryDspKey,
        String entryValue
    )
    {
        return new SecConfiguration(
            new SecConfigurationSpec(
                entryKey,
                entryDspKey,
                entryValue
            )
        );
    }

    @SuppressWarnings("rawtypes")
    @JsonInclude(Include.NON_NULL)
    public static class SecConfigurationSpec implements LinstorSpec
    {
        @JsonIgnore private static final long serialVersionUID = 8254920989854573890L;
        @JsonIgnore private static final String PK_FORMAT = "%s";

        @JsonIgnore private final String formattedPrimaryKey;

        @JsonProperty("entry_key") public final String entryKey; // PK
        @JsonProperty("entry_dsp_key") public final String entryDspKey;
        @JsonProperty("entry_value") public final String entryValue;

        @JsonCreator
        public SecConfigurationSpec(
            @JsonProperty("entry_key") String entryKeyRef,
            @JsonProperty("entry_dsp_key") String entryDspKeyRef,
            @JsonProperty("entry_value") String entryValueRef
        )
        {
            entryKey = entryKeyRef;
            entryDspKey = entryDspKeyRef;
            entryValue = entryValueRef;

            formattedPrimaryKey = String.format(
                SecConfigurationSpec.PK_FORMAT,
                entryKey
            );
        }

        @JsonIgnore
        @Override
        @SuppressWarnings("rawtypes")
        public LinstorCrd getCrd()
        {
            throw new ImplementationError("Pre 1_19_1 does not support this method");
        }

        @JsonIgnore
        @Override
        public Map<String, Object> asRawParameters()
        {
            Map<String, Object> ret = new TreeMap<>();
            ret.put("ENTRY_KEY", entryKey);
            ret.put("ENTRY_DSP_KEY", entryDspKey);
            ret.put("ENTRY_VALUE", entryValue);
            return ret;
        }

        @JsonIgnore
        @Override
        public Object getByColumn(String clmNameStr)
        {
            switch(clmNameStr)
            {
                case "ENTRY_KEY":
                    return entryKey;
                case "ENTRY_DSP_KEY":
                    return entryDspKey;
                case "ENTRY_VALUE":
                    return entryValue;
                default:
                    throw new ImplementationError("Unknown database column. Table: SEC_CONFIGURATION, Column: " + clmNameStr);
            }
        }

        @JsonIgnore
        @Override
        public final String getLinstorKey()
        {
            return formattedPrimaryKey;
        }

        @Override
        @JsonIgnore
        public DatabaseTable getDatabaseTable()
        {
            return GeneratedDatabaseTables.SEC_CONFIGURATION;
        }
    }

    @Version(GenCrdV1_18_2.VERSION)
    @Group(GenCrdV1_18_2.GROUP)
    @Plural("secconfiguration")
    @Singular("secconfiguration")
    public static class SecConfiguration extends CustomResource<SecConfigurationSpec, Void> implements LinstorCrd<SecConfigurationSpec>
    {
        private static final long serialVersionUID = 3406278239411432444L;
        String k8sKey = null;

        @JsonCreator
        public SecConfiguration()
        {
        }

        public SecConfiguration(SecConfigurationSpec spec)
        {
            setMetadata(new ObjectMetaBuilder().withName(deriveKey(spec.getLinstorKey())).build());
            setSpec(spec);
        }

        @Override
        public void setMetadata(ObjectMeta metadataRef)
        {
            super.setMetadata(metadataRef);
            k8sKey = metadataRef.getName();
        }

        @Override
        @JsonIgnore
        public String getLinstorKey()
        {
            return spec.getLinstorKey();
        }

        @Override
        @JsonIgnore
        public String getK8sKey()
        {
            return k8sKey;
        }
    }

    public static SecDfltRoles createSecDfltRoles(
        String identityName,
        String roleName
    )
    {
        return new SecDfltRoles(
            new SecDfltRolesSpec(
                identityName,
                roleName
            )
        );
    }

    @SuppressWarnings("rawtypes")
    @JsonInclude(Include.NON_NULL)
    public static class SecDfltRolesSpec implements LinstorSpec
    {
        @JsonIgnore private static final long serialVersionUID = 6973858763600109458L;
        @JsonIgnore private static final String PK_FORMAT = "%s";

        @JsonIgnore private final String formattedPrimaryKey;

        @JsonProperty("identity_name") public final String identityName; // PK
        @JsonProperty("role_name") public final String roleName;

        @JsonCreator
        public SecDfltRolesSpec(
            @JsonProperty("identity_name") String identityNameRef,
            @JsonProperty("role_name") String roleNameRef
        )
        {
            identityName = identityNameRef;
            roleName = roleNameRef;

            formattedPrimaryKey = String.format(
                SecDfltRolesSpec.PK_FORMAT,
                identityName
            );
        }

        @JsonIgnore
        @Override
        @SuppressWarnings("rawtypes")
        public LinstorCrd getCrd()
        {
            throw new ImplementationError("Pre 1_19_1 does not support this method");
        }

        @JsonIgnore
        @Override
        public Map<String, Object> asRawParameters()
        {
            Map<String, Object> ret = new TreeMap<>();
            ret.put("IDENTITY_NAME", identityName);
            ret.put("ROLE_NAME", roleName);
            return ret;
        }

        @JsonIgnore
        @Override
        public Object getByColumn(String clmNameStr)
        {
            switch(clmNameStr)
            {
                case "IDENTITY_NAME":
                    return identityName;
                case "ROLE_NAME":
                    return roleName;
                default:
                    throw new ImplementationError("Unknown database column. Table: SEC_DFLT_ROLES, Column: " + clmNameStr);
            }
        }

        @JsonIgnore
        @Override
        public final String getLinstorKey()
        {
            return formattedPrimaryKey;
        }

        @Override
        @JsonIgnore
        public DatabaseTable getDatabaseTable()
        {
            return GeneratedDatabaseTables.SEC_DFLT_ROLES;
        }
    }

    @Version(GenCrdV1_18_2.VERSION)
    @Group(GenCrdV1_18_2.GROUP)
    @Plural("secdfltroles")
    @Singular("secdfltroles")
    public static class SecDfltRoles extends CustomResource<SecDfltRolesSpec, Void> implements LinstorCrd<SecDfltRolesSpec>
    {
        private static final long serialVersionUID = -6861670300386478148L;
        String k8sKey = null;

        @JsonCreator
        public SecDfltRoles()
        {
        }

        public SecDfltRoles(SecDfltRolesSpec spec)
        {
            setMetadata(new ObjectMetaBuilder().withName(deriveKey(spec.getLinstorKey())).build());
            setSpec(spec);
        }

        @Override
        public void setMetadata(ObjectMeta metadataRef)
        {
            super.setMetadata(metadataRef);
            k8sKey = metadataRef.getName();
        }

        @Override
        @JsonIgnore
        public String getLinstorKey()
        {
            return spec.getLinstorKey();
        }

        @Override
        @JsonIgnore
        public String getK8sKey()
        {
            return k8sKey;
        }
    }

    public static SecIdentities createSecIdentities(
        String identityName,
        String identityDspName,
        String passSalt,
        String passHash,
        boolean idEnabled,
        boolean idLocked
    )
    {
        return new SecIdentities(
            new SecIdentitiesSpec(
                identityName,
                identityDspName,
                passSalt,
                passHash,
                idEnabled,
                idLocked
            )
        );
    }

    @SuppressWarnings("rawtypes")
    @JsonInclude(Include.NON_NULL)
    public static class SecIdentitiesSpec implements LinstorSpec
    {
        @JsonIgnore private static final long serialVersionUID = 8274893620932362336L;
        @JsonIgnore private static final String PK_FORMAT = "%s";

        @JsonIgnore private final String formattedPrimaryKey;

        @JsonProperty("identity_name") public final String identityName; // PK
        @JsonProperty("identity_dsp_name") public final String identityDspName;
        @JsonProperty("pass_salt") public final String passSalt;
        @JsonProperty("pass_hash") public final String passHash;
        @JsonProperty("id_enabled") public final boolean idEnabled;
        @JsonProperty("id_locked") public final boolean idLocked;

        @JsonCreator
        public SecIdentitiesSpec(
            @JsonProperty("identity_name") String identityNameRef,
            @JsonProperty("identity_dsp_name") String identityDspNameRef,
            @JsonProperty("pass_salt") String passSaltRef,
            @JsonProperty("pass_hash") String passHashRef,
            @JsonProperty("id_enabled") boolean idEnabledRef,
            @JsonProperty("id_locked") boolean idLockedRef
        )
        {
            identityName = identityNameRef;
            identityDspName = identityDspNameRef;
            passSalt = passSaltRef;
            passHash = passHashRef;
            idEnabled = idEnabledRef;
            idLocked = idLockedRef;

            formattedPrimaryKey = String.format(
                SecIdentitiesSpec.PK_FORMAT,
                identityName
            );
        }

        @JsonIgnore
        @Override
        @SuppressWarnings("rawtypes")
        public LinstorCrd getCrd()
        {
            throw new ImplementationError("Pre 1_19_1 does not support this method");
        }

        @JsonIgnore
        @Override
        public Map<String, Object> asRawParameters()
        {
            Map<String, Object> ret = new TreeMap<>();
            ret.put("IDENTITY_NAME", identityName);
            ret.put("IDENTITY_DSP_NAME", identityDspName);
            ret.put("PASS_SALT", passSalt);
            ret.put("PASS_HASH", passHash);
            ret.put("ID_ENABLED", idEnabled);
            ret.put("ID_LOCKED", idLocked);
            return ret;
        }

        @JsonIgnore
        @Override
        public Object getByColumn(String clmNameStr)
        {
            switch(clmNameStr)
            {
                case "IDENTITY_NAME":
                    return identityName;
                case "IDENTITY_DSP_NAME":
                    return identityDspName;
                case "PASS_SALT":
                    return passSalt;
                case "PASS_HASH":
                    return passHash;
                case "ID_ENABLED":
                    return idEnabled;
                case "ID_LOCKED":
                    return idLocked;
                default:
                    throw new ImplementationError("Unknown database column. Table: SEC_IDENTITIES, Column: " + clmNameStr);
            }
        }

        @JsonIgnore
        @Override
        public final String getLinstorKey()
        {
            return formattedPrimaryKey;
        }

        @Override
        @JsonIgnore
        public DatabaseTable getDatabaseTable()
        {
            return GeneratedDatabaseTables.SEC_IDENTITIES;
        }
    }

    @Version(GenCrdV1_18_2.VERSION)
    @Group(GenCrdV1_18_2.GROUP)
    @Plural("secidentities")
    @Singular("secidentities")
    public static class SecIdentities extends CustomResource<SecIdentitiesSpec, Void> implements LinstorCrd<SecIdentitiesSpec>
    {
        private static final long serialVersionUID = -9105771554207417899L;
        String k8sKey = null;

        @JsonCreator
        public SecIdentities()
        {
        }

        public SecIdentities(SecIdentitiesSpec spec)
        {
            setMetadata(new ObjectMetaBuilder().withName(deriveKey(spec.getLinstorKey())).build());
            setSpec(spec);
        }

        @Override
        public void setMetadata(ObjectMeta metadataRef)
        {
            super.setMetadata(metadataRef);
            k8sKey = metadataRef.getName();
        }

        @Override
        @JsonIgnore
        public String getLinstorKey()
        {
            return spec.getLinstorKey();
        }

        @Override
        @JsonIgnore
        public String getK8sKey()
        {
            return k8sKey;
        }
    }

    public static SecIdRoleMap createSecIdRoleMap(
        String identityName,
        String roleName
    )
    {
        return new SecIdRoleMap(
            new SecIdRoleMapSpec(
                identityName,
                roleName
            )
        );
    }

    @SuppressWarnings("rawtypes")
    @JsonInclude(Include.NON_NULL)
    public static class SecIdRoleMapSpec implements LinstorSpec
    {
        @JsonIgnore private static final long serialVersionUID = -8068357435009701955L;
        @JsonIgnore private static final String PK_FORMAT = "%s:%s";

        @JsonIgnore private final String formattedPrimaryKey;

        @JsonProperty("identity_name") public final String identityName; // PK
        @JsonProperty("role_name") public final String roleName; // PK

        @JsonCreator
        public SecIdRoleMapSpec(
            @JsonProperty("identity_name") String identityNameRef,
            @JsonProperty("role_name") String roleNameRef
        )
        {
            identityName = identityNameRef;
            roleName = roleNameRef;

            formattedPrimaryKey = String.format(
                SecIdRoleMapSpec.PK_FORMAT,
                identityName,
                roleName
            );
        }

        @JsonIgnore
        @Override
        @SuppressWarnings("rawtypes")
        public LinstorCrd getCrd()
        {
            throw new ImplementationError("Pre 1_19_1 does not support this method");
        }

        @JsonIgnore
        @Override
        public Map<String, Object> asRawParameters()
        {
            Map<String, Object> ret = new TreeMap<>();
            ret.put("IDENTITY_NAME", identityName);
            ret.put("ROLE_NAME", roleName);
            return ret;
        }

        @JsonIgnore
        @Override
        public Object getByColumn(String clmNameStr)
        {
            switch(clmNameStr)
            {
                case "IDENTITY_NAME":
                    return identityName;
                case "ROLE_NAME":
                    return roleName;
                default:
                    throw new ImplementationError("Unknown database column. Table: SEC_ID_ROLE_MAP, Column: " + clmNameStr);
            }
        }

        @JsonIgnore
        @Override
        public final String getLinstorKey()
        {
            return formattedPrimaryKey;
        }

        @Override
        @JsonIgnore
        public DatabaseTable getDatabaseTable()
        {
            return GeneratedDatabaseTables.SEC_ID_ROLE_MAP;
        }
    }

    @Version(GenCrdV1_18_2.VERSION)
    @Group(GenCrdV1_18_2.GROUP)
    @Plural("secidrolemap")
    @Singular("secidrolemap")
    public static class SecIdRoleMap extends CustomResource<SecIdRoleMapSpec, Void> implements LinstorCrd<SecIdRoleMapSpec>
    {
        private static final long serialVersionUID = -6708148783277246039L;
        String k8sKey = null;

        @JsonCreator
        public SecIdRoleMap()
        {
        }

        public SecIdRoleMap(SecIdRoleMapSpec spec)
        {
            setMetadata(new ObjectMetaBuilder().withName(deriveKey(spec.getLinstorKey())).build());
            setSpec(spec);
        }

        @Override
        public void setMetadata(ObjectMeta metadataRef)
        {
            super.setMetadata(metadataRef);
            k8sKey = metadataRef.getName();
        }

        @Override
        @JsonIgnore
        public String getLinstorKey()
        {
            return spec.getLinstorKey();
        }

        @Override
        @JsonIgnore
        public String getK8sKey()
        {
            return k8sKey;
        }
    }

    public static SecObjectProtection createSecObjectProtection(
        String objectPath,
        String creatorIdentityName,
        String ownerRoleName,
        String securityTypeName
    )
    {
        return new SecObjectProtection(
            new SecObjectProtectionSpec(
                objectPath,
                creatorIdentityName,
                ownerRoleName,
                securityTypeName
            )
        );
    }

    @SuppressWarnings("rawtypes")
    @JsonInclude(Include.NON_NULL)
    public static class SecObjectProtectionSpec implements LinstorSpec
    {
        @JsonIgnore private static final long serialVersionUID = -8799200749434151386L;
        @JsonIgnore private static final String PK_FORMAT = "%s";

        @JsonIgnore private final String formattedPrimaryKey;

        @JsonProperty("object_path") public final String objectPath; // PK
        @JsonProperty("creator_identity_name") public final String creatorIdentityName;
        @JsonProperty("owner_role_name") public final String ownerRoleName;
        @JsonProperty("security_type_name") public final String securityTypeName;

        @JsonCreator
        public SecObjectProtectionSpec(
            @JsonProperty("object_path") String objectPathRef,
            @JsonProperty("creator_identity_name") String creatorIdentityNameRef,
            @JsonProperty("owner_role_name") String ownerRoleNameRef,
            @JsonProperty("security_type_name") String securityTypeNameRef
        )
        {
            objectPath = objectPathRef;
            creatorIdentityName = creatorIdentityNameRef;
            ownerRoleName = ownerRoleNameRef;
            securityTypeName = securityTypeNameRef;

            formattedPrimaryKey = String.format(
                SecObjectProtectionSpec.PK_FORMAT,
                objectPath
            );
        }

        @JsonIgnore
        @Override
        @SuppressWarnings("rawtypes")
        public LinstorCrd getCrd()
        {
            throw new ImplementationError("Pre 1_19_1 does not support this method");
        }

        @JsonIgnore
        @Override
        public Map<String, Object> asRawParameters()
        {
            Map<String, Object> ret = new TreeMap<>();
            ret.put("OBJECT_PATH", objectPath);
            ret.put("CREATOR_IDENTITY_NAME", creatorIdentityName);
            ret.put("OWNER_ROLE_NAME", ownerRoleName);
            ret.put("SECURITY_TYPE_NAME", securityTypeName);
            return ret;
        }

        @JsonIgnore
        @Override
        public Object getByColumn(String clmNameStr)
        {
            switch(clmNameStr)
            {
                case "OBJECT_PATH":
                    return objectPath;
                case "CREATOR_IDENTITY_NAME":
                    return creatorIdentityName;
                case "OWNER_ROLE_NAME":
                    return ownerRoleName;
                case "SECURITY_TYPE_NAME":
                    return securityTypeName;
                default:
                    throw new ImplementationError("Unknown database column. Table: SEC_OBJECT_PROTECTION, Column: " + clmNameStr);
            }
        }

        @JsonIgnore
        @Override
        public final String getLinstorKey()
        {
            return formattedPrimaryKey;
        }

        @Override
        @JsonIgnore
        public DatabaseTable getDatabaseTable()
        {
            return GeneratedDatabaseTables.SEC_OBJECT_PROTECTION;
        }
    }

    @Version(GenCrdV1_18_2.VERSION)
    @Group(GenCrdV1_18_2.GROUP)
    @Plural("secobjectprotection")
    @Singular("secobjectprotection")
    public static class SecObjectProtection extends CustomResource<SecObjectProtectionSpec, Void> implements LinstorCrd<SecObjectProtectionSpec>
    {
        private static final long serialVersionUID = -8149952182865192985L;
        String k8sKey = null;

        @JsonCreator
        public SecObjectProtection()
        {
        }

        public SecObjectProtection(SecObjectProtectionSpec spec)
        {
            setMetadata(new ObjectMetaBuilder().withName(deriveKey(spec.getLinstorKey())).build());
            setSpec(spec);
        }

        @Override
        public void setMetadata(ObjectMeta metadataRef)
        {
            super.setMetadata(metadataRef);
            k8sKey = metadataRef.getName();
        }

        @Override
        @JsonIgnore
        public String getLinstorKey()
        {
            return spec.getLinstorKey();
        }

        @Override
        @JsonIgnore
        public String getK8sKey()
        {
            return k8sKey;
        }
    }

    public static SecRoles createSecRoles(
        String roleName,
        String roleDspName,
        String domainName,
        boolean roleEnabled,
        long rolePrivileges
    )
    {
        return new SecRoles(
            new SecRolesSpec(
                roleName,
                roleDspName,
                domainName,
                roleEnabled,
                rolePrivileges
            )
        );
    }

    @SuppressWarnings("rawtypes")
    @JsonInclude(Include.NON_NULL)
    public static class SecRolesSpec implements LinstorSpec
    {
        @JsonIgnore private static final long serialVersionUID = -5147224635267848677L;
        @JsonIgnore private static final String PK_FORMAT = "%s";

        @JsonIgnore private final String formattedPrimaryKey;

        @JsonProperty("role_name") public final String roleName; // PK
        @JsonProperty("role_dsp_name") public final String roleDspName;
        @JsonProperty("domain_name") public final String domainName;
        @JsonProperty("role_enabled") public final boolean roleEnabled;
        @JsonProperty("role_privileges") public final long rolePrivileges;

        @JsonCreator
        public SecRolesSpec(
            @JsonProperty("role_name") String roleNameRef,
            @JsonProperty("role_dsp_name") String roleDspNameRef,
            @JsonProperty("domain_name") String domainNameRef,
            @JsonProperty("role_enabled") boolean roleEnabledRef,
            @JsonProperty("role_privileges") long rolePrivilegesRef
        )
        {
            roleName = roleNameRef;
            roleDspName = roleDspNameRef;
            domainName = domainNameRef;
            roleEnabled = roleEnabledRef;
            rolePrivileges = rolePrivilegesRef;

            formattedPrimaryKey = String.format(
                SecRolesSpec.PK_FORMAT,
                roleName
            );
        }

        @JsonIgnore
        @Override
        @SuppressWarnings("rawtypes")
        public LinstorCrd getCrd()
        {
            throw new ImplementationError("Pre 1_19_1 does not support this method");
        }

        @JsonIgnore
        @Override
        public Map<String, Object> asRawParameters()
        {
            Map<String, Object> ret = new TreeMap<>();
            ret.put("ROLE_NAME", roleName);
            ret.put("ROLE_DSP_NAME", roleDspName);
            ret.put("DOMAIN_NAME", domainName);
            ret.put("ROLE_ENABLED", roleEnabled);
            ret.put("ROLE_PRIVILEGES", rolePrivileges);
            return ret;
        }

        @JsonIgnore
        @Override
        public Object getByColumn(String clmNameStr)
        {
            switch(clmNameStr)
            {
                case "ROLE_NAME":
                    return roleName;
                case "ROLE_DSP_NAME":
                    return roleDspName;
                case "DOMAIN_NAME":
                    return domainName;
                case "ROLE_ENABLED":
                    return roleEnabled;
                case "ROLE_PRIVILEGES":
                    return rolePrivileges;
                default:
                    throw new ImplementationError("Unknown database column. Table: SEC_ROLES, Column: " + clmNameStr);
            }
        }

        @JsonIgnore
        @Override
        public final String getLinstorKey()
        {
            return formattedPrimaryKey;
        }

        @Override
        @JsonIgnore
        public DatabaseTable getDatabaseTable()
        {
            return GeneratedDatabaseTables.SEC_ROLES;
        }
    }

    @Version(GenCrdV1_18_2.VERSION)
    @Group(GenCrdV1_18_2.GROUP)
    @Plural("secroles")
    @Singular("secroles")
    public static class SecRoles extends CustomResource<SecRolesSpec, Void> implements LinstorCrd<SecRolesSpec>
    {
        private static final long serialVersionUID = 9082867889840966720L;
        String k8sKey = null;

        @JsonCreator
        public SecRoles()
        {
        }

        public SecRoles(SecRolesSpec spec)
        {
            setMetadata(new ObjectMetaBuilder().withName(deriveKey(spec.getLinstorKey())).build());
            setSpec(spec);
        }

        @Override
        public void setMetadata(ObjectMeta metadataRef)
        {
            super.setMetadata(metadataRef);
            k8sKey = metadataRef.getName();
        }

        @Override
        @JsonIgnore
        public String getLinstorKey()
        {
            return spec.getLinstorKey();
        }

        @Override
        @JsonIgnore
        public String getK8sKey()
        {
            return k8sKey;
        }
    }

    public static SecTypes createSecTypes(
        String typeName,
        String typeDspName,
        boolean typeEnabled
    )
    {
        return new SecTypes(
            new SecTypesSpec(
                typeName,
                typeDspName,
                typeEnabled
            )
        );
    }

    @SuppressWarnings("rawtypes")
    @JsonInclude(Include.NON_NULL)
    public static class SecTypesSpec implements LinstorSpec
    {
        @JsonIgnore private static final long serialVersionUID = -271364694892490003L;
        @JsonIgnore private static final String PK_FORMAT = "%s";

        @JsonIgnore private final String formattedPrimaryKey;

        @JsonProperty("type_name") public final String typeName; // PK
        @JsonProperty("type_dsp_name") public final String typeDspName;
        @JsonProperty("type_enabled") public final boolean typeEnabled;

        @JsonCreator
        public SecTypesSpec(
            @JsonProperty("type_name") String typeNameRef,
            @JsonProperty("type_dsp_name") String typeDspNameRef,
            @JsonProperty("type_enabled") boolean typeEnabledRef
        )
        {
            typeName = typeNameRef;
            typeDspName = typeDspNameRef;
            typeEnabled = typeEnabledRef;

            formattedPrimaryKey = String.format(
                SecTypesSpec.PK_FORMAT,
                typeName
            );
        }

        @JsonIgnore
        @Override
        @SuppressWarnings("rawtypes")
        public LinstorCrd getCrd()
        {
            throw new ImplementationError("Pre 1_19_1 does not support this method");
        }

        @JsonIgnore
        @Override
        public Map<String, Object> asRawParameters()
        {
            Map<String, Object> ret = new TreeMap<>();
            ret.put("TYPE_NAME", typeName);
            ret.put("TYPE_DSP_NAME", typeDspName);
            ret.put("TYPE_ENABLED", typeEnabled);
            return ret;
        }

        @JsonIgnore
        @Override
        public Object getByColumn(String clmNameStr)
        {
            switch(clmNameStr)
            {
                case "TYPE_NAME":
                    return typeName;
                case "TYPE_DSP_NAME":
                    return typeDspName;
                case "TYPE_ENABLED":
                    return typeEnabled;
                default:
                    throw new ImplementationError("Unknown database column. Table: SEC_TYPES, Column: " + clmNameStr);
            }
        }

        @JsonIgnore
        @Override
        public final String getLinstorKey()
        {
            return formattedPrimaryKey;
        }

        @Override
        @JsonIgnore
        public DatabaseTable getDatabaseTable()
        {
            return GeneratedDatabaseTables.SEC_TYPES;
        }
    }

    @Version(GenCrdV1_18_2.VERSION)
    @Group(GenCrdV1_18_2.GROUP)
    @Plural("sectypes")
    @Singular("sectypes")
    public static class SecTypes extends CustomResource<SecTypesSpec, Void> implements LinstorCrd<SecTypesSpec>
    {
        private static final long serialVersionUID = -6187403781049742752L;
        String k8sKey = null;

        @JsonCreator
        public SecTypes()
        {
        }

        public SecTypes(SecTypesSpec spec)
        {
            setMetadata(new ObjectMetaBuilder().withName(deriveKey(spec.getLinstorKey())).build());
            setSpec(spec);
        }

        @Override
        public void setMetadata(ObjectMeta metadataRef)
        {
            super.setMetadata(metadataRef);
            k8sKey = metadataRef.getName();
        }

        @Override
        @JsonIgnore
        public String getLinstorKey()
        {
            return spec.getLinstorKey();
        }

        @Override
        @JsonIgnore
        public String getK8sKey()
        {
            return k8sKey;
        }
    }

    public static SecTypeRules createSecTypeRules(
        String domainName,
        String typeName,
        short accessType
    )
    {
        return new SecTypeRules(
            new SecTypeRulesSpec(
                domainName,
                typeName,
                accessType
            )
        );
    }

    @SuppressWarnings("rawtypes")
    @JsonInclude(Include.NON_NULL)
    public static class SecTypeRulesSpec implements LinstorSpec
    {
        @JsonIgnore private static final long serialVersionUID = 6915260146547341532L;
        @JsonIgnore private static final String PK_FORMAT = "%s:%s";

        @JsonIgnore private final String formattedPrimaryKey;

        @JsonProperty("domain_name") public final String domainName; // PK
        @JsonProperty("type_name") public final String typeName; // PK
        @JsonProperty("access_type") public final short accessType;

        @JsonCreator
        public SecTypeRulesSpec(
            @JsonProperty("domain_name") String domainNameRef,
            @JsonProperty("type_name") String typeNameRef,
            @JsonProperty("access_type") short accessTypeRef
        )
        {
            domainName = domainNameRef;
            typeName = typeNameRef;
            accessType = accessTypeRef;

            formattedPrimaryKey = String.format(
                SecTypeRulesSpec.PK_FORMAT,
                domainName,
                typeName
            );
        }

        @JsonIgnore
        @Override
        @SuppressWarnings("rawtypes")
        public LinstorCrd getCrd()
        {
            throw new ImplementationError("Pre 1_19_1 does not support this method");
        }

        @JsonIgnore
        @Override
        public Map<String, Object> asRawParameters()
        {
            Map<String, Object> ret = new TreeMap<>();
            ret.put("DOMAIN_NAME", domainName);
            ret.put("TYPE_NAME", typeName);
            ret.put("ACCESS_TYPE", accessType);
            return ret;
        }

        @JsonIgnore
        @Override
        public Object getByColumn(String clmNameStr)
        {
            switch(clmNameStr)
            {
                case "DOMAIN_NAME":
                    return domainName;
                case "TYPE_NAME":
                    return typeName;
                case "ACCESS_TYPE":
                    return accessType;
                default:
                    throw new ImplementationError("Unknown database column. Table: SEC_TYPE_RULES, Column: " + clmNameStr);
            }
        }

        @JsonIgnore
        @Override
        public final String getLinstorKey()
        {
            return formattedPrimaryKey;
        }

        @Override
        @JsonIgnore
        public DatabaseTable getDatabaseTable()
        {
            return GeneratedDatabaseTables.SEC_TYPE_RULES;
        }
    }

    @Version(GenCrdV1_18_2.VERSION)
    @Group(GenCrdV1_18_2.GROUP)
    @Plural("sectyperules")
    @Singular("sectyperules")
    public static class SecTypeRules extends CustomResource<SecTypeRulesSpec, Void> implements LinstorCrd<SecTypeRulesSpec>
    {
        private static final long serialVersionUID = -9163481527212408292L;
        String k8sKey = null;

        @JsonCreator
        public SecTypeRules()
        {
        }

        public SecTypeRules(SecTypeRulesSpec spec)
        {
            setMetadata(new ObjectMetaBuilder().withName(deriveKey(spec.getLinstorKey())).build());
            setSpec(spec);
        }

        @Override
        public void setMetadata(ObjectMeta metadataRef)
        {
            super.setMetadata(metadataRef);
            k8sKey = metadataRef.getName();
        }

        @Override
        @JsonIgnore
        public String getLinstorKey()
        {
            return spec.getLinstorKey();
        }

        @Override
        @JsonIgnore
        public String getK8sKey()
        {
            return k8sKey;
        }
    }

    public static SpaceHistory createSpaceHistory(
        Date entryDate,
        byte[] capacity
    )
    {
        return new SpaceHistory(
            new SpaceHistorySpec(
                entryDate,
                capacity
            )
        );
    }

    @SuppressWarnings("rawtypes")
    @JsonInclude(Include.NON_NULL)
    public static class SpaceHistorySpec implements LinstorSpec
    {
        @JsonIgnore private static final long serialVersionUID = -1253323830381737376L;
        @JsonIgnore private static final String PK_FORMAT = "%s";

        @JsonIgnore private final String formattedPrimaryKey;

        @JsonProperty("entry_date") public final Date entryDate; // PK
        @JsonProperty("capacity") public final byte[] capacity;

        @JsonCreator
        public SpaceHistorySpec(
            @JsonProperty("entry_date") Date entryDateRef,
            @JsonProperty("capacity") byte[] capacityRef
        )
        {
            entryDate = entryDateRef;
            capacity = capacityRef;

            formattedPrimaryKey = String.format(
                SpaceHistorySpec.PK_FORMAT,
                RFC3339.format(TimeUtils.millisToDate(entryDate.getTime()))
            );
        }

        @JsonIgnore
        @Override
        @SuppressWarnings("rawtypes")
        public LinstorCrd getCrd()
        {
            throw new ImplementationError("Pre 1_19_1 does not support this method");
        }

        @JsonIgnore
        @Override
        public Map<String, Object> asRawParameters()
        {
            Map<String, Object> ret = new TreeMap<>();
            ret.put("ENTRY_DATE", entryDate);
            ret.put("CAPACITY", capacity);
            return ret;
        }

        @JsonIgnore
        @Override
        public Object getByColumn(String clmNameStr)
        {
            switch(clmNameStr)
            {
                case "ENTRY_DATE":
                    return entryDate;
                case "CAPACITY":
                    return capacity;
                default:
                    throw new ImplementationError("Unknown database column. Table: SPACE_HISTORY, Column: " + clmNameStr);
            }
        }

        @JsonIgnore
        @Override
        public final String getLinstorKey()
        {
            return formattedPrimaryKey;
        }

        @Override
        @JsonIgnore
        public DatabaseTable getDatabaseTable()
        {
            return GeneratedDatabaseTables.SPACE_HISTORY;
        }
    }

    @Version(GenCrdV1_18_2.VERSION)
    @Group(GenCrdV1_18_2.GROUP)
    @Plural("spacehistory")
    @Singular("spacehistory")
    public static class SpaceHistory extends CustomResource<SpaceHistorySpec, Void> implements LinstorCrd<SpaceHistorySpec>
    {
        private static final long serialVersionUID = -6673643108319334681L;
        String k8sKey = null;

        @JsonCreator
        public SpaceHistory()
        {
        }

        public SpaceHistory(SpaceHistorySpec spec)
        {
            setMetadata(new ObjectMetaBuilder().withName(deriveKey(spec.getLinstorKey())).build());
            setSpec(spec);
        }

        @Override
        public void setMetadata(ObjectMeta metadataRef)
        {
            super.setMetadata(metadataRef);
            k8sKey = metadataRef.getName();
        }

        @Override
        @JsonIgnore
        public String getLinstorKey()
        {
            return spec.getLinstorKey();
        }

        @Override
        @JsonIgnore
        public String getK8sKey()
        {
            return k8sKey;
        }
    }

    public static StorPoolDefinitions createStorPoolDefinitions(
        String uuid,
        String poolName,
        String poolDspName
    )
    {
        return new StorPoolDefinitions(
            new StorPoolDefinitionsSpec(
                uuid,
                poolName,
                poolDspName
            )
        );
    }

    @SuppressWarnings("rawtypes")
    @JsonInclude(Include.NON_NULL)
    public static class StorPoolDefinitionsSpec implements LinstorSpec
    {
        @JsonIgnore private static final long serialVersionUID = 8071823275969066695L;
        @JsonIgnore private static final String PK_FORMAT = "%s";

        @JsonIgnore private final String formattedPrimaryKey;

        @JsonProperty("uuid") public final String uuid;
        @JsonProperty("pool_name") public final String poolName; // PK
        @JsonProperty("pool_dsp_name") public final String poolDspName;

        @JsonCreator
        public StorPoolDefinitionsSpec(
            @JsonProperty("uuid") String uuidRef,
            @JsonProperty("pool_name") String poolNameRef,
            @JsonProperty("pool_dsp_name") String poolDspNameRef
        )
        {
            uuid = uuidRef;
            poolName = poolNameRef;
            poolDspName = poolDspNameRef;

            formattedPrimaryKey = String.format(
                StorPoolDefinitionsSpec.PK_FORMAT,
                poolName
            );
        }

        @JsonIgnore
        @Override
        @SuppressWarnings("rawtypes")
        public LinstorCrd getCrd()
        {
            throw new ImplementationError("Pre 1_19_1 does not support this method");
        }

        @JsonIgnore
        @Override
        public Map<String, Object> asRawParameters()
        {
            Map<String, Object> ret = new TreeMap<>();
            ret.put("UUID", uuid);
            ret.put("POOL_NAME", poolName);
            ret.put("POOL_DSP_NAME", poolDspName);
            return ret;
        }

        @JsonIgnore
        @Override
        public Object getByColumn(String clmNameStr)
        {
            switch(clmNameStr)
            {
                case "UUID":
                    return uuid;
                case "POOL_NAME":
                    return poolName;
                case "POOL_DSP_NAME":
                    return poolDspName;
                default:
                    throw new ImplementationError("Unknown database column. Table: STOR_POOL_DEFINITIONS, Column: " + clmNameStr);
            }
        }

        @JsonIgnore
        @Override
        public final String getLinstorKey()
        {
            return formattedPrimaryKey;
        }

        @Override
        @JsonIgnore
        public DatabaseTable getDatabaseTable()
        {
            return GeneratedDatabaseTables.STOR_POOL_DEFINITIONS;
        }
    }

    @Version(GenCrdV1_18_2.VERSION)
    @Group(GenCrdV1_18_2.GROUP)
    @Plural("storpooldefinitions")
    @Singular("storpooldefinitions")
    public static class StorPoolDefinitions extends CustomResource<StorPoolDefinitionsSpec, Void> implements LinstorCrd<StorPoolDefinitionsSpec>
    {
        private static final long serialVersionUID = -553182171915556214L;
        String k8sKey = null;

        @JsonCreator
        public StorPoolDefinitions()
        {
        }

        public StorPoolDefinitions(StorPoolDefinitionsSpec spec)
        {
            setMetadata(new ObjectMetaBuilder().withName(deriveKey(spec.getLinstorKey())).build());
            setSpec(spec);
        }

        @Override
        public void setMetadata(ObjectMeta metadataRef)
        {
            super.setMetadata(metadataRef);
            k8sKey = metadataRef.getName();
        }

        @Override
        @JsonIgnore
        public String getLinstorKey()
        {
            return spec.getLinstorKey();
        }

        @Override
        @JsonIgnore
        public String getK8sKey()
        {
            return k8sKey;
        }
    }

    public static TrackingDate createTrackingDate(
        Date entryDate
    )
    {
        return new TrackingDate(
            new TrackingDateSpec(
                entryDate
            )
        );
    }

    @SuppressWarnings("rawtypes")
    @JsonInclude(Include.NON_NULL)
    public static class TrackingDateSpec implements LinstorSpec
    {
        @JsonIgnore private static final long serialVersionUID = -4019742079139570177L;
        @JsonIgnore private static final String PK_FORMAT = "%s";

        @JsonIgnore private final String formattedPrimaryKey;

        // No PK found. Combining ALL columns for K8s key
        @JsonProperty("entry_date") public final Date entryDate;

        @JsonCreator
        public TrackingDateSpec(
            @JsonProperty("entry_date") Date entryDateRef
        )
        {
            entryDate = entryDateRef;

            formattedPrimaryKey = String.format(
                TrackingDateSpec.PK_FORMAT,
                RFC3339.format(TimeUtils.millisToDate(entryDate.getTime()))
            );
        }

        @JsonIgnore
        @Override
        @SuppressWarnings("rawtypes")
        public LinstorCrd getCrd()
        {
            throw new ImplementationError("Pre 1_19_1 does not support this method");
        }

        @JsonIgnore
        @Override
        public Map<String, Object> asRawParameters()
        {
            Map<String, Object> ret = new TreeMap<>();
            ret.put("ENTRY_DATE", entryDate);
            return ret;
        }

        @JsonIgnore
        @Override
        public Object getByColumn(String clmNameStr)
        {
            switch(clmNameStr)
            {
                case "ENTRY_DATE":
                    return entryDate;
                default:
                    throw new ImplementationError("Unknown database column. Table: TRACKING_DATE, Column: " + clmNameStr);
            }
        }

        @JsonIgnore
        @Override
        public final String getLinstorKey()
        {
            return formattedPrimaryKey;
        }

        @Override
        @JsonIgnore
        public DatabaseTable getDatabaseTable()
        {
            return GeneratedDatabaseTables.TRACKING_DATE;
        }
    }

    @Version(GenCrdV1_18_2.VERSION)
    @Group(GenCrdV1_18_2.GROUP)
    @Plural("trackingdate")
    @Singular("trackingdate")
    public static class TrackingDate extends CustomResource<TrackingDateSpec, Void> implements LinstorCrd<TrackingDateSpec>
    {
        private static final long serialVersionUID = -8997723126837692653L;
        String k8sKey = null;

        @JsonCreator
        public TrackingDate()
        {
        }

        public TrackingDate(TrackingDateSpec spec)
        {
            setMetadata(new ObjectMetaBuilder().withName(deriveKey(spec.getLinstorKey())).build());
            setSpec(spec);
        }

        @Override
        public void setMetadata(ObjectMeta metadataRef)
        {
            super.setMetadata(metadataRef);
            k8sKey = metadataRef.getName();
        }

        @Override
        @JsonIgnore
        public String getLinstorKey()
        {
            return spec.getLinstorKey();
        }

        @Override
        @JsonIgnore
        public String getK8sKey()
        {
            return k8sKey;
        }
    }

    public static Volumes createVolumes(
        String uuid,
        String nodeName,
        String resourceName,
        String snapshotName,
        int vlmNr,
        long vlmFlags
    )
    {
        return new Volumes(
            new VolumesSpec(
                uuid,
                nodeName,
                resourceName,
                snapshotName,
                vlmNr,
                vlmFlags
            )
        );
    }

    @SuppressWarnings("rawtypes")
    @JsonInclude(Include.NON_NULL)
    public static class VolumesSpec implements LinstorSpec
    {
        @JsonIgnore private static final long serialVersionUID = 6293692869471530421L;
        @JsonIgnore private static final String PK_FORMAT = "%s:%s:%s:%d";

        @JsonIgnore private final String formattedPrimaryKey;

        @JsonProperty("uuid") public final String uuid;
        @JsonProperty("node_name") public final String nodeName; // PK
        @JsonProperty("resource_name") public final String resourceName; // PK
        @JsonProperty("snapshot_name") public final String snapshotName; // PK
        @JsonProperty("vlm_nr") public final int vlmNr; // PK
        @JsonProperty("vlm_flags") public final long vlmFlags;

        @JsonCreator
        public VolumesSpec(
            @JsonProperty("uuid") String uuidRef,
            @JsonProperty("node_name") String nodeNameRef,
            @JsonProperty("resource_name") String resourceNameRef,
            @JsonProperty("snapshot_name") String snapshotNameRef,
            @JsonProperty("vlm_nr") int vlmNrRef,
            @JsonProperty("vlm_flags") long vlmFlagsRef
        )
        {
            uuid = uuidRef;
            nodeName = nodeNameRef;
            resourceName = resourceNameRef;
            snapshotName = snapshotNameRef;
            vlmNr = vlmNrRef;
            vlmFlags = vlmFlagsRef;

            formattedPrimaryKey = String.format(
                VolumesSpec.PK_FORMAT,
                nodeName,
                resourceName,
                snapshotName,
                vlmNr
            );
        }

        @JsonIgnore
        @Override
        @SuppressWarnings("rawtypes")
        public LinstorCrd getCrd()
        {
            throw new ImplementationError("Pre 1_19_1 does not support this method");
        }

        @JsonIgnore
        @Override
        public Map<String, Object> asRawParameters()
        {
            Map<String, Object> ret = new TreeMap<>();
            ret.put("UUID", uuid);
            ret.put("NODE_NAME", nodeName);
            ret.put("RESOURCE_NAME", resourceName);
            ret.put("SNAPSHOT_NAME", snapshotName);
            ret.put("VLM_NR", vlmNr);
            ret.put("VLM_FLAGS", vlmFlags);
            return ret;
        }

        @JsonIgnore
        @Override
        public Object getByColumn(String clmNameStr)
        {
            switch(clmNameStr)
            {
                case "UUID":
                    return uuid;
                case "NODE_NAME":
                    return nodeName;
                case "RESOURCE_NAME":
                    return resourceName;
                case "SNAPSHOT_NAME":
                    return snapshotName;
                case "VLM_NR":
                    return vlmNr;
                case "VLM_FLAGS":
                    return vlmFlags;
                default:
                    throw new ImplementationError("Unknown database column. Table: VOLUMES, Column: " + clmNameStr);
            }
        }

        @JsonIgnore
        @Override
        public final String getLinstorKey()
        {
            return formattedPrimaryKey;
        }

        @Override
        @JsonIgnore
        public DatabaseTable getDatabaseTable()
        {
            return GeneratedDatabaseTables.VOLUMES;
        }
    }

    @Version(GenCrdV1_18_2.VERSION)
    @Group(GenCrdV1_18_2.GROUP)
    @Plural("volumes")
    @Singular("volumes")
    public static class Volumes extends CustomResource<VolumesSpec, Void> implements LinstorCrd<VolumesSpec>
    {
        private static final long serialVersionUID = -1448320073893530610L;
        String k8sKey = null;

        @JsonCreator
        public Volumes()
        {
        }

        public Volumes(VolumesSpec spec)
        {
            setMetadata(new ObjectMetaBuilder().withName(deriveKey(spec.getLinstorKey())).build());
            setSpec(spec);
        }

        @Override
        public void setMetadata(ObjectMeta metadataRef)
        {
            super.setMetadata(metadataRef);
            k8sKey = metadataRef.getName();
        }

        @Override
        @JsonIgnore
        public String getLinstorKey()
        {
            return spec.getLinstorKey();
        }

        @Override
        @JsonIgnore
        public String getK8sKey()
        {
            return k8sKey;
        }
    }

    public static VolumeConnections createVolumeConnections(
        String uuid,
        String nodeNameSrc,
        String nodeNameDst,
        String resourceName,
        String snapshotName,
        int vlmNr
    )
    {
        return new VolumeConnections(
            new VolumeConnectionsSpec(
                uuid,
                nodeNameSrc,
                nodeNameDst,
                resourceName,
                snapshotName,
                vlmNr
            )
        );
    }

    @SuppressWarnings("rawtypes")
    @JsonInclude(Include.NON_NULL)
    public static class VolumeConnectionsSpec implements LinstorSpec
    {
        @JsonIgnore private static final long serialVersionUID = -9186018992064054370L;
        @JsonIgnore private static final String PK_FORMAT = "%s:%s:%s:%s:%d";

        @JsonIgnore private final String formattedPrimaryKey;

        @JsonProperty("uuid") public final String uuid;
        @JsonProperty("node_name_src") public final String nodeNameSrc; // PK
        @JsonProperty("node_name_dst") public final String nodeNameDst; // PK
        @JsonProperty("resource_name") public final String resourceName; // PK
        @JsonProperty("snapshot_name") public final String snapshotName; // PK
        @JsonProperty("vlm_nr") public final int vlmNr; // PK

        @JsonCreator
        public VolumeConnectionsSpec(
            @JsonProperty("uuid") String uuidRef,
            @JsonProperty("node_name_src") String nodeNameSrcRef,
            @JsonProperty("node_name_dst") String nodeNameDstRef,
            @JsonProperty("resource_name") String resourceNameRef,
            @JsonProperty("snapshot_name") String snapshotNameRef,
            @JsonProperty("vlm_nr") int vlmNrRef
        )
        {
            uuid = uuidRef;
            nodeNameSrc = nodeNameSrcRef;
            nodeNameDst = nodeNameDstRef;
            resourceName = resourceNameRef;
            snapshotName = snapshotNameRef;
            vlmNr = vlmNrRef;

            formattedPrimaryKey = String.format(
                VolumeConnectionsSpec.PK_FORMAT,
                nodeNameSrc,
                nodeNameDst,
                resourceName,
                snapshotName,
                vlmNr
            );
        }

        @JsonIgnore
        @Override
        @SuppressWarnings("rawtypes")
        public LinstorCrd getCrd()
        {
            throw new ImplementationError("Pre 1_19_1 does not support this method");
        }

        @JsonIgnore
        @Override
        public Map<String, Object> asRawParameters()
        {
            Map<String, Object> ret = new TreeMap<>();
            ret.put("UUID", uuid);
            ret.put("NODE_NAME_SRC", nodeNameSrc);
            ret.put("NODE_NAME_DST", nodeNameDst);
            ret.put("RESOURCE_NAME", resourceName);
            ret.put("SNAPSHOT_NAME", snapshotName);
            ret.put("VLM_NR", vlmNr);
            return ret;
        }

        @JsonIgnore
        @Override
        public Object getByColumn(String clmNameStr)
        {
            switch(clmNameStr)
            {
                case "UUID":
                    return uuid;
                case "NODE_NAME_SRC":
                    return nodeNameSrc;
                case "NODE_NAME_DST":
                    return nodeNameDst;
                case "RESOURCE_NAME":
                    return resourceName;
                case "SNAPSHOT_NAME":
                    return snapshotName;
                case "VLM_NR":
                    return vlmNr;
                default:
                    throw new ImplementationError("Unknown database column. Table: VOLUME_CONNECTIONS, Column: " + clmNameStr);
            }
        }

        @JsonIgnore
        @Override
        public final String getLinstorKey()
        {
            return formattedPrimaryKey;
        }

        @Override
        @JsonIgnore
        public DatabaseTable getDatabaseTable()
        {
            return GeneratedDatabaseTables.VOLUME_CONNECTIONS;
        }
    }

    @Version(GenCrdV1_18_2.VERSION)
    @Group(GenCrdV1_18_2.GROUP)
    @Plural("volumeconnections")
    @Singular("volumeconnections")
    public static class VolumeConnections extends CustomResource<VolumeConnectionsSpec, Void> implements LinstorCrd<VolumeConnectionsSpec>
    {
        private static final long serialVersionUID = 1372386788981404402L;
        String k8sKey = null;

        @JsonCreator
        public VolumeConnections()
        {
        }

        public VolumeConnections(VolumeConnectionsSpec spec)
        {
            setMetadata(new ObjectMetaBuilder().withName(deriveKey(spec.getLinstorKey())).build());
            setSpec(spec);
        }

        @Override
        public void setMetadata(ObjectMeta metadataRef)
        {
            super.setMetadata(metadataRef);
            k8sKey = metadataRef.getName();
        }

        @Override
        @JsonIgnore
        public String getLinstorKey()
        {
            return spec.getLinstorKey();
        }

        @Override
        @JsonIgnore
        public String getK8sKey()
        {
            return k8sKey;
        }
    }

    public static VolumeDefinitions createVolumeDefinitions(
        String uuid,
        String resourceName,
        String snapshotName,
        int vlmNr,
        long vlmSize,
        long vlmFlags
    )
    {
        return new VolumeDefinitions(
            new VolumeDefinitionsSpec(
                uuid,
                resourceName,
                snapshotName,
                vlmNr,
                vlmSize,
                vlmFlags
            )
        );
    }

    @SuppressWarnings("rawtypes")
    @JsonInclude(Include.NON_NULL)
    public static class VolumeDefinitionsSpec implements LinstorSpec
    {
        @JsonIgnore private static final long serialVersionUID = 7757464692257845523L;
        @JsonIgnore private static final String PK_FORMAT = "%s:%s:%d";

        @JsonIgnore private final String formattedPrimaryKey;

        @JsonProperty("uuid") public final String uuid;
        @JsonProperty("resource_name") public final String resourceName; // PK
        @JsonProperty("snapshot_name") public final String snapshotName; // PK
        @JsonProperty("vlm_nr") public final int vlmNr; // PK
        @JsonProperty("vlm_size") public final long vlmSize;
        @JsonProperty("vlm_flags") public final long vlmFlags;

        @JsonCreator
        public VolumeDefinitionsSpec(
            @JsonProperty("uuid") String uuidRef,
            @JsonProperty("resource_name") String resourceNameRef,
            @JsonProperty("snapshot_name") String snapshotNameRef,
            @JsonProperty("vlm_nr") int vlmNrRef,
            @JsonProperty("vlm_size") long vlmSizeRef,
            @JsonProperty("vlm_flags") long vlmFlagsRef
        )
        {
            uuid = uuidRef;
            resourceName = resourceNameRef;
            snapshotName = snapshotNameRef;
            vlmNr = vlmNrRef;
            vlmSize = vlmSizeRef;
            vlmFlags = vlmFlagsRef;

            formattedPrimaryKey = String.format(
                VolumeDefinitionsSpec.PK_FORMAT,
                resourceName,
                snapshotName,
                vlmNr
            );
        }

        @JsonIgnore
        @Override
        @SuppressWarnings("rawtypes")
        public LinstorCrd getCrd()
        {
            throw new ImplementationError("Pre 1_19_1 does not support this method");
        }

        @JsonIgnore
        @Override
        public Map<String, Object> asRawParameters()
        {
            Map<String, Object> ret = new TreeMap<>();
            ret.put("UUID", uuid);
            ret.put("RESOURCE_NAME", resourceName);
            ret.put("SNAPSHOT_NAME", snapshotName);
            ret.put("VLM_NR", vlmNr);
            ret.put("VLM_SIZE", vlmSize);
            ret.put("VLM_FLAGS", vlmFlags);
            return ret;
        }

        @JsonIgnore
        @Override
        public Object getByColumn(String clmNameStr)
        {
            switch(clmNameStr)
            {
                case "UUID":
                    return uuid;
                case "RESOURCE_NAME":
                    return resourceName;
                case "SNAPSHOT_NAME":
                    return snapshotName;
                case "VLM_NR":
                    return vlmNr;
                case "VLM_SIZE":
                    return vlmSize;
                case "VLM_FLAGS":
                    return vlmFlags;
                default:
                    throw new ImplementationError("Unknown database column. Table: VOLUME_DEFINITIONS, Column: " + clmNameStr);
            }
        }

        @JsonIgnore
        @Override
        public final String getLinstorKey()
        {
            return formattedPrimaryKey;
        }

        @Override
        @JsonIgnore
        public DatabaseTable getDatabaseTable()
        {
            return GeneratedDatabaseTables.VOLUME_DEFINITIONS;
        }
    }

    @Version(GenCrdV1_18_2.VERSION)
    @Group(GenCrdV1_18_2.GROUP)
    @Plural("volumedefinitions")
    @Singular("volumedefinitions")
    public static class VolumeDefinitions extends CustomResource<VolumeDefinitionsSpec, Void> implements LinstorCrd<VolumeDefinitionsSpec>
    {
        private static final long serialVersionUID = 3690004998949281502L;
        String k8sKey = null;

        @JsonCreator
        public VolumeDefinitions()
        {
        }

        public VolumeDefinitions(VolumeDefinitionsSpec spec)
        {
            setMetadata(new ObjectMetaBuilder().withName(deriveKey(spec.getLinstorKey())).build());
            setSpec(spec);
        }

        @Override
        public void setMetadata(ObjectMeta metadataRef)
        {
            super.setMetadata(metadataRef);
            k8sKey = metadataRef.getName();
        }

        @Override
        @JsonIgnore
        public String getLinstorKey()
        {
            return spec.getLinstorKey();
        }

        @Override
        @JsonIgnore
        public String getK8sKey()
        {
            return k8sKey;
        }
    }

    public static VolumeGroups createVolumeGroups(
        String uuid,
        String resourceGroupName,
        int vlmNr,
        long flags
    )
    {
        return new VolumeGroups(
            new VolumeGroupsSpec(
                uuid,
                resourceGroupName,
                vlmNr,
                flags
            )
        );
    }

    @SuppressWarnings("rawtypes")
    @JsonInclude(Include.NON_NULL)
    public static class VolumeGroupsSpec implements LinstorSpec
    {
        @JsonIgnore private static final long serialVersionUID = 169816466813410275L;
        @JsonIgnore private static final String PK_FORMAT = "%s:%d";

        @JsonIgnore private final String formattedPrimaryKey;

        @JsonProperty("uuid") public final String uuid;
        @JsonProperty("resource_group_name") public final String resourceGroupName; // PK
        @JsonProperty("vlm_nr") public final int vlmNr; // PK
        @JsonProperty("flags") public final long flags;

        @JsonCreator
        public VolumeGroupsSpec(
            @JsonProperty("uuid") String uuidRef,
            @JsonProperty("resource_group_name") String resourceGroupNameRef,
            @JsonProperty("vlm_nr") int vlmNrRef,
            @JsonProperty("flags") long flagsRef
        )
        {
            uuid = uuidRef;
            resourceGroupName = resourceGroupNameRef;
            vlmNr = vlmNrRef;
            flags = flagsRef;

            formattedPrimaryKey = String.format(
                VolumeGroupsSpec.PK_FORMAT,
                resourceGroupName,
                vlmNr
            );
        }

        @JsonIgnore
        @Override
        @SuppressWarnings("rawtypes")
        public LinstorCrd getCrd()
        {
            throw new ImplementationError("Pre 1_19_1 does not support this method");
        }

        @JsonIgnore
        @Override
        public Map<String, Object> asRawParameters()
        {
            Map<String, Object> ret = new TreeMap<>();
            ret.put("UUID", uuid);
            ret.put("RESOURCE_GROUP_NAME", resourceGroupName);
            ret.put("VLM_NR", vlmNr);
            ret.put("FLAGS", flags);
            return ret;
        }

        @JsonIgnore
        @Override
        public Object getByColumn(String clmNameStr)
        {
            switch(clmNameStr)
            {
                case "UUID":
                    return uuid;
                case "RESOURCE_GROUP_NAME":
                    return resourceGroupName;
                case "VLM_NR":
                    return vlmNr;
                case "FLAGS":
                    return flags;
                default:
                    throw new ImplementationError("Unknown database column. Table: VOLUME_GROUPS, Column: " + clmNameStr);
            }
        }

        @JsonIgnore
        @Override
        public final String getLinstorKey()
        {
            return formattedPrimaryKey;
        }

        @Override
        @JsonIgnore
        public DatabaseTable getDatabaseTable()
        {
            return GeneratedDatabaseTables.VOLUME_GROUPS;
        }
    }

    @Version(GenCrdV1_18_2.VERSION)
    @Group(GenCrdV1_18_2.GROUP)
    @Plural("volumegroups")
    @Singular("volumegroups")
    public static class VolumeGroups extends CustomResource<VolumeGroupsSpec, Void> implements LinstorCrd<VolumeGroupsSpec>
    {
        private static final long serialVersionUID = -6214679262143632508L;
        String k8sKey = null;

        @JsonCreator
        public VolumeGroups()
        {
        }

        public VolumeGroups(VolumeGroupsSpec spec)
        {
            setMetadata(new ObjectMetaBuilder().withName(deriveKey(spec.getLinstorKey())).build());
            setSpec(spec);
        }

        @Override
        public void setMetadata(ObjectMeta metadataRef)
        {
            super.setMetadata(metadataRef);
            k8sKey = metadataRef.getName();
        }

        @Override
        @JsonIgnore
        public String getLinstorKey()
        {
            return spec.getLinstorKey();
        }

        @Override
        @JsonIgnore
        public String getK8sKey()
        {
            return k8sKey;
        }
    }

    public static final String deriveKey(String formattedPrimaryKey)
    {
        String sha = KEY_LUT.get(formattedPrimaryKey);
        if (sha == null)
        {
            synchronized (KEY_LUT)
            {
                sha = KEY_LUT.get(formattedPrimaryKey);
                if (sha == null)
                {
                    sha = ByteUtils.bytesToHex(ByteUtils.checksumSha256(formattedPrimaryKey.getBytes(StandardCharsets.UTF_8))).toLowerCase();
                    while (!USED_K8S_KEYS.add(sha))
                    {
                        String modifiedPk = formattedPrimaryKey + NEXT_ID.incrementAndGet();
                        sha = ByteUtils.bytesToHex(ByteUtils.checksumSha256(modifiedPk.getBytes(StandardCharsets.UTF_8))).toLowerCase();
                    }
                    KEY_LUT.put(formattedPrimaryKey, sha);
                }
            }
        }
        return sha;
    }

    public static class JsonTypeResolver extends TypeIdResolverBase
    {
        private JavaType baseType;

        @Override
        public void init(JavaType baseTypeRef)
        {
            super.init(baseTypeRef);
            baseType = baseTypeRef;
        }

        @Override
        public String idFromValue(Object valueRef)
        {
            return idFromValueAndType(valueRef, valueRef.getClass());
        }

        @Override
        public String idFromValueAndType(Object ignored, Class<?> suggestedTypeRef)
        {
            return suggestedTypeRef.getSimpleName();
        }

        @Override
        public Id getMechanism()
        {
            return Id.MINIMAL_CLASS;
        }

        @Override
        public JavaType typeFromId(DatabindContext contextRef, String idRef)
        {
            Class<?> typeClass = JSON_ID_TO_TYPE_CLASS_LUT.get(idRef);
            return TypeFactory.defaultInstance().constructSpecializedType(baseType, typeClass);
        }
    }

    @SuppressWarnings("checkstyle:linelength")
    public static class GeneratedDatabaseTables
    {
        private GeneratedDatabaseTables()
        {
        }

        // Schema name
        public static final String DATABASE_SCHEMA_NAME = "LINSTOR";

        public static class Files implements DatabaseTable
        {
            private Files()
            {
            }

            // Primary Key
            public static final ColumnImpl PATH = new ColumnImpl("PATH", Types.VARCHAR, true, false);

            public static final ColumnImpl UUID = new ColumnImpl("UUID", Types.CHAR, false, false);
            public static final ColumnImpl FLAGS = new ColumnImpl("FLAGS", Types.BIGINT, false, false);
            public static final ColumnImpl CONTENT = new ColumnImpl("CONTENT", Types.BLOB, false, false);
            public static final ColumnImpl CONTENT_CHECKSUM = new ColumnImpl(
                "CONTENT_CHECKSUM",
                Types.VARCHAR,
                false,
                false
            );

            public static final Column[] ALL = new Column[] {
                UUID,
                PATH,
                FLAGS,
                CONTENT,
                CONTENT_CHECKSUM
            };

            @Override
            public Column[] values()
            {
                return ALL;
            }

            @Override
            public String getName()
            {
                return "FILES";
            }

            @Override
            public String toString()
            {
                return "Table FILES";
            }
        }

        public static class KeyValueStore implements DatabaseTable
        {
            private KeyValueStore()
            {
            }

            // Primary Key
            public static final ColumnImpl KVS_NAME = new ColumnImpl("KVS_NAME", Types.VARCHAR, true, false);

            public static final ColumnImpl UUID = new ColumnImpl("UUID", Types.CHAR, false, false);
            public static final ColumnImpl KVS_DSP_NAME = new ColumnImpl("KVS_DSP_NAME", Types.VARCHAR, false, false);

            public static final Column[] ALL = new Column[] {
                UUID,
                KVS_NAME,
                KVS_DSP_NAME
            };

            @Override
            public Column[] values()
            {
                return ALL;
            }

            @Override
            public String getName()
            {
                return "KEY_VALUE_STORE";
            }

            @Override
            public String toString()
            {
                return "Table KEY_VALUE_STORE";
            }
        }

        public static class LayerBcacheVolumes implements DatabaseTable
        {
            private LayerBcacheVolumes()
            {
            }

            // Primary Keys
            public static final ColumnImpl LAYER_RESOURCE_ID = new ColumnImpl(
                "LAYER_RESOURCE_ID",
                Types.INTEGER,
                true,
                false
            );
            public static final ColumnImpl VLM_NR = new ColumnImpl("VLM_NR", Types.INTEGER, true, false);

            public static final ColumnImpl NODE_NAME = new ColumnImpl("NODE_NAME", Types.VARCHAR, false, true);
            public static final ColumnImpl POOL_NAME = new ColumnImpl("POOL_NAME", Types.VARCHAR, false, true);
            public static final ColumnImpl DEV_UUID = new ColumnImpl("DEV_UUID", Types.CHAR, false, true);

            public static final Column[] ALL = new Column[] {
                LAYER_RESOURCE_ID,
                VLM_NR,
                NODE_NAME,
                POOL_NAME,
                DEV_UUID
            };

            @Override
            public Column[] values()
            {
                return ALL;
            }

            @Override
            public String getName()
            {
                return "LAYER_BCACHE_VOLUMES";
            }

            @Override
            public String toString()
            {
                return "Table LAYER_BCACHE_VOLUMES";
            }
        }

        public static class LayerCacheVolumes implements DatabaseTable
        {
            private LayerCacheVolumes()
            {
            }

            // Primary Keys
            public static final ColumnImpl LAYER_RESOURCE_ID = new ColumnImpl(
                "LAYER_RESOURCE_ID",
                Types.INTEGER,
                true,
                false
            );
            public static final ColumnImpl VLM_NR = new ColumnImpl("VLM_NR", Types.INTEGER, true, false);

            public static final ColumnImpl NODE_NAME = new ColumnImpl("NODE_NAME", Types.VARCHAR, false, true);
            public static final ColumnImpl POOL_NAME_CACHE = new ColumnImpl(
                "POOL_NAME_CACHE",
                Types.VARCHAR,
                false,
                true
            );
            public static final ColumnImpl POOL_NAME_META = new ColumnImpl(
                "POOL_NAME_META",
                Types.VARCHAR,
                false,
                true
            );

            public static final Column[] ALL = new Column[] {
                LAYER_RESOURCE_ID,
                VLM_NR,
                NODE_NAME,
                POOL_NAME_CACHE,
                POOL_NAME_META
            };

            @Override
            public Column[] values()
            {
                return ALL;
            }

            @Override
            public String getName()
            {
                return "LAYER_CACHE_VOLUMES";
            }

            @Override
            public String toString()
            {
                return "Table LAYER_CACHE_VOLUMES";
            }
        }

        public static class LayerDrbdResources implements DatabaseTable
        {
            private LayerDrbdResources()
            {
            }

            // Primary Key
            public static final ColumnImpl LAYER_RESOURCE_ID = new ColumnImpl(
                "LAYER_RESOURCE_ID",
                Types.INTEGER,
                true,
                false
            );

            public static final ColumnImpl PEER_SLOTS = new ColumnImpl("PEER_SLOTS", Types.INTEGER, false, false);
            public static final ColumnImpl AL_STRIPES = new ColumnImpl("AL_STRIPES", Types.INTEGER, false, false);
            public static final ColumnImpl AL_STRIPE_SIZE = new ColumnImpl(
                "AL_STRIPE_SIZE",
                Types.BIGINT,
                false,
                false
            );
            public static final ColumnImpl FLAGS = new ColumnImpl("FLAGS", Types.BIGINT, false, false);
            public static final ColumnImpl NODE_ID = new ColumnImpl("NODE_ID", Types.INTEGER, false, false);

            public static final Column[] ALL = new Column[] {
                LAYER_RESOURCE_ID,
                PEER_SLOTS,
                AL_STRIPES,
                AL_STRIPE_SIZE,
                FLAGS,
                NODE_ID
            };

            @Override
            public Column[] values()
            {
                return ALL;
            }

            @Override
            public String getName()
            {
                return "LAYER_DRBD_RESOURCES";
            }

            @Override
            public String toString()
            {
                return "Table LAYER_DRBD_RESOURCES";
            }
        }

        public static class LayerDrbdResourceDefinitions implements DatabaseTable
        {
            private LayerDrbdResourceDefinitions()
            {
            }

            // Primary Keys
            public static final ColumnImpl RESOURCE_NAME = new ColumnImpl("RESOURCE_NAME", Types.VARCHAR, true, false);
            public static final ColumnImpl RESOURCE_NAME_SUFFIX = new ColumnImpl(
                "RESOURCE_NAME_SUFFIX",
                Types.VARCHAR,
                true,
                false
            );
            public static final ColumnImpl SNAPSHOT_NAME = new ColumnImpl("SNAPSHOT_NAME", Types.VARCHAR, true, false);

            public static final ColumnImpl PEER_SLOTS = new ColumnImpl("PEER_SLOTS", Types.INTEGER, false, false);
            public static final ColumnImpl AL_STRIPES = new ColumnImpl("AL_STRIPES", Types.INTEGER, false, false);
            public static final ColumnImpl AL_STRIPE_SIZE = new ColumnImpl(
                "AL_STRIPE_SIZE",
                Types.BIGINT,
                false,
                false
            );
            public static final ColumnImpl TCP_PORT = new ColumnImpl("TCP_PORT", Types.INTEGER, false, true);
            public static final ColumnImpl TRANSPORT_TYPE = new ColumnImpl(
                "TRANSPORT_TYPE",
                Types.VARCHAR,
                false,
                false
            );
            public static final ColumnImpl SECRET = new ColumnImpl("SECRET", Types.VARCHAR, false, true);

            public static final Column[] ALL = new Column[] {
                RESOURCE_NAME,
                RESOURCE_NAME_SUFFIX,
                SNAPSHOT_NAME,
                PEER_SLOTS,
                AL_STRIPES,
                AL_STRIPE_SIZE,
                TCP_PORT,
                TRANSPORT_TYPE,
                SECRET
            };

            @Override
            public Column[] values()
            {
                return ALL;
            }

            @Override
            public String getName()
            {
                return "LAYER_DRBD_RESOURCE_DEFINITIONS";
            }

            @Override
            public String toString()
            {
                return "Table LAYER_DRBD_RESOURCE_DEFINITIONS";
            }
        }

        public static class LayerDrbdVolumes implements DatabaseTable
        {
            private LayerDrbdVolumes()
            {
            }

            // Primary Keys
            public static final ColumnImpl LAYER_RESOURCE_ID = new ColumnImpl(
                "LAYER_RESOURCE_ID",
                Types.INTEGER,
                true,
                false
            );
            public static final ColumnImpl VLM_NR = new ColumnImpl("VLM_NR", Types.INTEGER, true, false);

            public static final ColumnImpl NODE_NAME = new ColumnImpl("NODE_NAME", Types.VARCHAR, false, true);
            public static final ColumnImpl POOL_NAME = new ColumnImpl("POOL_NAME", Types.VARCHAR, false, true);

            public static final Column[] ALL = new Column[] {
                LAYER_RESOURCE_ID,
                VLM_NR,
                NODE_NAME,
                POOL_NAME
            };

            @Override
            public Column[] values()
            {
                return ALL;
            }

            @Override
            public String getName()
            {
                return "LAYER_DRBD_VOLUMES";
            }

            @Override
            public String toString()
            {
                return "Table LAYER_DRBD_VOLUMES";
            }
        }

        public static class LayerDrbdVolumeDefinitions implements DatabaseTable
        {
            private LayerDrbdVolumeDefinitions()
            {
            }

            // Primary Keys
            public static final ColumnImpl RESOURCE_NAME = new ColumnImpl("RESOURCE_NAME", Types.VARCHAR, true, false);
            public static final ColumnImpl RESOURCE_NAME_SUFFIX = new ColumnImpl(
                "RESOURCE_NAME_SUFFIX",
                Types.VARCHAR,
                true,
                false
            );
            public static final ColumnImpl SNAPSHOT_NAME = new ColumnImpl("SNAPSHOT_NAME", Types.VARCHAR, true, false);
            public static final ColumnImpl VLM_NR = new ColumnImpl("VLM_NR", Types.INTEGER, true, false);

            public static final ColumnImpl VLM_MINOR_NR = new ColumnImpl("VLM_MINOR_NR", Types.INTEGER, false, true);

            public static final Column[] ALL = new Column[] {
                RESOURCE_NAME,
                RESOURCE_NAME_SUFFIX,
                SNAPSHOT_NAME,
                VLM_NR,
                VLM_MINOR_NR
            };

            @Override
            public Column[] values()
            {
                return ALL;
            }

            @Override
            public String getName()
            {
                return "LAYER_DRBD_VOLUME_DEFINITIONS";
            }

            @Override
            public String toString()
            {
                return "Table LAYER_DRBD_VOLUME_DEFINITIONS";
            }
        }

        public static class LayerLuksVolumes implements DatabaseTable
        {
            private LayerLuksVolumes()
            {
            }

            // Primary Keys
            public static final ColumnImpl LAYER_RESOURCE_ID = new ColumnImpl(
                "LAYER_RESOURCE_ID",
                Types.INTEGER,
                true,
                false
            );
            public static final ColumnImpl VLM_NR = new ColumnImpl("VLM_NR", Types.INTEGER, true, false);

            public static final ColumnImpl ENCRYPTED_PASSWORD = new ColumnImpl(
                "ENCRYPTED_PASSWORD",
                Types.VARCHAR,
                false,
                false
            );

            public static final Column[] ALL = new Column[] {
                LAYER_RESOURCE_ID,
                VLM_NR,
                ENCRYPTED_PASSWORD
            };

            @Override
            public Column[] values()
            {
                return ALL;
            }

            @Override
            public String getName()
            {
                return "LAYER_LUKS_VOLUMES";
            }

            @Override
            public String toString()
            {
                return "Table LAYER_LUKS_VOLUMES";
            }
        }

        public static class LayerOpenflexResourceDefinitions implements DatabaseTable
        {
            private LayerOpenflexResourceDefinitions()
            {
            }

            // Primary Keys
            public static final ColumnImpl RESOURCE_NAME = new ColumnImpl("RESOURCE_NAME", Types.VARCHAR, true, false);
            public static final ColumnImpl RESOURCE_NAME_SUFFIX = new ColumnImpl(
                "RESOURCE_NAME_SUFFIX",
                Types.VARCHAR,
                true,
                false
            );

            public static final ColumnImpl SNAPSHOT_NAME = new ColumnImpl("SNAPSHOT_NAME", Types.VARCHAR, false, false);
            public static final ColumnImpl NQN = new ColumnImpl("NQN", Types.VARCHAR, false, true);

            public static final Column[] ALL = new Column[] {
                RESOURCE_NAME,
                SNAPSHOT_NAME,
                RESOURCE_NAME_SUFFIX,
                NQN
            };

            @Override
            public Column[] values()
            {
                return ALL;
            }

            @Override
            public String getName()
            {
                return "LAYER_OPENFLEX_RESOURCE_DEFINITIONS";
            }

            @Override
            public String toString()
            {
                return "Table LAYER_OPENFLEX_RESOURCE_DEFINITIONS";
            }
        }

        public static class LayerOpenflexVolumes implements DatabaseTable
        {
            private LayerOpenflexVolumes()
            {
            }

            // Primary Keys
            public static final ColumnImpl LAYER_RESOURCE_ID = new ColumnImpl(
                "LAYER_RESOURCE_ID",
                Types.INTEGER,
                true,
                false
            );
            public static final ColumnImpl VLM_NR = new ColumnImpl("VLM_NR", Types.INTEGER, true, false);

            public static final ColumnImpl NODE_NAME = new ColumnImpl("NODE_NAME", Types.VARCHAR, false, true);
            public static final ColumnImpl POOL_NAME = new ColumnImpl("POOL_NAME", Types.VARCHAR, false, true);

            public static final Column[] ALL = new Column[] {
                LAYER_RESOURCE_ID,
                VLM_NR,
                NODE_NAME,
                POOL_NAME
            };

            @Override
            public Column[] values()
            {
                return ALL;
            }

            @Override
            public String getName()
            {
                return "LAYER_OPENFLEX_VOLUMES";
            }

            @Override
            public String toString()
            {
                return "Table LAYER_OPENFLEX_VOLUMES";
            }
        }

        public static class LayerResourceIds implements DatabaseTable
        {
            private LayerResourceIds()
            {
            }

            // Primary Key
            public static final ColumnImpl LAYER_RESOURCE_ID = new ColumnImpl(
                "LAYER_RESOURCE_ID",
                Types.INTEGER,
                true,
                false
            );

            public static final ColumnImpl NODE_NAME = new ColumnImpl("NODE_NAME", Types.VARCHAR, false, false);
            public static final ColumnImpl RESOURCE_NAME = new ColumnImpl("RESOURCE_NAME", Types.VARCHAR, false, false);
            public static final ColumnImpl SNAPSHOT_NAME = new ColumnImpl("SNAPSHOT_NAME", Types.VARCHAR, false, false);
            public static final ColumnImpl LAYER_RESOURCE_KIND = new ColumnImpl(
                "LAYER_RESOURCE_KIND",
                Types.VARCHAR,
                false,
                false
            );
            public static final ColumnImpl LAYER_RESOURCE_PARENT_ID = new ColumnImpl(
                "LAYER_RESOURCE_PARENT_ID",
                Types.INTEGER,
                false,
                true
            );
            public static final ColumnImpl LAYER_RESOURCE_SUFFIX = new ColumnImpl(
                "LAYER_RESOURCE_SUFFIX",
                Types.VARCHAR,
                false,
                false
            );
            public static final ColumnImpl LAYER_RESOURCE_SUSPENDED = new ColumnImpl(
                "LAYER_RESOURCE_SUSPENDED",
                Types.BOOLEAN,
                false,
                false
            );

            public static final Column[] ALL = new Column[] {
                LAYER_RESOURCE_ID,
                NODE_NAME,
                RESOURCE_NAME,
                SNAPSHOT_NAME,
                LAYER_RESOURCE_KIND,
                LAYER_RESOURCE_PARENT_ID,
                LAYER_RESOURCE_SUFFIX,
                LAYER_RESOURCE_SUSPENDED
            };

            @Override
            public Column[] values()
            {
                return ALL;
            }

            @Override
            public String getName()
            {
                return "LAYER_RESOURCE_IDS";
            }

            @Override
            public String toString()
            {
                return "Table LAYER_RESOURCE_IDS";
            }
        }

        public static class LayerStorageVolumes implements DatabaseTable
        {
            private LayerStorageVolumes()
            {
            }

            // Primary Keys
            public static final ColumnImpl LAYER_RESOURCE_ID = new ColumnImpl(
                "LAYER_RESOURCE_ID",
                Types.INTEGER,
                true,
                false
            );
            public static final ColumnImpl VLM_NR = new ColumnImpl("VLM_NR", Types.INTEGER, true, false);

            public static final ColumnImpl PROVIDER_KIND = new ColumnImpl("PROVIDER_KIND", Types.VARCHAR, false, false);
            public static final ColumnImpl NODE_NAME = new ColumnImpl("NODE_NAME", Types.VARCHAR, false, false);
            public static final ColumnImpl STOR_POOL_NAME = new ColumnImpl(
                "STOR_POOL_NAME",
                Types.VARCHAR,
                false,
                false
            );

            public static final Column[] ALL = new Column[] {
                LAYER_RESOURCE_ID,
                VLM_NR,
                PROVIDER_KIND,
                NODE_NAME,
                STOR_POOL_NAME
            };

            @Override
            public Column[] values()
            {
                return ALL;
            }

            @Override
            public String getName()
            {
                return "LAYER_STORAGE_VOLUMES";
            }

            @Override
            public String toString()
            {
                return "Table LAYER_STORAGE_VOLUMES";
            }
        }

        public static class LayerWritecacheVolumes implements DatabaseTable
        {
            private LayerWritecacheVolumes()
            {
            }

            // Primary Keys
            public static final ColumnImpl LAYER_RESOURCE_ID = new ColumnImpl(
                "LAYER_RESOURCE_ID",
                Types.INTEGER,
                true,
                false
            );
            public static final ColumnImpl VLM_NR = new ColumnImpl("VLM_NR", Types.INTEGER, true, false);

            public static final ColumnImpl NODE_NAME = new ColumnImpl("NODE_NAME", Types.VARCHAR, false, true);
            public static final ColumnImpl POOL_NAME = new ColumnImpl("POOL_NAME", Types.VARCHAR, false, true);

            public static final Column[] ALL = new Column[] {
                LAYER_RESOURCE_ID,
                VLM_NR,
                NODE_NAME,
                POOL_NAME
            };

            @Override
            public Column[] values()
            {
                return ALL;
            }

            @Override
            public String getName()
            {
                return "LAYER_WRITECACHE_VOLUMES";
            }

            @Override
            public String toString()
            {
                return "Table LAYER_WRITECACHE_VOLUMES";
            }
        }

        public static class LinstorRemotes implements DatabaseTable
        {
            private LinstorRemotes()
            {
            }

            // Primary Key
            public static final ColumnImpl NAME = new ColumnImpl("NAME", Types.VARCHAR, true, false);

            public static final ColumnImpl UUID = new ColumnImpl("UUID", Types.CHAR, false, false);
            public static final ColumnImpl DSP_NAME = new ColumnImpl("DSP_NAME", Types.VARCHAR, false, false);
            public static final ColumnImpl FLAGS = new ColumnImpl("FLAGS", Types.BIGINT, false, false);
            public static final ColumnImpl URL = new ColumnImpl("URL", Types.VARCHAR, false, false);
            public static final ColumnImpl ENCRYPTED_PASSPHRASE = new ColumnImpl(
                "ENCRYPTED_PASSPHRASE",
                Types.BLOB,
                false,
                true
            );
            public static final ColumnImpl CLUSTER_ID = new ColumnImpl("CLUSTER_ID", Types.CHAR, false, true);

            public static final Column[] ALL = new Column[] {
                UUID,
                NAME,
                DSP_NAME,
                FLAGS,
                URL,
                ENCRYPTED_PASSPHRASE,
                CLUSTER_ID
            };

            @Override
            public Column[] values()
            {
                return ALL;
            }

            @Override
            public String getName()
            {
                return "LINSTOR_REMOTES";
            }

            @Override
            public String toString()
            {
                return "Table LINSTOR_REMOTES";
            }
        }

        public static class Nodes implements DatabaseTable
        {
            private Nodes()
            {
            }

            // Primary Key
            public static final ColumnImpl NODE_NAME = new ColumnImpl("NODE_NAME", Types.VARCHAR, true, false);

            public static final ColumnImpl UUID = new ColumnImpl("UUID", Types.CHAR, false, false);
            public static final ColumnImpl NODE_DSP_NAME = new ColumnImpl("NODE_DSP_NAME", Types.VARCHAR, false, false);
            public static final ColumnImpl NODE_FLAGS = new ColumnImpl("NODE_FLAGS", Types.BIGINT, false, false);
            public static final ColumnImpl NODE_TYPE = new ColumnImpl("NODE_TYPE", Types.INTEGER, false, false);

            public static final Column[] ALL = new Column[] {
                UUID,
                NODE_NAME,
                NODE_DSP_NAME,
                NODE_FLAGS,
                NODE_TYPE
            };

            @Override
            public Column[] values()
            {
                return ALL;
            }

            @Override
            public String getName()
            {
                return "NODES";
            }

            @Override
            public String toString()
            {
                return "Table NODES";
            }
        }

        public static class NodeConnections implements DatabaseTable
        {
            private NodeConnections()
            {
            }

            // Primary Keys
            public static final ColumnImpl NODE_NAME_SRC = new ColumnImpl("NODE_NAME_SRC", Types.VARCHAR, true, false);
            public static final ColumnImpl NODE_NAME_DST = new ColumnImpl("NODE_NAME_DST", Types.VARCHAR, true, false);

            public static final ColumnImpl UUID = new ColumnImpl("UUID", Types.CHAR, false, false);

            public static final Column[] ALL = new Column[] {
                UUID,
                NODE_NAME_SRC,
                NODE_NAME_DST
            };

            @Override
            public Column[] values()
            {
                return ALL;
            }

            @Override
            public String getName()
            {
                return "NODE_CONNECTIONS";
            }

            @Override
            public String toString()
            {
                return "Table NODE_CONNECTIONS";
            }
        }

        public static class NodeNetInterfaces implements DatabaseTable
        {
            private NodeNetInterfaces()
            {
            }

            // Primary Keys
            public static final ColumnImpl NODE_NAME = new ColumnImpl("NODE_NAME", Types.VARCHAR, true, false);
            public static final ColumnImpl NODE_NET_NAME = new ColumnImpl("NODE_NET_NAME", Types.VARCHAR, true, false);

            public static final ColumnImpl UUID = new ColumnImpl("UUID", Types.CHAR, false, false);
            public static final ColumnImpl NODE_NET_DSP_NAME = new ColumnImpl(
                "NODE_NET_DSP_NAME",
                Types.VARCHAR,
                false,
                false
            );
            public static final ColumnImpl INET_ADDRESS = new ColumnImpl("INET_ADDRESS", Types.VARCHAR, false, false);
            public static final ColumnImpl STLT_CONN_PORT = new ColumnImpl(
                "STLT_CONN_PORT",
                Types.SMALLINT,
                false,
                true
            );
            public static final ColumnImpl STLT_CONN_ENCR_TYPE = new ColumnImpl(
                "STLT_CONN_ENCR_TYPE",
                Types.VARCHAR,
                false,
                true
            );

            public static final Column[] ALL = new Column[] {
                UUID,
                NODE_NAME,
                NODE_NET_NAME,
                NODE_NET_DSP_NAME,
                INET_ADDRESS,
                STLT_CONN_PORT,
                STLT_CONN_ENCR_TYPE
            };

            @Override
            public Column[] values()
            {
                return ALL;
            }

            @Override
            public String getName()
            {
                return "NODE_NET_INTERFACES";
            }

            @Override
            public String toString()
            {
                return "Table NODE_NET_INTERFACES";
            }
        }

        public static class NodeStorPool implements DatabaseTable
        {
            private NodeStorPool()
            {
            }

            // Primary Keys
            public static final ColumnImpl NODE_NAME = new ColumnImpl("NODE_NAME", Types.VARCHAR, true, false);
            public static final ColumnImpl POOL_NAME = new ColumnImpl("POOL_NAME", Types.VARCHAR, true, false);

            public static final ColumnImpl UUID = new ColumnImpl("UUID", Types.CHAR, false, false);
            public static final ColumnImpl DRIVER_NAME = new ColumnImpl("DRIVER_NAME", Types.VARCHAR, false, false);
            public static final ColumnImpl FREE_SPACE_MGR_NAME = new ColumnImpl(
                "FREE_SPACE_MGR_NAME",
                Types.VARCHAR,
                false,
                false
            );
            public static final ColumnImpl FREE_SPACE_MGR_DSP_NAME = new ColumnImpl(
                "FREE_SPACE_MGR_DSP_NAME",
                Types.VARCHAR,
                false,
                false
            );
            public static final ColumnImpl EXTERNAL_LOCKING = new ColumnImpl(
                "EXTERNAL_LOCKING",
                Types.BOOLEAN,
                false,
                false
            );

            public static final Column[] ALL = new Column[] {
                UUID,
                NODE_NAME,
                POOL_NAME,
                DRIVER_NAME,
                FREE_SPACE_MGR_NAME,
                FREE_SPACE_MGR_DSP_NAME,
                EXTERNAL_LOCKING
            };

            @Override
            public Column[] values()
            {
                return ALL;
            }

            @Override
            public String getName()
            {
                return "NODE_STOR_POOL";
            }

            @Override
            public String toString()
            {
                return "Table NODE_STOR_POOL";
            }
        }

        public static class PropsContainers implements DatabaseTable
        {
            private PropsContainers()
            {
            }

            // Primary Keys
            public static final ColumnImpl PROPS_INSTANCE = new ColumnImpl(
                "PROPS_INSTANCE",
                Types.VARCHAR,
                true,
                false
            );
            public static final ColumnImpl PROP_KEY = new ColumnImpl("PROP_KEY", Types.VARCHAR, true, false);

            public static final ColumnImpl PROP_VALUE = new ColumnImpl("PROP_VALUE", Types.VARCHAR, false, false);

            public static final Column[] ALL = new Column[] {
                PROPS_INSTANCE,
                PROP_KEY,
                PROP_VALUE
            };

            @Override
            public Column[] values()
            {
                return ALL;
            }

            @Override
            public String getName()
            {
                return "PROPS_CONTAINERS";
            }

            @Override
            public String toString()
            {
                return "Table PROPS_CONTAINERS";
            }
        }

        public static class Resources implements DatabaseTable
        {
            private Resources()
            {
            }

            // Primary Keys
            public static final ColumnImpl NODE_NAME = new ColumnImpl("NODE_NAME", Types.VARCHAR, true, false);
            public static final ColumnImpl RESOURCE_NAME = new ColumnImpl("RESOURCE_NAME", Types.VARCHAR, true, false);
            public static final ColumnImpl SNAPSHOT_NAME = new ColumnImpl("SNAPSHOT_NAME", Types.VARCHAR, true, false);

            public static final ColumnImpl UUID = new ColumnImpl("UUID", Types.CHAR, false, false);
            public static final ColumnImpl RESOURCE_FLAGS = new ColumnImpl(
                "RESOURCE_FLAGS",
                Types.BIGINT,
                false,
                false
            );
            public static final ColumnImpl CREATE_TIMESTAMP = new ColumnImpl(
                "CREATE_TIMESTAMP",
                Types.TIMESTAMP,
                false,
                true
            );

            public static final Column[] ALL = new Column[] {
                UUID,
                NODE_NAME,
                RESOURCE_NAME,
                SNAPSHOT_NAME,
                RESOURCE_FLAGS,
                CREATE_TIMESTAMP
            };

            @Override
            public Column[] values()
            {
                return ALL;
            }

            @Override
            public String getName()
            {
                return "RESOURCES";
            }

            @Override
            public String toString()
            {
                return "Table RESOURCES";
            }
        }

        public static class ResourceConnections implements DatabaseTable
        {
            private ResourceConnections()
            {
            }

            // Primary Keys
            public static final ColumnImpl NODE_NAME_SRC = new ColumnImpl("NODE_NAME_SRC", Types.VARCHAR, true, false);
            public static final ColumnImpl NODE_NAME_DST = new ColumnImpl("NODE_NAME_DST", Types.VARCHAR, true, false);
            public static final ColumnImpl RESOURCE_NAME = new ColumnImpl("RESOURCE_NAME", Types.VARCHAR, true, false);
            public static final ColumnImpl SNAPSHOT_NAME = new ColumnImpl("SNAPSHOT_NAME", Types.VARCHAR, true, false);

            public static final ColumnImpl UUID = new ColumnImpl("UUID", Types.CHAR, false, false);
            public static final ColumnImpl FLAGS = new ColumnImpl("FLAGS", Types.BIGINT, false, false);
            public static final ColumnImpl TCP_PORT = new ColumnImpl("TCP_PORT", Types.INTEGER, false, true);

            public static final Column[] ALL = new Column[] {
                UUID,
                NODE_NAME_SRC,
                NODE_NAME_DST,
                RESOURCE_NAME,
                SNAPSHOT_NAME,
                FLAGS,
                TCP_PORT
            };

            @Override
            public Column[] values()
            {
                return ALL;
            }

            @Override
            public String getName()
            {
                return "RESOURCE_CONNECTIONS";
            }

            @Override
            public String toString()
            {
                return "Table RESOURCE_CONNECTIONS";
            }
        }

        public static class ResourceDefinitions implements DatabaseTable
        {
            private ResourceDefinitions()
            {
            }

            // Primary Keys
            public static final ColumnImpl RESOURCE_NAME = new ColumnImpl("RESOURCE_NAME", Types.VARCHAR, true, false);
            public static final ColumnImpl SNAPSHOT_NAME = new ColumnImpl("SNAPSHOT_NAME", Types.VARCHAR, true, false);

            public static final ColumnImpl UUID = new ColumnImpl("UUID", Types.CHAR, false, false);
            public static final ColumnImpl RESOURCE_DSP_NAME = new ColumnImpl(
                "RESOURCE_DSP_NAME",
                Types.VARCHAR,
                false,
                true
            );
            public static final ColumnImpl SNAPSHOT_DSP_NAME = new ColumnImpl(
                "SNAPSHOT_DSP_NAME",
                Types.VARCHAR,
                false,
                false
            );
            public static final ColumnImpl RESOURCE_FLAGS = new ColumnImpl(
                "RESOURCE_FLAGS",
                Types.BIGINT,
                false,
                false
            );
            public static final ColumnImpl LAYER_STACK = new ColumnImpl("LAYER_STACK", Types.VARCHAR, false, false);
            public static final ColumnImpl RESOURCE_EXTERNAL_NAME = new ColumnImpl(
                "RESOURCE_EXTERNAL_NAME",
                Types.BLOB,
                false,
                true
            );
            public static final ColumnImpl RESOURCE_GROUP_NAME = new ColumnImpl(
                "RESOURCE_GROUP_NAME",
                Types.VARCHAR,
                false,
                false
            );
            public static final ColumnImpl PARENT_UUID = new ColumnImpl("PARENT_UUID", Types.CHAR, false, true);

            public static final Column[] ALL = new Column[] {
                UUID,
                RESOURCE_NAME,
                SNAPSHOT_NAME,
                RESOURCE_DSP_NAME,
                SNAPSHOT_DSP_NAME,
                RESOURCE_FLAGS,
                LAYER_STACK,
                RESOURCE_EXTERNAL_NAME,
                RESOURCE_GROUP_NAME,
                PARENT_UUID
            };

            @Override
            public Column[] values()
            {
                return ALL;
            }

            @Override
            public String getName()
            {
                return "RESOURCE_DEFINITIONS";
            }

            @Override
            public String toString()
            {
                return "Table RESOURCE_DEFINITIONS";
            }
        }

        public static class ResourceGroups implements DatabaseTable
        {
            private ResourceGroups()
            {
            }

            // Primary Key
            public static final ColumnImpl RESOURCE_GROUP_NAME = new ColumnImpl(
                "RESOURCE_GROUP_NAME",
                Types.VARCHAR,
                true,
                false
            );

            public static final ColumnImpl UUID = new ColumnImpl("UUID", Types.CHAR, false, false);
            public static final ColumnImpl RESOURCE_GROUP_DSP_NAME = new ColumnImpl(
                "RESOURCE_GROUP_DSP_NAME",
                Types.VARCHAR,
                false,
                false
            );
            public static final ColumnImpl DESCRIPTION = new ColumnImpl("DESCRIPTION", Types.VARCHAR, false, true);
            public static final ColumnImpl LAYER_STACK = new ColumnImpl("LAYER_STACK", Types.VARCHAR, false, true);
            public static final ColumnImpl REPLICA_COUNT = new ColumnImpl("REPLICA_COUNT", Types.INTEGER, false, false);
            public static final ColumnImpl NODE_NAME_LIST = new ColumnImpl("NODE_NAME_LIST", Types.CLOB, false, true);
            public static final ColumnImpl POOL_NAME = new ColumnImpl("POOL_NAME", Types.CLOB, false, true);
            public static final ColumnImpl POOL_NAME_DISKLESS = new ColumnImpl(
                "POOL_NAME_DISKLESS",
                Types.CLOB,
                false,
                true
            );
            public static final ColumnImpl DO_NOT_PLACE_WITH_RSC_REGEX = new ColumnImpl(
                "DO_NOT_PLACE_WITH_RSC_REGEX",
                Types.CLOB,
                false,
                true
            );
            public static final ColumnImpl DO_NOT_PLACE_WITH_RSC_LIST = new ColumnImpl(
                "DO_NOT_PLACE_WITH_RSC_LIST",
                Types.CLOB,
                false,
                true
            );
            public static final ColumnImpl REPLICAS_ON_SAME = new ColumnImpl(
                "REPLICAS_ON_SAME",
                Types.CLOB,
                false,
                true
            );
            public static final ColumnImpl REPLICAS_ON_DIFFERENT = new ColumnImpl(
                "REPLICAS_ON_DIFFERENT",
                Types.CLOB,
                false,
                true
            );
            public static final ColumnImpl ALLOWED_PROVIDER_LIST = new ColumnImpl(
                "ALLOWED_PROVIDER_LIST",
                Types.VARCHAR,
                false,
                true
            );
            public static final ColumnImpl DISKLESS_ON_REMAINING = new ColumnImpl(
                "DISKLESS_ON_REMAINING",
                Types.BOOLEAN,
                false,
                true
            );

            public static final Column[] ALL = new Column[] {
                UUID,
                RESOURCE_GROUP_NAME,
                RESOURCE_GROUP_DSP_NAME,
                DESCRIPTION,
                LAYER_STACK,
                REPLICA_COUNT,
                NODE_NAME_LIST,
                POOL_NAME,
                POOL_NAME_DISKLESS,
                DO_NOT_PLACE_WITH_RSC_REGEX,
                DO_NOT_PLACE_WITH_RSC_LIST,
                REPLICAS_ON_SAME,
                REPLICAS_ON_DIFFERENT,
                ALLOWED_PROVIDER_LIST,
                DISKLESS_ON_REMAINING
            };

            @Override
            public Column[] values()
            {
                return ALL;
            }

            @Override
            public String getName()
            {
                return "RESOURCE_GROUPS";
            }

            @Override
            public String toString()
            {
                return "Table RESOURCE_GROUPS";
            }
        }

        public static class S3Remotes implements DatabaseTable
        {
            private S3Remotes()
            {
            }

            // Primary Key
            public static final ColumnImpl NAME = new ColumnImpl("NAME", Types.VARCHAR, true, false);

            public static final ColumnImpl UUID = new ColumnImpl("UUID", Types.CHAR, false, false);
            public static final ColumnImpl DSP_NAME = new ColumnImpl("DSP_NAME", Types.VARCHAR, false, false);
            public static final ColumnImpl FLAGS = new ColumnImpl("FLAGS", Types.BIGINT, false, false);
            public static final ColumnImpl ENDPOINT = new ColumnImpl("ENDPOINT", Types.VARCHAR, false, false);
            public static final ColumnImpl BUCKET = new ColumnImpl("BUCKET", Types.VARCHAR, false, false);
            public static final ColumnImpl REGION = new ColumnImpl("REGION", Types.VARCHAR, false, false);
            public static final ColumnImpl ACCESS_KEY = new ColumnImpl("ACCESS_KEY", Types.BLOB, false, false);
            public static final ColumnImpl SECRET_KEY = new ColumnImpl("SECRET_KEY", Types.BLOB, false, false);

            public static final Column[] ALL = new Column[] {
                UUID,
                NAME,
                DSP_NAME,
                FLAGS,
                ENDPOINT,
                BUCKET,
                REGION,
                ACCESS_KEY,
                SECRET_KEY
            };

            @Override
            public Column[] values()
            {
                return ALL;
            }

            @Override
            public String getName()
            {
                return "S3_REMOTES";
            }

            @Override
            public String toString()
            {
                return "Table S3_REMOTES";
            }
        }

        public static class SatellitesCapacity implements DatabaseTable
        {
            private SatellitesCapacity()
            {
            }

            // Primary Key
            public static final ColumnImpl NODE_NAME = new ColumnImpl("NODE_NAME", Types.VARCHAR, true, false);

            public static final ColumnImpl CAPACITY = new ColumnImpl("CAPACITY", Types.BLOB, false, false);
            public static final ColumnImpl FAIL_FLAG = new ColumnImpl("FAIL_FLAG", Types.BOOLEAN, false, false);
            public static final ColumnImpl ALLOCATED = new ColumnImpl("ALLOCATED", Types.BLOB, false, false);
            public static final ColumnImpl USABLE = new ColumnImpl("USABLE", Types.BLOB, false, false);

            public static final Column[] ALL = new Column[] {
                NODE_NAME,
                CAPACITY,
                FAIL_FLAG,
                ALLOCATED,
                USABLE
            };

            @Override
            public Column[] values()
            {
                return ALL;
            }

            @Override
            public String getName()
            {
                return "SATELLITES_CAPACITY";
            }

            @Override
            public String toString()
            {
                return "Table SATELLITES_CAPACITY";
            }
        }

        public static class Schedules implements DatabaseTable
        {
            private Schedules()
            {
            }

            // Primary Key
            public static final ColumnImpl NAME = new ColumnImpl("NAME", Types.VARCHAR, true, false);

            public static final ColumnImpl UUID = new ColumnImpl("UUID", Types.CHAR, false, false);
            public static final ColumnImpl DSP_NAME = new ColumnImpl("DSP_NAME", Types.VARCHAR, false, false);
            public static final ColumnImpl FLAGS = new ColumnImpl("FLAGS", Types.BIGINT, false, false);
            public static final ColumnImpl FULL_CRON = new ColumnImpl("FULL_CRON", Types.VARCHAR, false, false);
            public static final ColumnImpl INC_CRON = new ColumnImpl("INC_CRON", Types.VARCHAR, false, true);
            public static final ColumnImpl KEEP_LOCAL = new ColumnImpl("KEEP_LOCAL", Types.INTEGER, false, true);
            public static final ColumnImpl KEEP_REMOTE = new ColumnImpl("KEEP_REMOTE", Types.INTEGER, false, true);
            public static final ColumnImpl ON_FAILURE = new ColumnImpl("ON_FAILURE", Types.BIGINT, false, false);
            public static final ColumnImpl MAX_RETRIES = new ColumnImpl("MAX_RETRIES", Types.INTEGER, false, true);

            public static final Column[] ALL = new Column[] {
                UUID,
                NAME,
                DSP_NAME,
                FLAGS,
                FULL_CRON,
                INC_CRON,
                KEEP_LOCAL,
                KEEP_REMOTE,
                ON_FAILURE,
                MAX_RETRIES
            };

            @Override
            public Column[] values()
            {
                return ALL;
            }

            @Override
            public String getName()
            {
                return "SCHEDULES";
            }

            @Override
            public String toString()
            {
                return "Table SCHEDULES";
            }
        }

        public static class SecAccessTypes implements DatabaseTable
        {
            private SecAccessTypes()
            {
            }

            // Primary Key
            public static final ColumnImpl ACCESS_TYPE_NAME = new ColumnImpl(
                "ACCESS_TYPE_NAME",
                Types.VARCHAR,
                true,
                false
            );

            public static final ColumnImpl ACCESS_TYPE_VALUE = new ColumnImpl(
                "ACCESS_TYPE_VALUE",
                Types.SMALLINT,
                false,
                false
            );

            public static final Column[] ALL = new Column[] {
                ACCESS_TYPE_NAME,
                ACCESS_TYPE_VALUE
            };

            @Override
            public Column[] values()
            {
                return ALL;
            }

            @Override
            public String getName()
            {
                return "SEC_ACCESS_TYPES";
            }

            @Override
            public String toString()
            {
                return "Table SEC_ACCESS_TYPES";
            }
        }

        public static class SecAclMap implements DatabaseTable
        {
            private SecAclMap()
            {
            }

            // Primary Keys
            public static final ColumnImpl OBJECT_PATH = new ColumnImpl("OBJECT_PATH", Types.VARCHAR, true, false);
            public static final ColumnImpl ROLE_NAME = new ColumnImpl("ROLE_NAME", Types.VARCHAR, true, false);

            public static final ColumnImpl ACCESS_TYPE = new ColumnImpl("ACCESS_TYPE", Types.SMALLINT, false, false);

            public static final Column[] ALL = new Column[] {
                OBJECT_PATH,
                ROLE_NAME,
                ACCESS_TYPE
            };

            @Override
            public Column[] values()
            {
                return ALL;
            }

            @Override
            public String getName()
            {
                return "SEC_ACL_MAP";
            }

            @Override
            public String toString()
            {
                return "Table SEC_ACL_MAP";
            }
        }

        public static class SecConfiguration implements DatabaseTable
        {
            private SecConfiguration()
            {
            }

            // Primary Key
            public static final ColumnImpl ENTRY_KEY = new ColumnImpl("ENTRY_KEY", Types.VARCHAR, true, false);

            public static final ColumnImpl ENTRY_DSP_KEY = new ColumnImpl("ENTRY_DSP_KEY", Types.VARCHAR, false, false);
            public static final ColumnImpl ENTRY_VALUE = new ColumnImpl("ENTRY_VALUE", Types.VARCHAR, false, false);

            public static final Column[] ALL = new Column[] {
                ENTRY_KEY,
                ENTRY_DSP_KEY,
                ENTRY_VALUE
            };

            @Override
            public Column[] values()
            {
                return ALL;
            }

            @Override
            public String getName()
            {
                return "SEC_CONFIGURATION";
            }

            @Override
            public String toString()
            {
                return "Table SEC_CONFIGURATION";
            }
        }

        public static class SecDfltRoles implements DatabaseTable
        {
            private SecDfltRoles()
            {
            }

            // Primary Key
            public static final ColumnImpl IDENTITY_NAME = new ColumnImpl("IDENTITY_NAME", Types.VARCHAR, true, false);

            public static final ColumnImpl ROLE_NAME = new ColumnImpl("ROLE_NAME", Types.VARCHAR, false, false);

            public static final Column[] ALL = new Column[] {
                IDENTITY_NAME,
                ROLE_NAME
            };

            @Override
            public Column[] values()
            {
                return ALL;
            }

            @Override
            public String getName()
            {
                return "SEC_DFLT_ROLES";
            }

            @Override
            public String toString()
            {
                return "Table SEC_DFLT_ROLES";
            }
        }

        public static class SecIdentities implements DatabaseTable
        {
            private SecIdentities()
            {
            }

            // Primary Key
            public static final ColumnImpl IDENTITY_NAME = new ColumnImpl("IDENTITY_NAME", Types.VARCHAR, true, false);

            public static final ColumnImpl IDENTITY_DSP_NAME = new ColumnImpl(
                "IDENTITY_DSP_NAME",
                Types.VARCHAR,
                false,
                false
            );
            public static final ColumnImpl PASS_SALT = new ColumnImpl("PASS_SALT", Types.CHAR, false, true);
            public static final ColumnImpl PASS_HASH = new ColumnImpl("PASS_HASH", Types.CHAR, false, true);
            public static final ColumnImpl ID_ENABLED = new ColumnImpl("ID_ENABLED", Types.BOOLEAN, false, false);
            public static final ColumnImpl ID_LOCKED = new ColumnImpl("ID_LOCKED", Types.BOOLEAN, false, false);

            public static final Column[] ALL = new Column[] {
                IDENTITY_NAME,
                IDENTITY_DSP_NAME,
                PASS_SALT,
                PASS_HASH,
                ID_ENABLED,
                ID_LOCKED
            };

            @Override
            public Column[] values()
            {
                return ALL;
            }

            @Override
            public String getName()
            {
                return "SEC_IDENTITIES";
            }

            @Override
            public String toString()
            {
                return "Table SEC_IDENTITIES";
            }
        }

        public static class SecIdRoleMap implements DatabaseTable
        {
            private SecIdRoleMap()
            {
            }

            // Primary Keys
            public static final ColumnImpl IDENTITY_NAME = new ColumnImpl("IDENTITY_NAME", Types.VARCHAR, true, false);
            public static final ColumnImpl ROLE_NAME = new ColumnImpl("ROLE_NAME", Types.VARCHAR, true, false);

            public static final Column[] ALL = new Column[] {
                IDENTITY_NAME,
                ROLE_NAME
            };

            @Override
            public Column[] values()
            {
                return ALL;
            }

            @Override
            public String getName()
            {
                return "SEC_ID_ROLE_MAP";
            }

            @Override
            public String toString()
            {
                return "Table SEC_ID_ROLE_MAP";
            }
        }

        public static class SecObjectProtection implements DatabaseTable
        {
            private SecObjectProtection()
            {
            }

            // Primary Key
            public static final ColumnImpl OBJECT_PATH = new ColumnImpl("OBJECT_PATH", Types.VARCHAR, true, false);

            public static final ColumnImpl CREATOR_IDENTITY_NAME = new ColumnImpl(
                "CREATOR_IDENTITY_NAME",
                Types.VARCHAR,
                false,
                false
            );
            public static final ColumnImpl OWNER_ROLE_NAME = new ColumnImpl(
                "OWNER_ROLE_NAME",
                Types.VARCHAR,
                false,
                false
            );
            public static final ColumnImpl SECURITY_TYPE_NAME = new ColumnImpl(
                "SECURITY_TYPE_NAME",
                Types.VARCHAR,
                false,
                false
            );

            public static final Column[] ALL = new Column[] {
                OBJECT_PATH,
                CREATOR_IDENTITY_NAME,
                OWNER_ROLE_NAME,
                SECURITY_TYPE_NAME
            };

            @Override
            public Column[] values()
            {
                return ALL;
            }

            @Override
            public String getName()
            {
                return "SEC_OBJECT_PROTECTION";
            }

            @Override
            public String toString()
            {
                return "Table SEC_OBJECT_PROTECTION";
            }
        }

        public static class SecRoles implements DatabaseTable
        {
            private SecRoles()
            {
            }

            // Primary Key
            public static final ColumnImpl ROLE_NAME = new ColumnImpl("ROLE_NAME", Types.VARCHAR, true, false);

            public static final ColumnImpl ROLE_DSP_NAME = new ColumnImpl("ROLE_DSP_NAME", Types.VARCHAR, false, false);
            public static final ColumnImpl DOMAIN_NAME = new ColumnImpl("DOMAIN_NAME", Types.VARCHAR, false, false);
            public static final ColumnImpl ROLE_ENABLED = new ColumnImpl("ROLE_ENABLED", Types.BOOLEAN, false, false);
            public static final ColumnImpl ROLE_PRIVILEGES = new ColumnImpl(
                "ROLE_PRIVILEGES",
                Types.BIGINT,
                false,
                false
            );

            public static final Column[] ALL = new Column[] {
                ROLE_NAME,
                ROLE_DSP_NAME,
                DOMAIN_NAME,
                ROLE_ENABLED,
                ROLE_PRIVILEGES
            };

            @Override
            public Column[] values()
            {
                return ALL;
            }

            @Override
            public String getName()
            {
                return "SEC_ROLES";
            }

            @Override
            public String toString()
            {
                return "Table SEC_ROLES";
            }
        }

        public static class SecTypes implements DatabaseTable
        {
            private SecTypes()
            {
            }

            // Primary Key
            public static final ColumnImpl TYPE_NAME = new ColumnImpl("TYPE_NAME", Types.VARCHAR, true, false);

            public static final ColumnImpl TYPE_DSP_NAME = new ColumnImpl("TYPE_DSP_NAME", Types.VARCHAR, false, false);
            public static final ColumnImpl TYPE_ENABLED = new ColumnImpl("TYPE_ENABLED", Types.BOOLEAN, false, false);

            public static final Column[] ALL = new Column[] {
                TYPE_NAME,
                TYPE_DSP_NAME,
                TYPE_ENABLED
            };

            @Override
            public Column[] values()
            {
                return ALL;
            }

            @Override
            public String getName()
            {
                return "SEC_TYPES";
            }

            @Override
            public String toString()
            {
                return "Table SEC_TYPES";
            }
        }

        public static class SecTypeRules implements DatabaseTable
        {
            private SecTypeRules()
            {
            }

            // Primary Keys
            public static final ColumnImpl DOMAIN_NAME = new ColumnImpl("DOMAIN_NAME", Types.VARCHAR, true, false);
            public static final ColumnImpl TYPE_NAME = new ColumnImpl("TYPE_NAME", Types.VARCHAR, true, false);

            public static final ColumnImpl ACCESS_TYPE = new ColumnImpl("ACCESS_TYPE", Types.SMALLINT, false, false);

            public static final Column[] ALL = new Column[] {
                DOMAIN_NAME,
                TYPE_NAME,
                ACCESS_TYPE
            };

            @Override
            public Column[] values()
            {
                return ALL;
            }

            @Override
            public String getName()
            {
                return "SEC_TYPE_RULES";
            }

            @Override
            public String toString()
            {
                return "Table SEC_TYPE_RULES";
            }
        }

        public static class SpaceHistory implements DatabaseTable
        {
            private SpaceHistory()
            {
            }

            // Primary Key
            public static final ColumnImpl ENTRY_DATE = new ColumnImpl("ENTRY_DATE", Types.DATE, true, false);

            public static final ColumnImpl CAPACITY = new ColumnImpl("CAPACITY", Types.BLOB, false, false);

            public static final Column[] ALL = new Column[] {
                ENTRY_DATE,
                CAPACITY
            };

            @Override
            public Column[] values()
            {
                return ALL;
            }

            @Override
            public String getName()
            {
                return "SPACE_HISTORY";
            }

            @Override
            public String toString()
            {
                return "Table SPACE_HISTORY";
            }
        }

        public static class StorPoolDefinitions implements DatabaseTable
        {
            private StorPoolDefinitions()
            {
            }

            // Primary Key
            public static final ColumnImpl POOL_NAME = new ColumnImpl("POOL_NAME", Types.VARCHAR, true, false);

            public static final ColumnImpl UUID = new ColumnImpl("UUID", Types.CHAR, false, false);
            public static final ColumnImpl POOL_DSP_NAME = new ColumnImpl("POOL_DSP_NAME", Types.VARCHAR, false, false);

            public static final Column[] ALL = new Column[] {
                UUID,
                POOL_NAME,
                POOL_DSP_NAME
            };

            @Override
            public Column[] values()
            {
                return ALL;
            }

            @Override
            public String getName()
            {
                return "STOR_POOL_DEFINITIONS";
            }

            @Override
            public String toString()
            {
                return "Table STOR_POOL_DEFINITIONS";
            }
        }

        public static class TrackingDate implements DatabaseTable
        {
            private TrackingDate()
            {
            }

            // Primary Key

            public static final ColumnImpl ENTRY_DATE = new ColumnImpl("ENTRY_DATE", Types.DATE, false, false);

            public static final Column[] ALL = new Column[] {
                ENTRY_DATE
            };

            @Override
            public Column[] values()
            {
                return ALL;
            }

            @Override
            public String getName()
            {
                return "TRACKING_DATE";
            }

            @Override
            public String toString()
            {
                return "Table TRACKING_DATE";
            }
        }

        public static class Volumes implements DatabaseTable
        {
            private Volumes()
            {
            }

            // Primary Keys
            public static final ColumnImpl NODE_NAME = new ColumnImpl("NODE_NAME", Types.VARCHAR, true, false);
            public static final ColumnImpl RESOURCE_NAME = new ColumnImpl("RESOURCE_NAME", Types.VARCHAR, true, false);
            public static final ColumnImpl SNAPSHOT_NAME = new ColumnImpl("SNAPSHOT_NAME", Types.VARCHAR, true, false);
            public static final ColumnImpl VLM_NR = new ColumnImpl("VLM_NR", Types.INTEGER, true, false);

            public static final ColumnImpl UUID = new ColumnImpl("UUID", Types.CHAR, false, false);
            public static final ColumnImpl VLM_FLAGS = new ColumnImpl("VLM_FLAGS", Types.BIGINT, false, false);

            public static final Column[] ALL = new Column[] {
                UUID,
                NODE_NAME,
                RESOURCE_NAME,
                SNAPSHOT_NAME,
                VLM_NR,
                VLM_FLAGS
            };

            @Override
            public Column[] values()
            {
                return ALL;
            }

            @Override
            public String getName()
            {
                return "VOLUMES";
            }

            @Override
            public String toString()
            {
                return "Table VOLUMES";
            }
        }

        public static class VolumeConnections implements DatabaseTable
        {
            private VolumeConnections()
            {
            }

            // Primary Keys
            public static final ColumnImpl NODE_NAME_SRC = new ColumnImpl("NODE_NAME_SRC", Types.VARCHAR, true, false);
            public static final ColumnImpl NODE_NAME_DST = new ColumnImpl("NODE_NAME_DST", Types.VARCHAR, true, false);
            public static final ColumnImpl RESOURCE_NAME = new ColumnImpl("RESOURCE_NAME", Types.VARCHAR, true, false);
            public static final ColumnImpl SNAPSHOT_NAME = new ColumnImpl("SNAPSHOT_NAME", Types.VARCHAR, true, false);
            public static final ColumnImpl VLM_NR = new ColumnImpl("VLM_NR", Types.INTEGER, true, false);

            public static final ColumnImpl UUID = new ColumnImpl("UUID", Types.CHAR, false, false);

            public static final Column[] ALL = new Column[] {
                UUID,
                NODE_NAME_SRC,
                NODE_NAME_DST,
                RESOURCE_NAME,
                SNAPSHOT_NAME,
                VLM_NR
            };

            @Override
            public Column[] values()
            {
                return ALL;
            }

            @Override
            public String getName()
            {
                return "VOLUME_CONNECTIONS";
            }

            @Override
            public String toString()
            {
                return "Table VOLUME_CONNECTIONS";
            }
        }

        public static class VolumeDefinitions implements DatabaseTable
        {
            private VolumeDefinitions()
            {
            }

            // Primary Keys
            public static final ColumnImpl RESOURCE_NAME = new ColumnImpl("RESOURCE_NAME", Types.VARCHAR, true, false);
            public static final ColumnImpl SNAPSHOT_NAME = new ColumnImpl("SNAPSHOT_NAME", Types.VARCHAR, true, false);
            public static final ColumnImpl VLM_NR = new ColumnImpl("VLM_NR", Types.INTEGER, true, false);

            public static final ColumnImpl UUID = new ColumnImpl("UUID", Types.CHAR, false, false);
            public static final ColumnImpl VLM_SIZE = new ColumnImpl("VLM_SIZE", Types.BIGINT, false, false);
            public static final ColumnImpl VLM_FLAGS = new ColumnImpl("VLM_FLAGS", Types.BIGINT, false, false);

            public static final Column[] ALL = new Column[] {
                UUID,
                RESOURCE_NAME,
                SNAPSHOT_NAME,
                VLM_NR,
                VLM_SIZE,
                VLM_FLAGS
            };

            @Override
            public Column[] values()
            {
                return ALL;
            }

            @Override
            public String getName()
            {
                return "VOLUME_DEFINITIONS";
            }

            @Override
            public String toString()
            {
                return "Table VOLUME_DEFINITIONS";
            }
        }

        public static class VolumeGroups implements DatabaseTable
        {
            private VolumeGroups()
            {
            }

            // Primary Keys
            public static final ColumnImpl RESOURCE_GROUP_NAME = new ColumnImpl(
                "RESOURCE_GROUP_NAME",
                Types.VARCHAR,
                true,
                false
            );
            public static final ColumnImpl VLM_NR = new ColumnImpl("VLM_NR", Types.INTEGER, true, false);

            public static final ColumnImpl UUID = new ColumnImpl("UUID", Types.CHAR, false, false);
            public static final ColumnImpl FLAGS = new ColumnImpl("FLAGS", Types.BIGINT, false, false);

            public static final Column[] ALL = new Column[] {
                UUID,
                RESOURCE_GROUP_NAME,
                VLM_NR,
                FLAGS
            };

            @Override
            public Column[] values()
            {
                return ALL;
            }

            @Override
            public String getName()
            {
                return "VOLUME_GROUPS";
            }

            @Override
            public String toString()
            {
                return "Table VOLUME_GROUPS";
            }
        }

        public static final DatabaseTable[] ALL_TABLES; // initialized in static block
        public static final Files FILES = new Files();
        public static final KeyValueStore KEY_VALUE_STORE = new KeyValueStore();
        public static final LayerBcacheVolumes LAYER_BCACHE_VOLUMES = new LayerBcacheVolumes();
        public static final LayerCacheVolumes LAYER_CACHE_VOLUMES = new LayerCacheVolumes();
        public static final LayerDrbdResources LAYER_DRBD_RESOURCES = new LayerDrbdResources();
        public static final LayerDrbdResourceDefinitions LAYER_DRBD_RESOURCE_DEFINITIONS = new LayerDrbdResourceDefinitions();
        public static final LayerDrbdVolumes LAYER_DRBD_VOLUMES = new LayerDrbdVolumes();
        public static final LayerDrbdVolumeDefinitions LAYER_DRBD_VOLUME_DEFINITIONS = new LayerDrbdVolumeDefinitions();
        public static final LayerLuksVolumes LAYER_LUKS_VOLUMES = new LayerLuksVolumes();
        public static final LayerOpenflexResourceDefinitions LAYER_OPENFLEX_RESOURCE_DEFINITIONS = new LayerOpenflexResourceDefinitions();
        public static final LayerOpenflexVolumes LAYER_OPENFLEX_VOLUMES = new LayerOpenflexVolumes();
        public static final LayerResourceIds LAYER_RESOURCE_IDS = new LayerResourceIds();
        public static final LayerStorageVolumes LAYER_STORAGE_VOLUMES = new LayerStorageVolumes();
        public static final LayerWritecacheVolumes LAYER_WRITECACHE_VOLUMES = new LayerWritecacheVolumes();
        public static final LinstorRemotes LINSTOR_REMOTES = new LinstorRemotes();
        public static final Nodes NODES = new Nodes();
        public static final NodeConnections NODE_CONNECTIONS = new NodeConnections();
        public static final NodeNetInterfaces NODE_NET_INTERFACES = new NodeNetInterfaces();
        public static final NodeStorPool NODE_STOR_POOL = new NodeStorPool();
        public static final PropsContainers PROPS_CONTAINERS = new PropsContainers();
        public static final Resources RESOURCES = new Resources();
        public static final ResourceConnections RESOURCE_CONNECTIONS = new ResourceConnections();
        public static final ResourceDefinitions RESOURCE_DEFINITIONS = new ResourceDefinitions();
        public static final ResourceGroups RESOURCE_GROUPS = new ResourceGroups();
        public static final S3Remotes S3_REMOTES = new S3Remotes();
        public static final SatellitesCapacity SATELLITES_CAPACITY = new SatellitesCapacity();
        public static final Schedules SCHEDULES = new Schedules();
        public static final SecAccessTypes SEC_ACCESS_TYPES = new SecAccessTypes();
        public static final SecAclMap SEC_ACL_MAP = new SecAclMap();
        public static final SecConfiguration SEC_CONFIGURATION = new SecConfiguration();
        public static final SecDfltRoles SEC_DFLT_ROLES = new SecDfltRoles();
        public static final SecIdentities SEC_IDENTITIES = new SecIdentities();
        public static final SecIdRoleMap SEC_ID_ROLE_MAP = new SecIdRoleMap();
        public static final SecObjectProtection SEC_OBJECT_PROTECTION = new SecObjectProtection();
        public static final SecRoles SEC_ROLES = new SecRoles();
        public static final SecTypes SEC_TYPES = new SecTypes();
        public static final SecTypeRules SEC_TYPE_RULES = new SecTypeRules();
        public static final SpaceHistory SPACE_HISTORY = new SpaceHistory();
        public static final StorPoolDefinitions STOR_POOL_DEFINITIONS = new StorPoolDefinitions();
        public static final TrackingDate TRACKING_DATE = new TrackingDate();
        public static final Volumes VOLUMES = new Volumes();
        public static final VolumeConnections VOLUME_CONNECTIONS = new VolumeConnections();
        public static final VolumeDefinitions VOLUME_DEFINITIONS = new VolumeDefinitions();
        public static final VolumeGroups VOLUME_GROUPS = new VolumeGroups();

        static
        {
            ALL_TABLES = new DatabaseTable[] {
                FILES,
                KEY_VALUE_STORE,
                LAYER_BCACHE_VOLUMES,
                LAYER_CACHE_VOLUMES,
                LAYER_DRBD_RESOURCES,
                LAYER_DRBD_RESOURCE_DEFINITIONS,
                LAYER_DRBD_VOLUMES,
                LAYER_DRBD_VOLUME_DEFINITIONS,
                LAYER_LUKS_VOLUMES,
                LAYER_OPENFLEX_RESOURCE_DEFINITIONS,
                LAYER_OPENFLEX_VOLUMES,
                LAYER_RESOURCE_IDS,
                LAYER_STORAGE_VOLUMES,
                LAYER_WRITECACHE_VOLUMES,
                LINSTOR_REMOTES,
                NODES,
                NODE_CONNECTIONS,
                NODE_NET_INTERFACES,
                NODE_STOR_POOL,
                PROPS_CONTAINERS,
                RESOURCES,
                RESOURCE_CONNECTIONS,
                RESOURCE_DEFINITIONS,
                RESOURCE_GROUPS,
                S3_REMOTES,
                SATELLITES_CAPACITY,
                SCHEDULES,
                SEC_ACCESS_TYPES,
                SEC_ACL_MAP,
                SEC_CONFIGURATION,
                SEC_DFLT_ROLES,
                SEC_IDENTITIES,
                SEC_ID_ROLE_MAP,
                SEC_OBJECT_PROTECTION,
                SEC_ROLES,
                SEC_TYPES,
                SEC_TYPE_RULES,
                SPACE_HISTORY,
                STOR_POOL_DEFINITIONS,
                TRACKING_DATE,
                VOLUMES,
                VOLUME_CONNECTIONS,
                VOLUME_DEFINITIONS,
                VOLUME_GROUPS
            };

            Files.UUID.table = FILES;
            Files.PATH.table = FILES;
            Files.FLAGS.table = FILES;
            Files.CONTENT.table = FILES;
            Files.CONTENT_CHECKSUM.table = FILES;
            KeyValueStore.UUID.table = KEY_VALUE_STORE;
            KeyValueStore.KVS_NAME.table = KEY_VALUE_STORE;
            KeyValueStore.KVS_DSP_NAME.table = KEY_VALUE_STORE;
            LayerBcacheVolumes.LAYER_RESOURCE_ID.table = LAYER_BCACHE_VOLUMES;
            LayerBcacheVolumes.VLM_NR.table = LAYER_BCACHE_VOLUMES;
            LayerBcacheVolumes.NODE_NAME.table = LAYER_BCACHE_VOLUMES;
            LayerBcacheVolumes.POOL_NAME.table = LAYER_BCACHE_VOLUMES;
            LayerBcacheVolumes.DEV_UUID.table = LAYER_BCACHE_VOLUMES;
            LayerCacheVolumes.LAYER_RESOURCE_ID.table = LAYER_CACHE_VOLUMES;
            LayerCacheVolumes.VLM_NR.table = LAYER_CACHE_VOLUMES;
            LayerCacheVolumes.NODE_NAME.table = LAYER_CACHE_VOLUMES;
            LayerCacheVolumes.POOL_NAME_CACHE.table = LAYER_CACHE_VOLUMES;
            LayerCacheVolumes.POOL_NAME_META.table = LAYER_CACHE_VOLUMES;
            LayerDrbdResources.LAYER_RESOURCE_ID.table = LAYER_DRBD_RESOURCES;
            LayerDrbdResources.PEER_SLOTS.table = LAYER_DRBD_RESOURCES;
            LayerDrbdResources.AL_STRIPES.table = LAYER_DRBD_RESOURCES;
            LayerDrbdResources.AL_STRIPE_SIZE.table = LAYER_DRBD_RESOURCES;
            LayerDrbdResources.FLAGS.table = LAYER_DRBD_RESOURCES;
            LayerDrbdResources.NODE_ID.table = LAYER_DRBD_RESOURCES;
            LayerDrbdResourceDefinitions.RESOURCE_NAME.table = LAYER_DRBD_RESOURCE_DEFINITIONS;
            LayerDrbdResourceDefinitions.RESOURCE_NAME_SUFFIX.table = LAYER_DRBD_RESOURCE_DEFINITIONS;
            LayerDrbdResourceDefinitions.SNAPSHOT_NAME.table = LAYER_DRBD_RESOURCE_DEFINITIONS;
            LayerDrbdResourceDefinitions.PEER_SLOTS.table = LAYER_DRBD_RESOURCE_DEFINITIONS;
            LayerDrbdResourceDefinitions.AL_STRIPES.table = LAYER_DRBD_RESOURCE_DEFINITIONS;
            LayerDrbdResourceDefinitions.AL_STRIPE_SIZE.table = LAYER_DRBD_RESOURCE_DEFINITIONS;
            LayerDrbdResourceDefinitions.TCP_PORT.table = LAYER_DRBD_RESOURCE_DEFINITIONS;
            LayerDrbdResourceDefinitions.TRANSPORT_TYPE.table = LAYER_DRBD_RESOURCE_DEFINITIONS;
            LayerDrbdResourceDefinitions.SECRET.table = LAYER_DRBD_RESOURCE_DEFINITIONS;
            LayerDrbdVolumes.LAYER_RESOURCE_ID.table = LAYER_DRBD_VOLUMES;
            LayerDrbdVolumes.VLM_NR.table = LAYER_DRBD_VOLUMES;
            LayerDrbdVolumes.NODE_NAME.table = LAYER_DRBD_VOLUMES;
            LayerDrbdVolumes.POOL_NAME.table = LAYER_DRBD_VOLUMES;
            LayerDrbdVolumeDefinitions.RESOURCE_NAME.table = LAYER_DRBD_VOLUME_DEFINITIONS;
            LayerDrbdVolumeDefinitions.RESOURCE_NAME_SUFFIX.table = LAYER_DRBD_VOLUME_DEFINITIONS;
            LayerDrbdVolumeDefinitions.SNAPSHOT_NAME.table = LAYER_DRBD_VOLUME_DEFINITIONS;
            LayerDrbdVolumeDefinitions.VLM_NR.table = LAYER_DRBD_VOLUME_DEFINITIONS;
            LayerDrbdVolumeDefinitions.VLM_MINOR_NR.table = LAYER_DRBD_VOLUME_DEFINITIONS;
            LayerLuksVolumes.LAYER_RESOURCE_ID.table = LAYER_LUKS_VOLUMES;
            LayerLuksVolumes.VLM_NR.table = LAYER_LUKS_VOLUMES;
            LayerLuksVolumes.ENCRYPTED_PASSWORD.table = LAYER_LUKS_VOLUMES;
            LayerOpenflexResourceDefinitions.RESOURCE_NAME.table = LAYER_OPENFLEX_RESOURCE_DEFINITIONS;
            LayerOpenflexResourceDefinitions.SNAPSHOT_NAME.table = LAYER_OPENFLEX_RESOURCE_DEFINITIONS;
            LayerOpenflexResourceDefinitions.RESOURCE_NAME_SUFFIX.table = LAYER_OPENFLEX_RESOURCE_DEFINITIONS;
            LayerOpenflexResourceDefinitions.NQN.table = LAYER_OPENFLEX_RESOURCE_DEFINITIONS;
            LayerOpenflexVolumes.LAYER_RESOURCE_ID.table = LAYER_OPENFLEX_VOLUMES;
            LayerOpenflexVolumes.VLM_NR.table = LAYER_OPENFLEX_VOLUMES;
            LayerOpenflexVolumes.NODE_NAME.table = LAYER_OPENFLEX_VOLUMES;
            LayerOpenflexVolumes.POOL_NAME.table = LAYER_OPENFLEX_VOLUMES;
            LayerResourceIds.LAYER_RESOURCE_ID.table = LAYER_RESOURCE_IDS;
            LayerResourceIds.NODE_NAME.table = LAYER_RESOURCE_IDS;
            LayerResourceIds.RESOURCE_NAME.table = LAYER_RESOURCE_IDS;
            LayerResourceIds.SNAPSHOT_NAME.table = LAYER_RESOURCE_IDS;
            LayerResourceIds.LAYER_RESOURCE_KIND.table = LAYER_RESOURCE_IDS;
            LayerResourceIds.LAYER_RESOURCE_PARENT_ID.table = LAYER_RESOURCE_IDS;
            LayerResourceIds.LAYER_RESOURCE_SUFFIX.table = LAYER_RESOURCE_IDS;
            LayerResourceIds.LAYER_RESOURCE_SUSPENDED.table = LAYER_RESOURCE_IDS;
            LayerStorageVolumes.LAYER_RESOURCE_ID.table = LAYER_STORAGE_VOLUMES;
            LayerStorageVolumes.VLM_NR.table = LAYER_STORAGE_VOLUMES;
            LayerStorageVolumes.PROVIDER_KIND.table = LAYER_STORAGE_VOLUMES;
            LayerStorageVolumes.NODE_NAME.table = LAYER_STORAGE_VOLUMES;
            LayerStorageVolumes.STOR_POOL_NAME.table = LAYER_STORAGE_VOLUMES;
            LayerWritecacheVolumes.LAYER_RESOURCE_ID.table = LAYER_WRITECACHE_VOLUMES;
            LayerWritecacheVolumes.VLM_NR.table = LAYER_WRITECACHE_VOLUMES;
            LayerWritecacheVolumes.NODE_NAME.table = LAYER_WRITECACHE_VOLUMES;
            LayerWritecacheVolumes.POOL_NAME.table = LAYER_WRITECACHE_VOLUMES;
            LinstorRemotes.UUID.table = LINSTOR_REMOTES;
            LinstorRemotes.NAME.table = LINSTOR_REMOTES;
            LinstorRemotes.DSP_NAME.table = LINSTOR_REMOTES;
            LinstorRemotes.FLAGS.table = LINSTOR_REMOTES;
            LinstorRemotes.URL.table = LINSTOR_REMOTES;
            LinstorRemotes.ENCRYPTED_PASSPHRASE.table = LINSTOR_REMOTES;
            LinstorRemotes.CLUSTER_ID.table = LINSTOR_REMOTES;
            Nodes.UUID.table = NODES;
            Nodes.NODE_NAME.table = NODES;
            Nodes.NODE_DSP_NAME.table = NODES;
            Nodes.NODE_FLAGS.table = NODES;
            Nodes.NODE_TYPE.table = NODES;
            NodeConnections.UUID.table = NODE_CONNECTIONS;
            NodeConnections.NODE_NAME_SRC.table = NODE_CONNECTIONS;
            NodeConnections.NODE_NAME_DST.table = NODE_CONNECTIONS;
            NodeNetInterfaces.UUID.table = NODE_NET_INTERFACES;
            NodeNetInterfaces.NODE_NAME.table = NODE_NET_INTERFACES;
            NodeNetInterfaces.NODE_NET_NAME.table = NODE_NET_INTERFACES;
            NodeNetInterfaces.NODE_NET_DSP_NAME.table = NODE_NET_INTERFACES;
            NodeNetInterfaces.INET_ADDRESS.table = NODE_NET_INTERFACES;
            NodeNetInterfaces.STLT_CONN_PORT.table = NODE_NET_INTERFACES;
            NodeNetInterfaces.STLT_CONN_ENCR_TYPE.table = NODE_NET_INTERFACES;
            NodeStorPool.UUID.table = NODE_STOR_POOL;
            NodeStorPool.NODE_NAME.table = NODE_STOR_POOL;
            NodeStorPool.POOL_NAME.table = NODE_STOR_POOL;
            NodeStorPool.DRIVER_NAME.table = NODE_STOR_POOL;
            NodeStorPool.FREE_SPACE_MGR_NAME.table = NODE_STOR_POOL;
            NodeStorPool.FREE_SPACE_MGR_DSP_NAME.table = NODE_STOR_POOL;
            NodeStorPool.EXTERNAL_LOCKING.table = NODE_STOR_POOL;
            PropsContainers.PROPS_INSTANCE.table = PROPS_CONTAINERS;
            PropsContainers.PROP_KEY.table = PROPS_CONTAINERS;
            PropsContainers.PROP_VALUE.table = PROPS_CONTAINERS;
            Resources.UUID.table = RESOURCES;
            Resources.NODE_NAME.table = RESOURCES;
            Resources.RESOURCE_NAME.table = RESOURCES;
            Resources.SNAPSHOT_NAME.table = RESOURCES;
            Resources.RESOURCE_FLAGS.table = RESOURCES;
            Resources.CREATE_TIMESTAMP.table = RESOURCES;
            ResourceConnections.UUID.table = RESOURCE_CONNECTIONS;
            ResourceConnections.NODE_NAME_SRC.table = RESOURCE_CONNECTIONS;
            ResourceConnections.NODE_NAME_DST.table = RESOURCE_CONNECTIONS;
            ResourceConnections.RESOURCE_NAME.table = RESOURCE_CONNECTIONS;
            ResourceConnections.SNAPSHOT_NAME.table = RESOURCE_CONNECTIONS;
            ResourceConnections.FLAGS.table = RESOURCE_CONNECTIONS;
            ResourceConnections.TCP_PORT.table = RESOURCE_CONNECTIONS;
            ResourceDefinitions.UUID.table = RESOURCE_DEFINITIONS;
            ResourceDefinitions.RESOURCE_NAME.table = RESOURCE_DEFINITIONS;
            ResourceDefinitions.SNAPSHOT_NAME.table = RESOURCE_DEFINITIONS;
            ResourceDefinitions.RESOURCE_DSP_NAME.table = RESOURCE_DEFINITIONS;
            ResourceDefinitions.SNAPSHOT_DSP_NAME.table = RESOURCE_DEFINITIONS;
            ResourceDefinitions.RESOURCE_FLAGS.table = RESOURCE_DEFINITIONS;
            ResourceDefinitions.LAYER_STACK.table = RESOURCE_DEFINITIONS;
            ResourceDefinitions.RESOURCE_EXTERNAL_NAME.table = RESOURCE_DEFINITIONS;
            ResourceDefinitions.RESOURCE_GROUP_NAME.table = RESOURCE_DEFINITIONS;
            ResourceDefinitions.PARENT_UUID.table = RESOURCE_DEFINITIONS;
            ResourceGroups.UUID.table = RESOURCE_GROUPS;
            ResourceGroups.RESOURCE_GROUP_NAME.table = RESOURCE_GROUPS;
            ResourceGroups.RESOURCE_GROUP_DSP_NAME.table = RESOURCE_GROUPS;
            ResourceGroups.DESCRIPTION.table = RESOURCE_GROUPS;
            ResourceGroups.LAYER_STACK.table = RESOURCE_GROUPS;
            ResourceGroups.REPLICA_COUNT.table = RESOURCE_GROUPS;
            ResourceGroups.NODE_NAME_LIST.table = RESOURCE_GROUPS;
            ResourceGroups.POOL_NAME.table = RESOURCE_GROUPS;
            ResourceGroups.POOL_NAME_DISKLESS.table = RESOURCE_GROUPS;
            ResourceGroups.DO_NOT_PLACE_WITH_RSC_REGEX.table = RESOURCE_GROUPS;
            ResourceGroups.DO_NOT_PLACE_WITH_RSC_LIST.table = RESOURCE_GROUPS;
            ResourceGroups.REPLICAS_ON_SAME.table = RESOURCE_GROUPS;
            ResourceGroups.REPLICAS_ON_DIFFERENT.table = RESOURCE_GROUPS;
            ResourceGroups.ALLOWED_PROVIDER_LIST.table = RESOURCE_GROUPS;
            ResourceGroups.DISKLESS_ON_REMAINING.table = RESOURCE_GROUPS;
            S3Remotes.UUID.table = S3_REMOTES;
            S3Remotes.NAME.table = S3_REMOTES;
            S3Remotes.DSP_NAME.table = S3_REMOTES;
            S3Remotes.FLAGS.table = S3_REMOTES;
            S3Remotes.ENDPOINT.table = S3_REMOTES;
            S3Remotes.BUCKET.table = S3_REMOTES;
            S3Remotes.REGION.table = S3_REMOTES;
            S3Remotes.ACCESS_KEY.table = S3_REMOTES;
            S3Remotes.SECRET_KEY.table = S3_REMOTES;
            SatellitesCapacity.NODE_NAME.table = SATELLITES_CAPACITY;
            SatellitesCapacity.CAPACITY.table = SATELLITES_CAPACITY;
            SatellitesCapacity.FAIL_FLAG.table = SATELLITES_CAPACITY;
            SatellitesCapacity.ALLOCATED.table = SATELLITES_CAPACITY;
            SatellitesCapacity.USABLE.table = SATELLITES_CAPACITY;
            Schedules.UUID.table = SCHEDULES;
            Schedules.NAME.table = SCHEDULES;
            Schedules.DSP_NAME.table = SCHEDULES;
            Schedules.FLAGS.table = SCHEDULES;
            Schedules.FULL_CRON.table = SCHEDULES;
            Schedules.INC_CRON.table = SCHEDULES;
            Schedules.KEEP_LOCAL.table = SCHEDULES;
            Schedules.KEEP_REMOTE.table = SCHEDULES;
            Schedules.ON_FAILURE.table = SCHEDULES;
            Schedules.MAX_RETRIES.table = SCHEDULES;
            SecAccessTypes.ACCESS_TYPE_NAME.table = SEC_ACCESS_TYPES;
            SecAccessTypes.ACCESS_TYPE_VALUE.table = SEC_ACCESS_TYPES;
            SecAclMap.OBJECT_PATH.table = SEC_ACL_MAP;
            SecAclMap.ROLE_NAME.table = SEC_ACL_MAP;
            SecAclMap.ACCESS_TYPE.table = SEC_ACL_MAP;
            SecConfiguration.ENTRY_KEY.table = SEC_CONFIGURATION;
            SecConfiguration.ENTRY_DSP_KEY.table = SEC_CONFIGURATION;
            SecConfiguration.ENTRY_VALUE.table = SEC_CONFIGURATION;
            SecDfltRoles.IDENTITY_NAME.table = SEC_DFLT_ROLES;
            SecDfltRoles.ROLE_NAME.table = SEC_DFLT_ROLES;
            SecIdentities.IDENTITY_NAME.table = SEC_IDENTITIES;
            SecIdentities.IDENTITY_DSP_NAME.table = SEC_IDENTITIES;
            SecIdentities.PASS_SALT.table = SEC_IDENTITIES;
            SecIdentities.PASS_HASH.table = SEC_IDENTITIES;
            SecIdentities.ID_ENABLED.table = SEC_IDENTITIES;
            SecIdentities.ID_LOCKED.table = SEC_IDENTITIES;
            SecIdRoleMap.IDENTITY_NAME.table = SEC_ID_ROLE_MAP;
            SecIdRoleMap.ROLE_NAME.table = SEC_ID_ROLE_MAP;
            SecObjectProtection.OBJECT_PATH.table = SEC_OBJECT_PROTECTION;
            SecObjectProtection.CREATOR_IDENTITY_NAME.table = SEC_OBJECT_PROTECTION;
            SecObjectProtection.OWNER_ROLE_NAME.table = SEC_OBJECT_PROTECTION;
            SecObjectProtection.SECURITY_TYPE_NAME.table = SEC_OBJECT_PROTECTION;
            SecRoles.ROLE_NAME.table = SEC_ROLES;
            SecRoles.ROLE_DSP_NAME.table = SEC_ROLES;
            SecRoles.DOMAIN_NAME.table = SEC_ROLES;
            SecRoles.ROLE_ENABLED.table = SEC_ROLES;
            SecRoles.ROLE_PRIVILEGES.table = SEC_ROLES;
            SecTypes.TYPE_NAME.table = SEC_TYPES;
            SecTypes.TYPE_DSP_NAME.table = SEC_TYPES;
            SecTypes.TYPE_ENABLED.table = SEC_TYPES;
            SecTypeRules.DOMAIN_NAME.table = SEC_TYPE_RULES;
            SecTypeRules.TYPE_NAME.table = SEC_TYPE_RULES;
            SecTypeRules.ACCESS_TYPE.table = SEC_TYPE_RULES;
            SpaceHistory.ENTRY_DATE.table = SPACE_HISTORY;
            SpaceHistory.CAPACITY.table = SPACE_HISTORY;
            StorPoolDefinitions.UUID.table = STOR_POOL_DEFINITIONS;
            StorPoolDefinitions.POOL_NAME.table = STOR_POOL_DEFINITIONS;
            StorPoolDefinitions.POOL_DSP_NAME.table = STOR_POOL_DEFINITIONS;
            TrackingDate.ENTRY_DATE.table = TRACKING_DATE;
            Volumes.UUID.table = VOLUMES;
            Volumes.NODE_NAME.table = VOLUMES;
            Volumes.RESOURCE_NAME.table = VOLUMES;
            Volumes.SNAPSHOT_NAME.table = VOLUMES;
            Volumes.VLM_NR.table = VOLUMES;
            Volumes.VLM_FLAGS.table = VOLUMES;
            VolumeConnections.UUID.table = VOLUME_CONNECTIONS;
            VolumeConnections.NODE_NAME_SRC.table = VOLUME_CONNECTIONS;
            VolumeConnections.NODE_NAME_DST.table = VOLUME_CONNECTIONS;
            VolumeConnections.RESOURCE_NAME.table = VOLUME_CONNECTIONS;
            VolumeConnections.SNAPSHOT_NAME.table = VOLUME_CONNECTIONS;
            VolumeConnections.VLM_NR.table = VOLUME_CONNECTIONS;
            VolumeDefinitions.UUID.table = VOLUME_DEFINITIONS;
            VolumeDefinitions.RESOURCE_NAME.table = VOLUME_DEFINITIONS;
            VolumeDefinitions.SNAPSHOT_NAME.table = VOLUME_DEFINITIONS;
            VolumeDefinitions.VLM_NR.table = VOLUME_DEFINITIONS;
            VolumeDefinitions.VLM_SIZE.table = VOLUME_DEFINITIONS;
            VolumeDefinitions.VLM_FLAGS.table = VOLUME_DEFINITIONS;
            VolumeGroups.UUID.table = VOLUME_GROUPS;
            VolumeGroups.RESOURCE_GROUP_NAME.table = VOLUME_GROUPS;
            VolumeGroups.VLM_NR.table = VOLUME_GROUPS;
            VolumeGroups.FLAGS.table = VOLUME_GROUPS;
        }

        public static class ColumnImpl implements Column
        {
            private final String name;
            private final int sqlType;
            private final boolean isPk;
            private final boolean isNullable;
            private DatabaseTable table;

            public ColumnImpl(
                final String nameRef,
                final int sqlTypeRef,
                final boolean isPkRef,
                final boolean isNullableRef
            )
            {
                name = nameRef;
                sqlType = sqlTypeRef;
                isPk = isPkRef;
                isNullable = isNullableRef;
            }

            @Override
            public String getName()
            {
                return name;
            }

            @Override
            public int getSqlType()
            {
                return sqlType;
            }

            @Override
            public boolean isPk()
            {
                return isPk;
            }

            @Override
            public boolean isNullable()
            {
                return isNullable;
            }

            @Override
            public DatabaseTable getTable()
            {
                return table;
            }

            @Override
            public String toString()
            {
                return (table == null ? "No table set" : table) + ", Column: " + name;
            }
        }

        @SuppressWarnings("checkstyle:ReturnCount")
        public static DatabaseTable getByValue(String value)
        {
            switch (value.toUpperCase())
            {
                case "FILES":
                    return FILES;
                case "KEY_VALUE_STORE":
                    return KEY_VALUE_STORE;
                case "LAYER_BCACHE_VOLUMES":
                    return LAYER_BCACHE_VOLUMES;
                case "LAYER_CACHE_VOLUMES":
                    return LAYER_CACHE_VOLUMES;
                case "LAYER_DRBD_RESOURCES":
                    return LAYER_DRBD_RESOURCES;
                case "LAYER_DRBD_RESOURCE_DEFINITIONS":
                    return LAYER_DRBD_RESOURCE_DEFINITIONS;
                case "LAYER_DRBD_VOLUMES":
                    return LAYER_DRBD_VOLUMES;
                case "LAYER_DRBD_VOLUME_DEFINITIONS":
                    return LAYER_DRBD_VOLUME_DEFINITIONS;
                case "LAYER_LUKS_VOLUMES":
                    return LAYER_LUKS_VOLUMES;
                case "LAYER_OPENFLEX_RESOURCE_DEFINITIONS":
                    return LAYER_OPENFLEX_RESOURCE_DEFINITIONS;
                case "LAYER_OPENFLEX_VOLUMES":
                    return LAYER_OPENFLEX_VOLUMES;
                case "LAYER_RESOURCE_IDS":
                    return LAYER_RESOURCE_IDS;
                case "LAYER_STORAGE_VOLUMES":
                    return LAYER_STORAGE_VOLUMES;
                case "LAYER_WRITECACHE_VOLUMES":
                    return LAYER_WRITECACHE_VOLUMES;
                case "LINSTOR_REMOTES":
                    return LINSTOR_REMOTES;
                case "NODES":
                    return NODES;
                case "NODE_CONNECTIONS":
                    return NODE_CONNECTIONS;
                case "NODE_NET_INTERFACES":
                    return NODE_NET_INTERFACES;
                case "NODE_STOR_POOL":
                    return NODE_STOR_POOL;
                case "PROPS_CONTAINERS":
                    return PROPS_CONTAINERS;
                case "RESOURCES":
                    return RESOURCES;
                case "RESOURCE_CONNECTIONS":
                    return RESOURCE_CONNECTIONS;
                case "RESOURCE_DEFINITIONS":
                    return RESOURCE_DEFINITIONS;
                case "RESOURCE_GROUPS":
                    return RESOURCE_GROUPS;
                case "S3_REMOTES":
                    return S3_REMOTES;
                case "SATELLITES_CAPACITY":
                    return SATELLITES_CAPACITY;
                case "SCHEDULES":
                    return SCHEDULES;
                case "SEC_ACCESS_TYPES":
                    return SEC_ACCESS_TYPES;
                case "SEC_ACL_MAP":
                    return SEC_ACL_MAP;
                case "SEC_CONFIGURATION":
                    return SEC_CONFIGURATION;
                case "SEC_DFLT_ROLES":
                    return SEC_DFLT_ROLES;
                case "SEC_IDENTITIES":
                    return SEC_IDENTITIES;
                case "SEC_ID_ROLE_MAP":
                    return SEC_ID_ROLE_MAP;
                case "SEC_OBJECT_PROTECTION":
                    return SEC_OBJECT_PROTECTION;
                case "SEC_ROLES":
                    return SEC_ROLES;
                case "SEC_TYPES":
                    return SEC_TYPES;
                case "SEC_TYPE_RULES":
                    return SEC_TYPE_RULES;
                case "SPACE_HISTORY":
                    return SPACE_HISTORY;
                case "STOR_POOL_DEFINITIONS":
                    return STOR_POOL_DEFINITIONS;
                case "TRACKING_DATE":
                    return TRACKING_DATE;
                case "VOLUMES":
                    return VOLUMES;
                case "VOLUME_CONNECTIONS":
                    return VOLUME_CONNECTIONS;
                case "VOLUME_DEFINITIONS":
                    return VOLUME_DEFINITIONS;
                case "VOLUME_GROUPS":
                    return VOLUME_GROUPS;
                default:
                    throw new ImplementationError("Unknown database table: " + value);
            }
        }
    }

}
