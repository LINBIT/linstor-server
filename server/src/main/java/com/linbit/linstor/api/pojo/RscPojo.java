package com.linbit.linstor.api.pojo;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.interfaces.RscLayerDataApi;
import com.linbit.linstor.core.apis.ResourceApi;
import com.linbit.linstor.core.apis.ResourceConnectionApi;
import com.linbit.linstor.core.apis.ResourceDefinitionApi;
import com.linbit.linstor.core.apis.VolumeApi;
import com.linbit.linstor.core.apis.VolumeDefinitionApi;
import com.linbit.linstor.core.objects.AbsResource;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

public class RscPojo implements Comparable<RscPojo>, ResourceApi
{
    private final String rscName;
    private final String nodeName;
    private final @Nullable UUID nodeUuid;
    private final @Nullable ResourceDefinitionApi rscDefinition;
    private final @Nullable UUID localRscUuid;
    private final long localRscFlags;
    private final Map<String, String> localRscProps;
    private final List<VolumeApi> localVlms;
    private final @Nullable List<OtherRscPojo> otherRscs;
    private final @Nullable List<ResourceConnectionApi> rscConnections;
    private final @Nullable Long fullSyncId;
    private final @Nullable Long updateId;
    private final @Nullable RscLayerDataApi rscLayerDataPojo;
    private final @Nullable Date createTimestamp;
    private final @Nullable EffectivePropertiesPojo propsPojo;

    public RscPojo(
        final String rscNameRef,
        final String nodeNameRef,
        final @Nullable UUID nodeUuidRef,
        final @Nullable ResourceDefinitionApi rscDefinitionRef,
        final @Nullable UUID localRscUuidRef,
        final long localRscFlagsRef,
        final Map<String, String> localRscPropsRef,
        final List<VolumeApi> localVlmsRef,
        final @Nullable List<OtherRscPojo> otherRscListRef,
        final @Nullable List<ResourceConnectionApi> rscConnectionsRef,
        final @Nullable Long fullSyncIdRef,
        final @Nullable Long updateIdRef,
        final @Nullable RscLayerDataApi rscLayerDataPojoRef,
        final @Nullable Date createTimestampRef,
        @Nullable EffectivePropertiesPojo propsPojoRef
    )
    {
        rscName = rscNameRef;
        nodeName = nodeNameRef;
        nodeUuid = nodeUuidRef;
        rscDefinition = rscDefinitionRef;
        localRscUuid = localRscUuidRef;
        localRscFlags = localRscFlagsRef;
        localRscProps = localRscPropsRef;
        rscConnections = rscConnectionsRef;
        localVlms = localVlmsRef;
        otherRscs = otherRscListRef;
        fullSyncId = fullSyncIdRef;
        updateId = updateIdRef;
        rscLayerDataPojo = rscLayerDataPojoRef;
        propsPojo = propsPojoRef;
        createTimestamp = createTimestampRef != null &&
            createTimestampRef.getTime() != AbsResource.CREATE_DATE_INIT_VALUE ?
                createTimestampRef : null;
    }

    /**
     * Simple constructor only initializing must have fields for resource creation.
     *
     * @param rscNameRef Resource name.
     * @param nodeNameRef Node name.
     * @param flagsRef Flags mask value.
     * @param propsRef Property map.
     */
    public RscPojo(
        final String rscNameRef,
        final String nodeNameRef,
        final long flagsRef,
        final Map<String, String> propsRef
    )
    {
        rscName = rscNameRef;
        nodeName = nodeNameRef;
        localRscFlags = flagsRef;
        localRscProps = propsRef;


        nodeUuid = null;
        rscDefinition = null;
        localRscUuid = null;
        localVlms = new ArrayList<>();
        otherRscs = null;
        rscConnections = null;
        fullSyncId = null;
        updateId = null;
        rscLayerDataPojo = null;
        createTimestamp = null;
        propsPojo = null;
    }

    @Override
    public String getName()
    {
        return rscName;
    }

    @Override
    public @Nullable UUID getUuid()
    {
        return localRscUuid;
    }

    @Override
    public String getNodeName()
    {
        return nodeName;
    }

    @Override
    public @Nullable UUID getNodeUuid()
    {
        return nodeUuid;
    }

    @Override
    public @Nullable UUID getRscDfnUuid()
    {
        return rscDefinition == null ? null : rscDefinition.getUuid();
    }

    public @Nullable ResourceDefinitionApi getRscDfnApi()
    {
        return rscDefinition;
    }

    public @Nullable Long getRscDfnFlags()
    {
        return rscDefinition == null ? null : rscDefinition.getFlags();
    }

    public @Nullable Map<String, String> getRscDfnProps()
    {
        return rscDefinition == null ? null : rscDefinition.getProps();
    }

    public @Nullable String getResourceGroupName()
    {
        return rscDefinition == null ? null : rscDefinition.getResourceName();
    }

    public @Nullable UUID getLocalRscUuid()
    {
        return localRscUuid;
    }

    public long getLocalRscFlags()
    {
        return localRscFlags;
    }

    @Override
    public long getFlags()
    {
        return localRscFlags;
    }

    public Map<String, String> getLocalRscProps()
    {
        return localRscProps;
    }

    public @Nullable List<VolumeDefinitionApi> getVlmDfns()
    {
        return rscDefinition == null ? null : rscDefinition.getVlmDfnList();
    }

    public List<VolumeApi> getLocalVlms()
    {
        return localVlms;
    }

    @Override
    public Optional<Date> getCreateTimestamp()
    {
        return Optional.ofNullable(createTimestamp);
    }

    @Override
    public Map<String, String> getProps()
    {
        return localRscProps;
    }

    @Override
    public List<? extends VolumeApi> getVlmList()
    {
        return localVlms;
    }

    public @Nullable List<OtherRscPojo> getOtherRscList()
    {
        return otherRscs;
    }

    public @Nullable List<ResourceConnectionApi> getRscConnections()
    {
        return rscConnections;
    }

    public @Nullable Long getFullSyncId()
    {
        return fullSyncId;
    }

    public @Nullable Long getUpdateId()
    {
        return updateId;
    }

    @Override
    public @Nullable RscLayerDataApi getLayerData()
    {
        return rscLayerDataPojo;
    }

    @Override
    public @Nullable EffectivePropertiesPojo getEffectivePropsPojo()
    {
        return propsPojo;
    }

    @Override
    public int compareTo(RscPojo otherRscPojo)
    {
        return rscName.compareTo(otherRscPojo.rscName);
    }

    public static class OtherRscPojo
    {
        private final String nodeName;
        private final UUID nodeUuid;
        private final String nodeType;
        private final long nodeFlags;
        private final Map<String, String> nodeProps;
        private final UUID rscUuid;
        private final long rscFlags;
        private final Map<String, String> rscProps;
        private final List<VolumeApi> vlms;
        private final List<OtherNodeNetInterfacePojo> netInterfacefPojos;
        private final RscLayerDataApi rscLayerDataPojo;

        public OtherRscPojo(
            String nodeNameRef,
            UUID nodeUuidRef,
            String nodeTypeRef,
            long nodeFlagsRef,
            Map<String, String> nodePropsRef,
            List<OtherNodeNetInterfacePojo> netIfPojosRef,
            UUID rscUuidRef,
            long rscFlagsRef,
            Map<String, String> rscPropsRef,
            List<VolumeApi> vlmsRef,
            RscLayerDataApi rscLayerDataPojoRef
        )
        {
            nodeName = nodeNameRef;
            nodeUuid = nodeUuidRef;
            nodeType = nodeTypeRef;
            nodeFlags = nodeFlagsRef;
            nodeProps = nodePropsRef;
            netInterfacefPojos = netIfPojosRef;
            rscUuid = rscUuidRef;
            rscFlags = rscFlagsRef;
            rscProps = rscPropsRef;
            vlms = vlmsRef;
            rscLayerDataPojo = rscLayerDataPojoRef;
        }

        public String getNodeName()
        {
            return nodeName;
        }

        public UUID getNodeUuid()
        {
            return nodeUuid;
        }

        public String getNodeType()
        {
            return nodeType;
        }

        public long getNodeFlags()
        {
            return nodeFlags;
        }

        public List<OtherNodeNetInterfacePojo> getNetInterfacefPojos()
        {
            return netInterfacefPojos;
        }

        public Map<String, String> getNodeProps()
        {
            return nodeProps;
        }

        public UUID getRscUuid()
        {
            return rscUuid;
        }

        public long getRscFlags()
        {
            return rscFlags;
        }

        public Map<String, String> getRscProps()
        {
            return rscProps;
        }

        public List<VolumeApi> getVlms()
        {
            return vlms;
        }

        public RscLayerDataApi getRscLayerDataPojo()
        {
            return rscLayerDataPojo;
        }
    }

    public static class OtherNodeNetInterfacePojo
    {
        private final UUID uuid;
        private final String name;
        private final String address;

        public OtherNodeNetInterfacePojo(final UUID uuidRef, final String nameRef, final String addressRef)
        {
            uuid = uuidRef;
            name = nameRef;
            address = addressRef;
        }

        public UUID getUuid()
        {
            return uuid;
        }

        public String getName()
        {
            return name;
        }

        public String getAddress()
        {
            return address;
        }
    }

}
