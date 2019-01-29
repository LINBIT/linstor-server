package com.linbit.linstor.api.interfaces.serializer;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.linbit.linstor.KeyValueStore;
import com.linbit.linstor.Node;
import com.linbit.linstor.NodeName;
import com.linbit.linstor.Resource;
import com.linbit.linstor.ResourceConnection;
import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.SnapshotDefinition;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.StorPoolDefinition.StorPoolDfnApi;
import com.linbit.linstor.api.protobuf.MaxVlmSizeCandidatePojo;
import com.linbit.linstor.satellitestate.SatelliteState;

public interface CtrlClientSerializer extends CommonSerializer
{
    @Override
    CtrlClientSerializerBuilder headerlessBuilder();

    @Override
    CtrlClientSerializerBuilder onewayBuilder(String apiCall);

    @Override
    CtrlClientSerializerBuilder apiCallBuilder(String apiCall, Long apiCallId);

    @Override
    CtrlClientSerializerBuilder answerBuilder(String msgContent, Long apiCallId);

    @Override
    CtrlClientSerializerBuilder completionBuilder(Long apiCallId);

    public interface CtrlClientSerializerBuilder extends CommonSerializerBuilder
    {
        /*
         * Controller -> Client
         */
        CtrlClientSerializerBuilder nodeList(List<Node.NodeApi> nodes);
        CtrlClientSerializerBuilder storPoolDfnList(List<StorPoolDfnApi> storPoolDfns);
        CtrlClientSerializerBuilder storPoolList(List<StorPool.StorPoolApi> storPools);
        CtrlClientSerializerBuilder resourceDfnList(List<ResourceDefinition.RscDfnApi> rscDfns);
        CtrlClientSerializerBuilder resourceList(
            List<Resource.RscApi> rscs, Map<NodeName, SatelliteState> satelliteStates);
        CtrlClientSerializerBuilder resourceConnList(List<ResourceConnection.RscConnApi> rscConns);
        CtrlClientSerializerBuilder snapshotDfnList(List<SnapshotDefinition.SnapshotDfnListItemApi> snapshotDfns);

        CtrlClientSerializerBuilder apiVersion(long features, String controllerInfo);

        CtrlClientSerializerBuilder ctrlCfgSingleProp(String namespace, String key, String value);
        CtrlClientSerializerBuilder ctrlCfgProps(Map<String, String> map);

        CtrlClientSerializerBuilder maxVlmSizeCandidateList(List<MaxVlmSizeCandidatePojo> candidates);
        CtrlClientSerializerBuilder keyValueStoreListProps(Map<String, String> listKvsProps);
        CtrlClientSerializerBuilder keyValueStoreList(Set<KeyValueStore.KvsApi> listKvs);
    }
}
