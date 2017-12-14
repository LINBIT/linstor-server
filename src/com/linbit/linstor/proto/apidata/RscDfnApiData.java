package com.linbit.linstor.proto.apidata;

import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.VolumeDefinition;
import com.linbit.linstor.api.protobuf.BaseProtoApiCall;
import com.linbit.linstor.proto.LinStorMapEntryOuterClass;
import com.linbit.linstor.proto.RscDfnOuterClass;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 *
 * @author rpeinthor
 */
public class RscDfnApiData implements ResourceDefinition.RscDfnApi {
    private RscDfnOuterClass.RscDfn rscDfn;

    public RscDfnApiData(RscDfnOuterClass.RscDfn refRscDfn)
    {
        rscDfn = refRscDfn;
    }

    @Override
    public UUID getUuid() {
        UUID uuid = null;
        if(rscDfn.hasUuid())
        {
            uuid = UUID.fromString(rscDfn.getUuid());
        }
        return uuid;
    }

    @Override
    public String getResourceName() {
        return rscDfn.getRscName();
    }

    @Override
    public int getPort() {
        return rscDfn.getRscPort();
    }

    @Override
    public String getSecret() {
        return rscDfn.getRscSecret();
    }

    @Override
    public long getFlags() {
        return rscDfn.getRscFlags();
    }

    @Override
    public Map<String, String> getProps() {
        Map<String, String> ret = new HashMap<>();
        for (LinStorMapEntryOuterClass.LinStorMapEntry entry : rscDfn.getRscPropsList())
        {
            ret.put(entry.getKey(), entry.getValue());
        }
        return ret;
    }

    @Override
    public List<VolumeDefinition.VlmDfnApi> getVlmDfnList() {
        return VlmDfnApiData.toApiList(rscDfn.getVlmDfnsList());
    }

    public static RscDfnOuterClass.RscDfn fromRscDfnApi(final ResourceDefinition.RscDfnApi apiRscDfn)
    {
        RscDfnOuterClass.RscDfn.Builder rscDfnBuilder = RscDfnOuterClass.RscDfn.newBuilder();
        rscDfnBuilder.setRscName(apiRscDfn.getResourceName());
        rscDfnBuilder.setRscPort(apiRscDfn.getPort());
        rscDfnBuilder.setRscSecret(apiRscDfn.getSecret());
        rscDfnBuilder.setUuid(apiRscDfn.getUuid().toString());
        rscDfnBuilder.addAllVlmDfns(VlmDfnApiData.fromApiList(apiRscDfn.getVlmDfnList()));
        rscDfnBuilder.addAllRscProps(BaseProtoApiCall.fromMap(apiRscDfn.getProps()));

        return rscDfnBuilder.build();
    }

    public static List<RscDfnOuterClass.RscDfn> fromApiList(final List<ResourceDefinition.RscDfnApi> rscDfns)
    {
        ArrayList<RscDfnOuterClass.RscDfn> protoRscDfs = new ArrayList<>();
        for(ResourceDefinition.RscDfnApi rscDfnApi : rscDfns)
        {
            protoRscDfs.add(RscDfnApiData.fromRscDfnApi(rscDfnApi));
        }
        return protoRscDfs;
    }

}
