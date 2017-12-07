package com.linbit.linstor.proto.apidata;

import com.google.protobuf.ByteString;
import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.StorPoolDefinition;
import com.linbit.linstor.api.protobuf.BaseProtoApiCall;
import com.linbit.linstor.proto.LinStorMapEntryOuterClass;
import com.linbit.linstor.proto.RscDfnOuterClass;
import com.linbit.linstor.proto.StorPoolDfnOuterClass;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 *
 * @author rpeinthor
 */
public class StorPoolDfnApiData implements StorPoolDefinition.StorPoolDfnApi {
    private StorPoolDfnOuterClass.StorPoolDfn storPoolDfn;

    public StorPoolDfnApiData(StorPoolDfnOuterClass.StorPoolDfn refStorPoolDfn)
    {
        storPoolDfn = refStorPoolDfn;
    }

    @Override
    public UUID getUuid() {
        UUID uuid = null;
        if(storPoolDfn.hasUuid())
        {
            uuid = UUID.nameUUIDFromBytes(storPoolDfn.getUuid().toByteArray());
        }
        return uuid;
    }

    @Override
    public String getName() {
        return storPoolDfn.getStorPoolName();
    }

    @Override
    public Map<String, String> getProps() {
        Map<String, String> ret = new HashMap<>();
        for (LinStorMapEntryOuterClass.LinStorMapEntry entry : storPoolDfn.getPropsList())
        {
            ret.put(entry.getKey(), entry.getValue());
        }
        return ret;
    }

    public static StorPoolDfnOuterClass.StorPoolDfn fromStorPoolDfnApi(final StorPoolDefinition.StorPoolDfnApi apiStorPoolDfn)
    {
        StorPoolDfnOuterClass.StorPoolDfn.Builder storPoolDfnBld = StorPoolDfnOuterClass.StorPoolDfn.newBuilder();
        storPoolDfnBld.setStorPoolName(apiStorPoolDfn.getName());
        storPoolDfnBld.setUuid(ByteString.copyFrom(apiStorPoolDfn.getUuid().toString().getBytes()));
        storPoolDfnBld.addAllProps(BaseProtoApiCall.fromMap(apiStorPoolDfn.getProps()));

        return storPoolDfnBld.build();
    }
}
