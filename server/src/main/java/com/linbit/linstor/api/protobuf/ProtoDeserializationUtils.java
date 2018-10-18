package com.linbit.linstor.api.protobuf;

import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.proto.LinStorMapEntryOuterClass;
import com.linbit.linstor.proto.MsgApiCallResponseOuterClass;
import com.linbit.utils.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ProtoDeserializationUtils
{
    public static ApiCallRc.RcEntry parseApiCallRc(
        MsgApiCallResponseOuterClass.MsgApiCallResponse apiCallResponse,
        String messagePrefix
    )
    {
        ApiCallRcImpl.EntryBuilder entryBuilder = ApiCallRcImpl
            .entryBuilder(
                apiCallResponse.getRetCode(),
                messagePrefix + apiCallResponse.getMessage()
            );

        if (!StringUtils.isEmpty(apiCallResponse.getCause()))
        {
            entryBuilder.setCause(apiCallResponse.getCause());
        }
        if (!StringUtils.isEmpty(apiCallResponse.getCorrection()))
        {
            entryBuilder.setCorrection(apiCallResponse.getCorrection());
        }
        if (!StringUtils.isEmpty(apiCallResponse.getDetails()))
        {
            entryBuilder.setDetails(apiCallResponse.getDetails());
        }

        entryBuilder.putAllObjRefs(readLinStorMap(apiCallResponse.getObjRefsList()));

        return entryBuilder.build();
    }

    private static Map<String, String> readLinStorMap(List<LinStorMapEntryOuterClass.LinStorMapEntry> linStorMap)
    {
        return linStorMap.stream()
            .collect(Collectors.toMap(
                LinStorMapEntryOuterClass.LinStorMapEntry::getKey,
                LinStorMapEntryOuterClass.LinStorMapEntry::getValue
            ));
    }

    private ProtoDeserializationUtils()
    {
    }
}
