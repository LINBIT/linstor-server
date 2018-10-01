package com.linbit.linstor.api.protobuf;

import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.proto.LinStorMapEntryOuterClass;
import com.linbit.linstor.proto.MsgApiCallResponseOuterClass;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ProtoDeserializationUtils
{
    public static ApiCallRc.RcEntry parseApiCallRc(
        MsgApiCallResponseOuterClass.MsgApiCallResponse apiCallResponse,
        String messagePrefix
    )
        throws IOException
    {
        return ApiCallRcImpl
            .entryBuilder(
                apiCallResponse.getRetCode(),
                messagePrefix + apiCallResponse.getMessage()
            )
            .setCause(apiCallResponse.getCause())
            .setCorrection(apiCallResponse.getCorrection())
            .setDetails(apiCallResponse.getDetails())
            .putAllObjRefs(readLinStorMap(apiCallResponse.getObjRefsList()))
            .build();
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
