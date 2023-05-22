package com.linbit.linstor.api.protobuf;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiCallReactive;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.DeviceManager;
import com.linbit.linstor.core.apicallhandler.ResponseSerializer;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.io.IOException;
import java.io.InputStream;

import reactor.core.publisher.Flux;

@ProtobufApiCall(
    name = InternalApiConsts.API_CHANGED_CONTROLLER,
    description = "Called by the controller to indicate that a controller properties changed."
)
@Singleton
public class ChangedController implements ApiCallReactive
{
    private final DeviceManager deviceManager;
    private final ResponseSerializer responseSerializer;

    @Inject
    public ChangedController(
        DeviceManager deviceManagerRef,
        ResponseSerializer responseSerializerRef
    )
    {
        deviceManager = deviceManagerRef;
        responseSerializer = responseSerializerRef;
    }

    @Override
    public Flux<byte[]> executeReactive(InputStream msgDataIn)
        throws IOException
    {
        return deviceManager.getUpdateTracker()
            .updateController()
            .concatWith(Flux.just(ApiCallRcImpl.singleApiCallRc(ApiConsts.MASK_INFO, "Controller properties applied")))
            .transform(responseSerializer::transform);
    }
}
