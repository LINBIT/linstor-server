package com.linbit.linstor.api.rest.v1;

import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.rest.v1.serializer.JsonGenTypes;
import com.linbit.linstor.api.rest.v1.utils.ApiCallRcRestUtils;
import com.linbit.linstor.core.apicallhandler.controller.CtrlSnapshotShippingApiCallHandler;
import com.linbit.linstor.logging.ErrorReporter;

import javax.inject.Inject;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;

import java.io.IOException;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.glassfish.grizzly.http.server.Request;
import org.slf4j.MDC;
import reactor.core.publisher.Flux;

@Deprecated(forRemoval = true)
@Path("v1/resource-definitions/{rscName}/snapshot-shipping")
public class SnapshotShipping
{
    private final RequestHelper requestHelper;
    private final CtrlSnapshotShippingApiCallHandler ctrlSnapshotShippingApiCallHandler;

    private final ObjectMapper objectMapper;

    @Inject
    public SnapshotShipping(
        RequestHelper requestHelperRef,
        CtrlSnapshotShippingApiCallHandler ctrlSnapshotShippingApiCallHandlerRef
    )
    {
        requestHelper = requestHelperRef;
        ctrlSnapshotShippingApiCallHandler = ctrlSnapshotShippingApiCallHandlerRef;

        objectMapper = new ObjectMapper();
    }

    @POST
    public void shipSnapshot(
        @Context Request request,
        @Suspended final AsyncResponse asyncResponse,
        @PathParam("rscName") String rscName,
        String jsonData
    )
    {
        try (var ignore = MDC.putCloseable(ErrorReporter.LOGID, ErrorReporter.getNewLogId()))
        {
            JsonGenTypes.SnapshotShipping snapShipData = objectMapper.readValue(
                jsonData,
                JsonGenTypes.SnapshotShipping.class
            );
            Flux<ApiCallRc> flux = ctrlSnapshotShippingApiCallHandler.shipSnapshot(
                rscName,
                snapShipData.from_node,
                snapShipData.from_nic,
                snapShipData.to_node,
                snapShipData.to_nic,
                false
            );

            requestHelper.doFlux(
                ApiConsts.API_SHIP_SNAPSHOT,
                request,
                asyncResponse,
                ApiCallRcRestUtils.mapToMonoResponse(flux)
            );
        }
        catch (IOException ioExc)
        {
            ApiCallRcRestUtils.handleJsonParseException(ioExc, asyncResponse);
        }
    }
}
