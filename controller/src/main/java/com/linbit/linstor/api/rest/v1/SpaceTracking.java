package com.linbit.linstor.api.rest.v1;

import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.rest.v1.serializer.JsonSpaceTracking;
import com.linbit.linstor.api.rest.v1.utils.ApiCallRcRestUtils;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.spacetracking.SpaceTrackingService;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.security.DigestException;
import java.security.NoSuchAlgorithmException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.glassfish.grizzly.http.server.Request;

@Path("v1/space-report")
@Produces(MediaType.APPLICATION_JSON)
public class SpaceTracking
{
    private final Provider<SpaceTrackingService> spcTrkSvc;
    private final ObjectMapper objectMapper;
    private final RequestHelper requestHelper;

    @Inject
    public SpaceTracking(RequestHelper requestHelperRef, Provider<SpaceTrackingService> spcTrkSvcRef)
    {
        requestHelper = requestHelperRef;
        spcTrkSvc = spcTrkSvcRef;
        objectMapper = new ObjectMapper();
    }

    @GET
    public Response querySpaceReport(@Context Request request)
        throws JsonProcessingException
    {
        return requestHelper.doInScope(
            ApiConsts.API_LST_CTRL_PROPS,
            request,
            () ->
            {
                Response rsp;
                try
                    {
                    JsonSpaceTracking.SpaceReport jsonReportText = new JsonSpaceTracking.SpaceReport();
                    if (spcTrkSvc != null && spcTrkSvc.get() != null)
                    {
                        jsonReportText.reportText = spcTrkSvc.get().querySpaceReport(null);
                    }
                    else
                    {
                        jsonReportText.reportText = "The SpaceTracking service is not installed.";
                    }
                    rsp = Response.status(Response.Status.OK)
                        .entity(
                            objectMapper.writeValueAsString(jsonReportText)
                        )
                        .build();
                    }
                catch (DatabaseException dbExc)
                {
                    ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
                    apiCallRc.addEntry(
                        ApiCallRcImpl.simpleEntry(
                            ApiConsts.FAIL_SQL,
                            "Generation of the space report failed: Database error"
                        )
                    );
                    rsp = Response.status(Response.Status.SERVICE_UNAVAILABLE)
                        .entity(
                            ApiCallRcRestUtils.toJSON(apiCallRc)
                        )
                        .type(MediaType.APPLICATION_JSON)
                        .build();
                }
                catch (NoSuchAlgorithmException | DigestException cryptoExc)
                {
                    ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
                    apiCallRc.addEntry(
                        ApiCallRcImpl.simpleEntry(
                            ApiConsts.FAIL_CRYPT_INIT,
                            "Generation of the space report failed: Cryptographic hash function not available"
                        )
                    );
                    rsp = Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                        .entity(
                            ApiCallRcRestUtils.toJSON(apiCallRc)
                        )
                        .type(MediaType.APPLICATION_JSON)
                        .build();
                }
                return rsp;
            },
            true
        );
    }
}
