package com.linbit.linstor.api.rest.v1;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.rest.v1.serializer.JsonSpaceTracking;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.spacetracking.SpaceTrackingService;
import java.security.DigestException;
import java.security.NoSuchAlgorithmException;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.glassfish.grizzly.http.server.Request;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.rest.v1.utils.ApiCallRcRestUtils;
import javax.inject.Inject;

@Path("v1/space-report")
@Produces(MediaType.APPLICATION_JSON)
public class SpaceTracking
{
    private final SpaceTrackingService spcTrkSvc;
    private final ObjectMapper objectMapper;

    @Inject
    public SpaceTracking(SpaceTrackingService spcTrkSvcRef)
    {
        spcTrkSvc = spcTrkSvcRef;
        objectMapper = new ObjectMapper();
    }

    @GET
    public Response querySpaceReport(@Context Request request)
        throws JsonProcessingException
    {
        Response rsp;
        try
        {
            JsonSpaceTracking.SpaceReport jsonReportText = new JsonSpaceTracking.SpaceReport();
            if (spcTrkSvc != null)
            {
                jsonReportText.reportText = spcTrkSvc.querySpaceReport(null);
            }
            else
            {
                jsonReportText.reportText = "The SpaceTracking service is not installed.";
            }
            rsp = Response.status(Response.Status.OK).entity(
                objectMapper.writeValueAsString(jsonReportText)
            ).build();
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
            rsp = Response.status(Response.Status.SERVICE_UNAVAILABLE).entity(
                ApiCallRcRestUtils.toJSON(apiCallRc)
            ).type(MediaType.APPLICATION_JSON).build();
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
            rsp = Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(
                ApiCallRcRestUtils.toJSON(apiCallRc)
            ).type(MediaType.APPLICATION_JSON).build();
        }
        return rsp;
    }
}
