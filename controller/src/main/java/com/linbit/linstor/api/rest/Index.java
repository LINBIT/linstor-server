package com.linbit.linstor.api.rest;

import com.linbit.linstor.api.rest.v1.serializer.JsonGenTypes;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.Response;

@Path("")
public class Index {
    private static final String INDEX_CONTENT = "<html><title>Linstor REST server</title>" +
            "<body><a href=\"https://app.swaggerhub.com/apis-docs/Linstor/Linstor/" + JsonGenTypes.REST_API_VERSION +
            "\">Documentation</a></body></html>";

    @GET
    public Response index()
    {
        return Response.status(Response.Status.OK).entity(INDEX_CONTENT).build();
    }
}
