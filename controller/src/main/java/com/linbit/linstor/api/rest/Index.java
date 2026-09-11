package com.linbit.linstor.api.rest;

import com.linbit.linstor.api.rest.v1.serializer.JsonGenTypes;
import com.linbit.linstor.core.cfg.CtrlConfig;

import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.core.Response;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;

@Path("")
public class Index
{
    private final CtrlConfig linstorConfig;
    private static final String INDEX_CONTENT = "<html><title>LINSTOR REST server</title>" +
            "<body><a href=\"https://app.swaggerhub.com/apis-docs/Linstor/Linstor/" + JsonGenTypes.REST_API_VERSION +
            "\">API Documentation</a><p>You can install the GUI by running " +
            "<code>apt install linstor-gui</code> or <code>dnf install linstor-gui</code>.</p></body></html>";

    @Inject
    public Index(
        CtrlConfig linstorConfigRef
    )
    {
        linstorConfig = linstorConfigRef;
    }

    @GET
    public Response index()
    {
        // if webUiDirectory exists, we will redirect to it, if not display api doc link and ui install hint
        final Response.ResponseBuilder respBuilder = Files.exists(Paths.get(linstorConfig.getWebUiDirectory())) ?
            Response.seeOther(URI.create("/ui/")) :  Response.status(Response.Status.OK).entity(INDEX_CONTENT);
        return respBuilder.build();
    }
}
