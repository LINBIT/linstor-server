package com.linbit.linstor.api.rest;

import com.linbit.ServiceName;
import com.linbit.SystemService;
import com.linbit.linstor.ControllerDatabase;
import com.linbit.linstor.dbdrivers.DatabaseException;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.Response;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Path("health")
public class Health
{

    private final ControllerDatabase ctrlDb;
    private final Map<ServiceName, SystemService> systemServiceMap;

    @Inject
    public Health(
        ControllerDatabase ctrlDbRef,
        Map<ServiceName, SystemService> systemServiceMapRef
    )
    {
        ctrlDb = ctrlDbRef;
        systemServiceMap = systemServiceMapRef;
    }

    @GET
    public Response health()
    {
        Response response;
        try
        {
            ctrlDb.checkHealth();
            List<String> notRunning = systemServiceMap.values().stream()
                    .filter(service -> !service.isStarted())
                    .map(service -> service.getServiceName().getDisplayName())
                    .collect(Collectors.toList());
            if (notRunning.isEmpty())
            {
                response = Response.status(Response.Status.OK).build();
            }
            else
            {
                final String errorMsg = "Services not running: " +
                        String.join(",", notRunning);
                response = Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(errorMsg).build();
            }
        }
        catch (DatabaseException exc)
        {
            final String errorMsg = "Failed to connect to database: " + exc.getMessage();
            response = Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(errorMsg).build();
        }

        return response;
    }
}
