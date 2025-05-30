package com.linbit.linstor.api.rest.v1.config;

import java.io.IOException;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import javax.ws.rs.ext.Provider;

@Provider
public class CORSFilter implements ContainerResponseFilter
{
    @Override
    public void filter(ContainerRequestContext request,
                       ContainerResponseContext response) throws IOException
    {
        // Remove the dangerous combination of "*" origin with credentials
        String allowedOrigin = getCORSConfiguration().getAllowOrigin();
        response.getHeaders().add("Access-Control-Allow-Origin", allowedOrigin);
        response.getHeaders().add("Access-Control-Allow-Headers",
            "origin, content-type, accept, authorization");
        // Only add credentials if origin is specifically configured
        if (!"*".equals(allowedOrigin)) {
            response.getHeaders().add("Access-Control-Allow-Credentials", "true");
        }
        response.getHeaders().add("Access-Control-Allow-Methods",
            "GET, POST, PUT, DELETE, OPTIONS, HEAD");
    }
}

// Helper method to get CORS configuration
private CORSConfiguration getCORSConfiguration() {
    // This should return your application's CORS configuration
    // with properly validated allowed origins
    return corsConfig;
}