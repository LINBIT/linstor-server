package com.linbit.linstor.api.rest.v1.config;

import com.linbit.linstor.LinStorRuntimeException;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.rest.v1.utils.ApiCallRcRestUtils;
import com.linbit.linstor.core.objects.AuthToken;
import com.linbit.linstor.core.apicallhandler.controller.CtrlAuthHandler;
import com.linbit.linstor.core.repository.AuthTokenRepository;
import com.linbit.linstor.core.repository.SystemConfRepository;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;

import javax.annotation.Priority;
import javax.inject.Inject;
import javax.ws.rs.Priorities;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.Provider;

import java.io.IOException;
import java.time.Instant;

@Provider
@Priority(Priorities.AUTHENTICATION)
public class AuthenticationFilter implements ContainerRequestFilter
{
    private static final String AUTH_SCHEME_BEARER = "Bearer";
    public static final String REMOTE_ADDR_PROPERTY = "linstor.remote.addr";
    private static final String[] ALLOWED_PATHS = {
        "",
        "health",
        "ui"
    };

    private final AuthTokenRepository authTokenRepository;
    private final SystemConfRepository systemConfRepository;
    private final AccessContext sysCtx;
    private final ErrorReporter errorReporter;

    @Inject
    public AuthenticationFilter(
        AuthTokenRepository authTokenRepositoryRef,
        SystemConfRepository systemConfRepositoryRef,
        @SystemContext AccessContext sysCtxRef,
        ErrorReporter errorReporterRef
    )
    {
        authTokenRepository = authTokenRepositoryRef;
        systemConfRepository = systemConfRepositoryRef;
        sysCtx = sysCtxRef;
        errorReporter = errorReporterRef;
    }

    @Override
    public void filter(ContainerRequestContext requestContext) throws IOException
    {
        // Health endpoint should always be accessible without authentication
        String path = requestContext.getUriInfo().getPath();
        if (isAllowedPath(path))
        {
            return;
        }

        // Only enforce authentication if token auth is enabled and tokens exist
        if (!isTokenAuthEnabled())
        {
            return;
        }

        // Get the Authorization header from the request
        @Nullable String authorizationHeader =
            requestContext.getHeaderString(HttpHeaders.AUTHORIZATION);

        // Validate the Authorization header
        if (!isTokenBasedAuthentication(authorizationHeader))
        {
            abortWithUnauthorized(requestContext);
            return;
        }

        // Extract the token from the Authorization header
        String token = authorizationHeader
            .substring(AUTH_SCHEME_BEARER.length()).trim();

        try
        {
            // Validate the token against the client's remote IP
            @Nullable String remoteAddr = (String) requestContext.getProperty(REMOTE_ADDR_PROPERTY);
            validateToken(token, remoteAddr);
        }
        catch (Exception e)
        {
            abortWithUnauthorized(requestContext);
        }
    }

    @SuppressWarnings("DescendantToken")
    private boolean isAllowedPath(String path)
    {
        for (var allowedPath : ALLOWED_PATHS)
        {
            if (allowedPath.equals(path) || (allowedPath + "/").equals(path))
            {
                return true;
            }
        }
        return false;
    }

    private boolean isTokenAuthEnabled()
    {
        // Token auth is enabled only if the property is set to true AND there is at least one valid token
        if (!hasValidTokens())
        {
            return false;
        }

        try
        {
            @Nullable String tokenAuthEnabled = systemConfRepository
                .getCtrlConfForView(sysCtx)
                .getProp(ApiConsts.KEY_TOKEN_AUTH_ENABLED, ApiConsts.NAMESPC_AUTH);
            return Boolean.parseBoolean(tokenAuthEnabled);
        }
        catch (AccessDeniedException exc)
        {
            errorReporter.reportError(exc);
            return false;
        }
    }

    private boolean hasValidTokens()
    {
        return authTokenRepository.getMapForView().values().stream()
            .anyMatch(token -> isTokenValid(token, Instant.now()));
    }

    private boolean isTokenBasedAuthentication(@Nullable String authorizationHeader)
    {
        // Check if the Authorization header is valid
        // It must not be null and must be prefixed with "Bearer" plus a whitespace
        // The authentication scheme comparison must be case-insensitive
        return authorizationHeader != null && authorizationHeader.toLowerCase()
            .startsWith(AUTH_SCHEME_BEARER.toLowerCase() + " ");
    }

    private void abortWithUnauthorized(ContainerRequestContext requestContext)
    {
        // Abort the filter chain with a 401 status code response
        // The WWW-Authenticate header is sent along with the response
        ApiCallRcImpl apiCallRc = ApiCallRcImpl.singleApiCallRc(
            ApiConsts.FAIL_ACC_DENIED_COMMAND,
            "Unauthorized: token invalid or not provided");
        requestContext.abortWith(
            Response.status(Response.Status.UNAUTHORIZED)
                .type(MediaType.APPLICATION_JSON)
                .header(HttpHeaders.WWW_AUTHENTICATE,
                    AUTH_SCHEME_BEARER + " realm=\"linstor-controller\"")
                .entity(ApiCallRcRestUtils.toJSON(errorReporter, apiCallRc))
                .build());
    }

    private void validateToken(String tokenStr, @Nullable String remoteAddr)
    {
        String tokenHash = CtrlAuthHandler.sha256Hex(tokenStr);
        @Nullable AuthToken authToken = authTokenRepository.findByTokenHash(tokenHash);

        if (authToken == null)
        {
            throw new LinStorRuntimeException("Token not found.");
        }

        @Nullable String validationError = getTokenValidationError(authToken, Instant.now());
        if (validationError != null)
        {
            throw new LinStorRuntimeException(validationError);
        }

        @Nullable String ipFilter = authToken.getIPFilter();
        // if the remoteAddr is null, we should fail as then we can't be sure the ip is matching the filter
        if (ipFilter != null && !ipFilter.isEmpty() && !ipFilter.equals(remoteAddr))
        {
            throw new LinStorRuntimeException("Token IP filter mismatch.");
        }
    }

    /**
     * Checks if a token is valid (active, not deleted, not expired).
     * Used by hasValidTokens() to check if at least one valid token exists.
     */
    private boolean isTokenValid(AuthToken token, Instant now)
    {
        return getTokenValidationError(token, now) == null;
    }

    /**
     * Returns an error message if the token is invalid, or null if it's valid.
     */
    private @Nullable String getTokenValidationError(AuthToken token, Instant now)
    {
        if (token.getDeletedAt() != null)
        {
            return "Token has been revoked.";
        }

        if (!token.isActive())
        {
            return "Token is disabled.";
        }

        @Nullable Instant expiresAt = token.getExpiresAt();
        if (expiresAt != null && expiresAt.isBefore(now))
        {
            return "Token has expired.";
        }

        return null;
    }

}
