package com.linbit.linstor.api.rest.v1.config;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.apicallhandler.controller.CtrlAuthHandler;
import com.linbit.linstor.core.objects.AuthToken;
import com.linbit.linstor.core.repository.AuthTokenRepository;
import com.linbit.linstor.core.repository.SystemConfRepository;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.ReadOnlyProps;

import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.UriInfo;

import java.time.Instant;

import org.junit.Test;
import org.mockito.ArgumentCaptor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AuthenticationFilterTest
{
    private static final String TOKEN = "secret";

    private final AuthTokenRepository authTokenRepository = mock(AuthTokenRepository.class);
    private final SystemConfRepository systemConfRepository = mock(SystemConfRepository.class);
    private final ReadOnlyProps ctrlProps = mock(ReadOnlyProps.class);
    private final AuthenticationFilter filter = new AuthenticationFilter(
        authTokenRepository,
        systemConfRepository,
        mock(ErrorReporter.class)
    );

    private ContainerRequestContext request(String path, @Nullable String token) throws Exception
    {
        when(systemConfRepository.getCtrlConfForView()).thenReturn(ctrlProps);
        UriInfo uriInfo = mock(UriInfo.class);
        when(uriInfo.getPath()).thenReturn(path);
        ContainerRequestContext ctx = mock(ContainerRequestContext.class);
        when(ctx.getUriInfo()).thenReturn(uriInfo);
        if (token != null)
        {
            when(ctx.getHeaderString(HttpHeaders.AUTHORIZATION)).thenReturn("Bearer " + token);
        }
        return ctx;
    }

    private void enableTokenAuth() throws Exception
    {
        when(ctrlProps.getProp(ApiConsts.KEY_TOKEN_AUTH_ENABLED, ApiConsts.NAMESPC_AUTH)).thenReturn("true");
    }

    private void assertUnauthorized(ContainerRequestContext ctx)
    {
        ArgumentCaptor<Response> response = ArgumentCaptor.forClass(Response.class);
        verify(ctx).abortWith(response.capture());
        assertThat(response.getValue().getStatus()).isEqualTo(Response.Status.UNAUTHORIZED.getStatusCode());
    }

    @Test
    public void disabledTokenAuthAllowsRequestsWithoutToken() throws Exception
    {
        ContainerRequestContext ctx = request("v1/nodes", null);
        filter.filter(ctx);
        verify(ctx, never()).abortWith(any());
    }

    @Test
    public void enabledTokenAuthWithoutAnyTokenLocksOut() throws Exception
    {
        // no token exists at all: token auth must not disable itself
        enableTokenAuth();

        ContainerRequestContext withoutToken = request("v1/nodes", null);
        filter.filter(withoutToken);
        assertUnauthorized(withoutToken);

        ContainerRequestContext withUnknownToken = request("v1/nodes", TOKEN);
        filter.filter(withUnknownToken);
        assertUnauthorized(withUnknownToken);
    }

    @Test
    public void enabledTokenAuthWithOnlyRevokedTokenLocksOut() throws Exception
    {
        enableTokenAuth();
        AuthToken revoked = mock(AuthToken.class);
        when(revoked.getDeletedAt()).thenReturn(Instant.now());
        when(authTokenRepository.findByTokenHash(CtrlAuthHandler.sha256Hex(TOKEN))).thenReturn(revoked);

        ContainerRequestContext ctx = request("v1/nodes", TOKEN);
        filter.filter(ctx);
        assertUnauthorized(ctx);
    }

    @Test
    public void enabledTokenAuthAcceptsValidToken() throws Exception
    {
        enableTokenAuth();
        AuthToken valid = mock(AuthToken.class);
        when(valid.isActive()).thenReturn(true);
        when(authTokenRepository.findByTokenHash(CtrlAuthHandler.sha256Hex(TOKEN))).thenReturn(valid);

        ContainerRequestContext ctx = request("v1/nodes", TOKEN);
        filter.filter(ctx);
        verify(ctx, never()).abortWith(any());
    }

    @Test
    public void healthStaysAccessibleWhenLockedOut() throws Exception
    {
        enableTokenAuth();
        ContainerRequestContext ctx = request("health", null);
        filter.filter(ctx);
        verify(ctx, never()).abortWith(any());
    }

    @Test
    public void ipv6FilterMatchesAnyNotationOfTheClientAddress()
    {
        // the client address as Grizzly reports it: expanded, lowercase
        String remote = "fd00:226:0:0:0:0:0:12";
        assertThat(AuthenticationFilter.ipFilterMatches("fd00:226::12", remote)).isTrue();
        // satellite tokens use the stored node address, which LINSTOR keeps in uppercase
        assertThat(AuthenticationFilter.ipFilterMatches("FD00:226::12", remote)).isTrue();
        assertThat(AuthenticationFilter.ipFilterMatches("fd00:226:0:0:0:0:0:12", remote)).isTrue();
        assertThat(AuthenticationFilter.ipFilterMatches("::1", "0:0:0:0:0:0:0:1")).isTrue();
    }

    @Test
    public void ipv6FilterRejectsOtherAddresses()
    {
        assertThat(AuthenticationFilter.ipFilterMatches("fd00:226::12", "fd00:226:0:0:0:0:0:13")).isFalse();
    }

    @Test
    public void ipv4Filter()
    {
        assertThat(AuthenticationFilter.ipFilterMatches("192.168.123.237", "192.168.123.237")).isTrue();
        assertThat(AuthenticationFilter.ipFilterMatches("192.168.123.237", "192.168.123.238")).isFalse();
        // an IPv4 client seen through a dual-stack socket
        assertThat(AuthenticationFilter.ipFilterMatches("::ffff:192.168.123.237", "192.168.123.237")).isTrue();
    }

    @Test
    public void unknownOrInvalidClientAddressNeverMatches()
    {
        assertThat(AuthenticationFilter.ipFilterMatches("192.168.123.237", null)).isFalse();
        assertThat(AuthenticationFilter.ipFilterMatches("192.168.123.237", "")).isFalse();
        assertThat(AuthenticationFilter.ipFilterMatches("192.168.123.237", "not-an-address")).isFalse();
        assertThat(AuthenticationFilter.ipFilterMatches("fd00:226::12", "fd00:226::12::1")).isFalse();
    }
}
