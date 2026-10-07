package com.linbit.linstor.api.rest.v1.config;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class AuthenticationFilterTest
{
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
