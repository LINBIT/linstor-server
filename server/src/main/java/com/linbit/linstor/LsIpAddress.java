package com.linbit.linstor;

import static com.google.common.net.InetAddresses.isInetAddress;

public class LsIpAddress
{
    private final String addr;

    // Naming convention exception: Usual capitalization of IP address types
    @SuppressWarnings("checkstyle:constantname")
    public enum AddrType
    {
        IPv4,
        IPv6
    }

    /**
     * addr has to be IPv4 or IPv6
     */
    public LsIpAddress(String address)
    {
        isInetAddress(address.trim());

        addr = address.trim().toUpperCase();
    }

    public String getAddress()
    {
        return addr;
    }

    public AddrType getAddressType()
    {
        AddrType ipType = AddrType.IPv4;
        if (addr.contains(":"))
        {
            ipType = AddrType.IPv6;
        }
        return ipType;
    }
}
