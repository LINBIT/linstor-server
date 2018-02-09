package com.linbit.linstor;

import com.linbit.Checks;
import com.linbit.InvalidIpAddressException;

public class LsIpAddress
{
    private final String addr;

    /**
     * addr has to be IPv4 or IPv6
     * @throws InvalidIpAddressException
     */
    public LsIpAddress(String address) throws InvalidIpAddressException
    {
        Checks.ipAddrCheck(address);

        addr = address.toUpperCase();
    }

    public String getAddress()
    {
        return addr;
    }
}
