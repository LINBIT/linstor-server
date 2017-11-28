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
    public LsIpAddress(String addr) throws InvalidIpAddressException
    {
        Checks.ipAddrCheck(addr);

        this.addr = addr.toUpperCase();
    }

    public String getAddress()
    {
        return addr;
    }
}
