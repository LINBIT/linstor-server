package com.linbit.linstor;

import com.linbit.Checks;
import com.linbit.InvalidIpAddressException;

public class DmIpAddress
{
    private final String addr;

    /**
     * addr has to be IPv4 or IPv6
     * @throws InvalidIpAddressException
     */
    public DmIpAddress(String addr) throws InvalidIpAddressException
    {
        Checks.ipAddrCheck(addr);

        this.addr = addr.toUpperCase();
    }

    public String getAddress()
    {
        return addr;
    }
}
