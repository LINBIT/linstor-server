package com.linbit;

public class InvalidIpAddressException extends Exception
{
    private static final long serialVersionUID = 507890352031133664L;

    public InvalidIpAddressException(String str)
    {
        super(str);
    }
}
