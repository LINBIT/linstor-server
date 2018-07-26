package com.linbit;

/**
 * Valid name of a SystemService
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class ServiceName extends GenericName
{
    public static final int MIN_LENGTH = 3;
    public static final int MAX_LENGTH = 32;

    public static final byte[] VALID_CHARS = {'_'};
    public static final byte[] VALID_INNER_CHARS = {'_', '-'};

    public ServiceName(String svcName) throws InvalidNameException
    {
        super(svcName);
        Checks.nameCheck(svcName, MIN_LENGTH, MAX_LENGTH, VALID_CHARS, VALID_INNER_CHARS);
    }
}
