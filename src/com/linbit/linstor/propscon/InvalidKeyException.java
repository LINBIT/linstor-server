package com.linbit.linstor.propscon;

/**
 * Thrown to indicate an invalid PropsContainer key
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class InvalidKeyException extends Exception
{
    private static final long serialVersionUID = -8876984313356533830L;

    public final String invalidKey;

    public InvalidKeyException(String invalidKeyValue)
    {
        invalidKey = invalidKeyValue;
    }
}
