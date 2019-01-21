package com.linbit.linstor.propscon;

/**
 * Thrown to indicate an invalid PropsContainer value
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class InvalidValueException extends Exception
{
    private static final long serialVersionUID = 2696642939160868138L;

    public final String key;
    public final String value;

    public InvalidValueException(String keyRef, String valueRef, String errMsg)
    {
        super(errMsg);
        key = keyRef;
        value = valueRef;
    }

    public InvalidValueException(String keyRef, String valueRef)
    {
        key = keyRef;
        value = valueRef;
    }
}
