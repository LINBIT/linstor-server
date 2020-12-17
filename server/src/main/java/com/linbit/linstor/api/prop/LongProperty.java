package com.linbit.linstor.api.prop;

public class LongProperty extends GenericProperty implements Property
{

    public LongProperty(
        String nameRef,
        String keyRef,
        boolean internalRef,
        String infoRef,
        String unitRef,
        String dfltRef
    )
    {
        super(nameRef, keyRef, internalRef, infoRef, unitRef, dfltRef);
    }

    @Override
    public boolean isValid(String value)
    {
        try
        {
            Long.parseLong(value);
            return true;
        }
        catch (NumberFormatException exc)
        {
            return false;
        }
    }

    @Override
    public PropertyType getType()
    {
        return Property.PropertyType.LONG;
    }
}
