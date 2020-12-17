package com.linbit.linstor.api.prop;

public class StringProperty extends GenericProperty implements Property
{

    public StringProperty(
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
        return true;
    }

    @Override
    public PropertyType getType()
    {
        return Property.PropertyType.STRING;
    }

}
