package com.linbit.linstor.api.prop;

import com.linbit.linstor.annotation.Nullable;

public class LongProperty extends GenericProperty
{

    public LongProperty(
        String nameRef,
        String keyRef,
        boolean internalRef,
        @Nullable String infoRef,
        @Nullable String unitRef,
        @Nullable String dfltRef
    )
    {
        super(nameRef, keyRef, internalRef, infoRef, unitRef, dfltRef);
    }

    @Override
    public boolean isValid(String value)
    {
        boolean validFlag = false;
        try
        {
            Long.parseLong(value);
            validFlag = true;
        }
        catch (NumberFormatException ignored)
        {
        }
        return validFlag;
    }

    @Override
    public PropertyType getType()
    {
        return Property.PropertyType.LONG;
    }

    @Override
    public String getValue()
    {
        return "Any signed 64 bit integer";
    }

    @Override
    public String getErrorMsg()
    {
        String errorMsg;
        if (super.getUnit() == null)
        {
            errorMsg = "This value has to be of type Long.";
        }
        else
        {
            errorMsg = "This value contains " + getUnit() + " and has to be of type Long.";
        }
        return errorMsg;
    }
}
