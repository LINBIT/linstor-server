package com.linbit.linstor.api.prop;

import com.linbit.linstor.annotation.Nullable;

public class StringProperty extends GenericProperty
{

    public StringProperty(
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
        return true;
    }

    @Override
    public PropertyType getType()
    {
        return Property.PropertyType.STRING;
    }

    @Override
    public String getValue()
    {
        return "Arbitrary string";
    }

    @Override
    public String getErrorMsg()
    {
        return "Congratulations, you found an easter-egg. " +
            "Now please report to the deveolpers how you actually got here, " +
            "or did you cheat and just read the source-code?";
    }

}
