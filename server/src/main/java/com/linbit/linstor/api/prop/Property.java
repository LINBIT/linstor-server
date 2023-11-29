package com.linbit.linstor.api.prop;

import com.linbit.ImplementationError;
import com.linbit.linstor.propscon.PropsContainer;

public interface Property
{
    enum PropertyType
    {
        REGEX,
        SYMBOL("handler"),
        // 'yes' or 'no'
        BOOLEAN,
        // 'true' or 'false' (also accepts 'yes' or 'no' as input)
        BOOLEAN_TRUE_FALSE,
        RANGE("numeric"),
        RANGE_FLOAT,
        STRING,
        NUMERIC_OR_SYMBOL,
        LONG;

        private String[] alternativeNames;

        PropertyType(String... alternativeNamesRef)
        {
            alternativeNames = alternativeNamesRef;
        }

        public static PropertyType valueOfIgnoreCase(String typeRef)
        {
            String type = typeRef.replace("-", "_");
            PropertyType ret = null;
            for (PropertyType propType : values())
            {
                if (propType.name().equalsIgnoreCase(type))
                {
                    ret = propType;
                    break;
                }
                for (String altName : propType.alternativeNames)
                {
                    if (altName.equalsIgnoreCase(type))
                    {
                        ret = propType;
                        break;
                    }
                }
            }
            return ret;
        }
    }

    /**
     * Returns the name of the property. This is NOT the key, just an internal
     * name of the property for referencing this property
     */
    String getName();

    /**
     * Returns the full qualified key starting from the root {@link PropsContainer}
     */
    String getKey();

    /**
     * Returns the value the user input has to match later
     */
    String getValue();

    /**
     * If true and the "user value" does not match the property an {@link ImplementationError}
     * will be thrown
     */
    boolean isInternal();

    /**
     * Describes the current property. Mostly used by the client
     */
    String getInfo();

    /**
     * Returns true if the value matches the configured property
     */
    boolean isValid(String value);

    /**
     * Describes the unit used by the property
     */
    String getUnit();

    /**
     * Returns information about the default value of the property
     */
    String getDflt();

    /**
     * Returns the type of the property
     */
    PropertyType getType();

    /**
     * Returns a message that describes what kind of input this property expects
     * e.g. "The value must match (0-60) minutes."
     */
    String getErrorMsg();

    /**
     * Converts the value to its canonical form
     */
    default String normalize(String value)
    {
        return value;
    }

    /**
     * Describes this property. It should contain information about type, rule_name, and validation
     * pattern
     */
    default String getDescription()
    {
        return this.getClass().getSimpleName() + ", " +
            "name: '" + getName() + "', " +
            "pattern: '" + getValue() + "'";
    }
}
