package com.linbit.linstor.api.prop;

import com.linbit.ImplementationError;
import com.linbit.linstor.logging.ErrorReporter;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.stream.Collectors;

public class PropsWhitelist
{
    public enum LinStorObject
    {
        NODE,
        RESOURCE_DEFINITION,
        RESOURCE,
        VOLUME_DEFINITION,
        CONTROLLER,
        STORAGEPOOL,
        STORAGEPOOL_DEFINITION
    }

    private ErrorReporter errorReporter;
    private Map<LinStorObject, Map<String, Property>> rules;

    public PropsWhitelist(ErrorReporter errorReporterRef)
    {
        errorReporter = errorReporterRef;

        List<Property> props = GeneratedPropertyRules.getWhitelistedProperties();

        Map<String, Property> propsByName = props.stream().collect(
            Collectors.toMap(Property::getName, Function.identity())
        );

        Map<LinStorObject, List<String>> rulesStr = GeneratedPropertyRules.getWhitelistedRules();

        rules = new HashMap<>();
        for (Entry<LinStorObject, List<String>> ruleEntry: rulesStr.entrySet())
        {
            Map<String, Property> rulesPropMap = new HashMap<>();
            for (String ruleProp : ruleEntry.getValue())
            {
                Property property = propsByName.get(ruleProp);
                rulesPropMap.put(property.getKey(), property);
            }
            rules.put(ruleEntry.getKey(), rulesPropMap);
        }
    }

    public boolean isAllowed(LinStorObject lsObj, String key, String value)
    {
        boolean validProp = false;
        Property rule = rules.get(lsObj).get(key);
        if (rule != null)
        {
            validProp = rule.isValid(value);
            if (!validProp)
            {
                if (rule.isInternal())
                {
                    throw new ImplementationError(
                        "Value '%s' for key '%s' is not valid. Rule matches for: %s",
                        null
                    );
                }
                errorReporter.logWarning(
                    "Value '%s' for key '%s' is not valid. Rule matches for: %s",
                    value,
                    key,
                    rule.getDescription()
                );
            }
        }
        else
        {
            errorReporter.logWarning("Ignoring unknown property key: %s", key);
        }

        return validProp;
    }
}
