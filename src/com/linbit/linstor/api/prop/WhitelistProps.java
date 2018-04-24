package com.linbit.linstor.api.prop;

import com.linbit.linstor.core.AbsApiCallHandler.LinStorObject;
import com.linbit.linstor.logging.ErrorReporter;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class WhitelistProps
{
    private ErrorReporter errorReporter;
    private Map<LinStorObject, Map<String, Property>> rules;

    @Inject
    public WhitelistProps(ErrorReporter errorReporterRef)
    {
        errorReporter = errorReporterRef;

        List<Property> props = GeneratedPropertyRules.getWhitelistedProperties();

        Map<String, Property> propsByName = props.stream().collect(
            Collectors.toMap(Property::getName, Function.identity())
        );

        Map<LinStorObject, List<String>> rulesStr = GeneratedPropertyRules.getWhitelistedRules();

        rules = new HashMap<>();
        for (Entry<LinStorObject, List<String>> ruleEntry : rulesStr.entrySet())
        {
            Map<String, Property> rulesPropMap = new HashMap<>();
            for (String ruleProp : ruleEntry.getValue())
            {
                Property property = propsByName.get(ruleProp);
                rulesPropMap.put(property.getKey(), property);
            }
            rules.put(ruleEntry.getKey(), rulesPropMap);
        }

        // init unused objects with an empty whitelist. avoid null pointer
        Arrays.stream(LinStorObject.values())
            .filter(obj -> !rules.containsKey(obj))
            .forEach(obj -> rules.put(obj, new HashMap<>()));
    }

    public boolean isAllowed(LinStorObject lsObj, String key, String value, boolean log)
    {
        boolean validProp = false;
        Property rule = rules.get(lsObj).get(key);
        if (rule != null)
        {
            validProp = rule.isValid(value);
            if (!validProp && log)
            {
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
            if (log)
            {
                errorReporter.logWarning("Ignoring unknown property key: %s", key);
            }
        }
        return validProp;
    }

    public Map<String, String> sanitize(LinStorObject lsObj, Map<String, String> props)
    {
        return props.entrySet().stream()
            .filter(entry -> isAllowed(lsObj, entry.getKey(), entry.getValue(), false))
            .collect(Collectors.toMap(entry -> entry.getKey(), entry -> entry.getValue()));
    }

    public String getRuleValue(LinStorObject linstorObj, String key)
    {
        return rules.get(linstorObj).get(key).getValue();
    }

    public boolean isKeyKnown(LinStorObject linstorObj, String key)
    {
        return rules.get(linstorObj).get(key) != null;
    }
}
