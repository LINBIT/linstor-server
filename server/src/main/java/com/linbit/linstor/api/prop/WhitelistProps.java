package com.linbit.linstor.api.prop;

import com.linbit.linstor.LinStorRuntimeException;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.logging.ErrorReporter;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

@Singleton
public class WhitelistProps
{
    private final ErrorReporter errorReporter;
    private final Map<LinStorObject, Map<String, Property>> rules;
    private final Map<String, Property> dynamicProps = new HashMap<>();

    @Inject
    public WhitelistProps(ErrorReporter errorReporterRef)
    {
        rules = new TreeMap<>();
        errorReporter = errorReporterRef;

        List<Property> props = GeneratedPropertyRules.getWhitelistedProperties();

        Map<String, Property> propsByName = props.stream().collect(
            Collectors.toMap(Property::getName, Function.identity())
        );

        Map<LinStorObject, List<String>> rulesStr = GeneratedPropertyRules.getWhitelistedRules();
        for (Entry<LinStorObject, List<String>> ruleEntry : rulesStr.entrySet())
        {
            Map<String, Property> rulesPropMap = new TreeMap<>();
            for (String ruleProp : ruleEntry.getValue())
            {
                Property property = propsByName.get(ruleProp);
                if (property == null)
                {
                    throw new LinStorRuntimeException("rule not found: " + ruleProp);
                }
                rulesPropMap.put(property.getKey(), property);
            }
            rules.put(ruleEntry.getKey(), rulesPropMap);
        }

        // init unused objects with an empty whitelist. avoid null pointer
        Arrays.stream(LinStorObject.values())
            .filter(obj -> !rules.containsKey(obj))
            .forEach(obj -> rules.put(obj, new TreeMap<>()));
    }

    /**
     * This method should only be called while holding a reconfiguration.writeLock
     *
     * This instance should be singleton, as (beside reconfiguration) it is only read-only.
     * Modifying it while not holding the reconf.writeLock might cause {@link ConcurrentModificationException}s
     */
    public void reconfigure(LinStorObject... lsObjs)
    {
        for (LinStorObject lsObj : lsObjs)
        {
            rules.remove(lsObj);
        }
    }

    private Property createHandler(final String name)
    {
        return new PropertyBuilder()
            .name(name)
            .keyStr("DrbdOptions/Handlers/" + name)
            .internal("True")
            .type("string")
            .build();
    }

    public void overrideDrbdProperties()
    {
        // fix entry for resync-after, it is defined as range by drbdsetup-utils
        rules.values().forEach(objMap ->
            objMap.keySet().stream()
                .filter(key -> key.startsWith("DrbdOptions") && key.endsWith("resync-after"))
                .forEach(key ->
                    objMap.put(key,
                        new PropertyBuilder().name("resync-after").keyStr(key).internal("True").type("string").build()
                    )
                )
        );

        // add drbd handlers
        List<String> handlers = Arrays.asList(
            "after-resync-target",
            "before-resync-target",
            "before-resync-source",
            "out-of-sync",
            "quorum-lost",
            "fence-peer",
            "unfence-peer",
            "initial-split-brain",
            "local-io-error",
            "pri-lost",
            "pri-lost-after-sb",
            "pri-on-incon-degr",
            "split-brain"
        );
        Map<String, Property> ctrlProps = rules.get(LinStorObject.CTRL);
        for (String handler : handlers)
        {
            ctrlProps.put("DrbdOptions/Handlers/" + handler, createHandler(handler));
        }
    }

    /**
     * Merges the property rules found in the XML stream into the rules class member.
     *
     * @param overrideProp If false, property collisions will throw a LinStorRuntimeException.
     *     Otherwise the property rule from the XML stream will silently override the existing rule in the class member
     * @param xmlStream The XML stream to read the new rules from
     * @param keyPrefix The namespace of the property.
     * @param isDynamic if true all found properties from the given XML stream will be added in the dynamicProp class
     *     member
     * @param lsObjs The categories the new rule should be merged into.
     *
     */
    public void appendRules(
        boolean overrideProp,
        InputStream xmlStream,
        String keyPrefix,
        boolean isDynamic,
        LinStorObject... lsObjs
    )
    {
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        try
        {
            Document doc = dbf.newDocumentBuilder().parse(xmlStream);
            NodeList options = doc.getElementsByTagName("option");

            for (int idx = 0; idx < options.getLength(); idx++)
            {
                Element option = (Element) options.item(idx);
                NamedNodeMap optionArgs = option.getAttributes();
                String propName = optionArgs.getNamedItem("name").getTextContent();
                String propType = optionArgs.getNamedItem("type").getTextContent();

                if (!"set-defaults".equals(propName))
                {
                    // just ignore "set-defaults"

                    PropertyBuilder propBuilder = new PropertyBuilder();
                    propBuilder.name(propName);
                    propBuilder.type(propType);
                    String actualKey;
                    if (keyPrefix == null)
                    {
                        actualKey = propName;
                    }
                    else
                    {
                        actualKey = keyPrefix + "/" + propName;
                    }
                    propBuilder.keyStr(actualKey);

                    switch (propType.toLowerCase())
                    {
                        case "boolean":
                        case "boolean_true_false":
                        case "string":
                            // nothing to do
                            break;
                        case "handler":
                            propBuilder.values(extractEnumValues(option, "handler"));
                            break;
                        case "numeric":
                            propBuilder.min(getText(option, "min"));
                            propBuilder.max(getText(option, "max"));
                            break;
                        case "numeric-or-symbol":
                            propBuilder.values(extractEnumValues(option, "symbol"));
                            propBuilder.min(getText(option, "min"));
                            propBuilder.max(getText(option, "max"));
                            break;
                        default:
                            throw new RuntimeException("unknown type: " + propType);
                    }

                    Property prop = propBuilder.build();

                    for (LinStorObject lsObj : lsObjs)
                    {
                        Map<String, Property> propMap = rules.get(lsObj);
                        if (propMap == null)
                        {
                            propMap = new HashMap<>();
                            rules.put(lsObj, propMap);
                        }

                        if (!overrideProp)
                        {
                            Property oldProp = propMap.get(propName);
                            if (oldProp != null)
                            {
                                throw new LinStorRuntimeException(
                                    "Property '" + propName + "' would be overridden by the current operation."
                                );
                            }
                        }
                        propMap.put(prop.getKey(), prop);
                        if (isDynamic)
                        {
                            dynamicProps.put(prop.getKey(), prop);
                        }
                    }
                }
            }
        }
        catch (SAXException exc)
        {
            throw new LinStorRuntimeException(
                "Invalid XML file",
                exc
            );
        }
        catch (IOException exc)
        {
            throw new LinStorRuntimeException(
                "IO Exception occurred while reading drbd options xml",
                exc
            );
        }
        catch (ParserConfigurationException exc)
        {
            throw new LinStorRuntimeException(
                "Error while parsing the drbd options xml",
                exc
            );
        }
    }

    /**
     * Returns which drbd options need quoting.
     * @param lsObj
     * @param key
     * @return true if value should be quoted, otherwise false.
     */
    public boolean needsQuoting(LinStorObject lsObj, String key)
    {
        Property rule = rules.getOrDefault(lsObj, Collections.emptyMap()).get(key);

        boolean result;
        if (rule != null)
        {
            result = rule.getType() == Property.PropertyType.STRING || rule.getType() == Property.PropertyType.SYMBOL;
        }
        else
        {
            result = false;
        }
        return result;
    }

    public boolean isAllowed(
        LinStorObject lsObj,
        List<String> ignoredKeys,
        String key,
        @Nullable String value,
        boolean log
    )
    {
        boolean validProp = ignoredKeys.stream().anyMatch(ignoredKey -> key.startsWith(ignoredKey));
        if (!validProp)
        {
            Property rule = rules.get(lsObj).get(key);
            if (rule != null)
            {
                validProp = value == null || rule.isValid(value); // value is null if user tries to delete it
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
        }
        return validProp;
    }

    /**
     * Converts the value to its canonical form.
     * The property should already have been validated with {@link #isAllowed(LinStorObject, String, String, boolean)}.
     */
    public String normalize(LinStorObject lsObj, String key, String value)
    {
        String ret = value;
        Property rule = rules.get(lsObj).get(key);
        if (rule != null)
        {
            ret = rule.normalize(value);
        }
        return ret;
    }

    public String getRuleValue(LinStorObject linstorObj, String key)
    {
        return rules.get(linstorObj).get(key).getValue();
    }

    public String getErrMsg(LinStorObject linstorObj, String key)
    {
        return rules.get(linstorObj).get(key).getErrorMsg();
    }

    public boolean isKeyKnown(LinStorObject linstorObj, String key)
    {
        return rules.get(linstorObj).get(key) != null;
    }

    public Map<LinStorObject, Map<String, Property>> getRules()
    {
        return Collections.unmodifiableMap(rules);
    }

    private String[] extractEnumValues(Element option, String tag)
    {
        ArrayList<String> enums = new ArrayList<>();
        foreachElement(
            option,
            tag,
            (elem) ->  enums.add(elem.getTextContent())
        );
        String[] arr = new String[enums.size()];
        enums.toArray(arr);
        return arr;
    }


    private void foreachElement(Element elem, String tag, Consumer<Element> consumer)
    {
        NodeList list = elem.getElementsByTagName(tag);
        for (int idx = 0; idx < list.getLength(); idx++)
        {
            Node node = list.item(idx);
            if (node instanceof Element)
            {
                consumer.accept((Element) node);
            }
        }
    }

    private String getText(Element elem, String string)
    {
        return elem.getElementsByTagName(string).item(0).getTextContent();
    }

    public void clearDynamicProps()
    {
        dynamicProps.clear();
    }

    public Map<String, Property> getDynamicProps()
    {
        return new HashMap<>(dynamicProps);
    }
}
