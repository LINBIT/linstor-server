package com.linbit.linstor.api.prop;

import com.linbit.linstor.LinStorRuntimeException;
import com.linbit.linstor.logging.ErrorReporter;

import java.util.Arrays;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

@Singleton
public class WhitelistProps
{
    private ErrorReporter errorReporter;
    private Map<LinStorObject, Map<String, Property>> rules;

    @Inject
    public WhitelistProps(ErrorReporter errorReporterRef)
    {
        rules = new HashMap<>();
        errorReporter = errorReporterRef;

        List<Property> props = GeneratedPropertyRules.getWhitelistedProperties();

        Map<String, Property> propsByName = props.stream().collect(
            Collectors.toMap(Property::getName, Function.identity())
        );

        Map<LinStorObject, List<String>> rulesStr = GeneratedPropertyRules.getWhitelistedRules();
        for (Entry<LinStorObject, List<String>> ruleEntry : rulesStr.entrySet())
        {
            Map<String, Property> rulesPropMap = new HashMap<>();
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
            .forEach(obj -> rules.put(obj, new HashMap<>()));
    }

    /**
     * This method should only be called while holding a reconfiguration.writeLock
     *
     * This instance should be singleton, as (beside reconfiguration) it is only read-only.
     * Modifying it while not holding the reconf.writeLock might cause {@link ConcurrentModificationException}s
     */
    public void reconfigure()
    {
        rules.clear();
    }

    public void appendRules(
        boolean overrideProp,
        InputStream xmlStream,
        String keyPrefix,
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
                "IO Exception occured while reading drbd options xml",
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
}
