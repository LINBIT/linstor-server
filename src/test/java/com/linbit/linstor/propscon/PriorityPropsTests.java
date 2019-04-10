package com.linbit.linstor.propscon;

import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.security.GenericDbBase;

import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class PriorityPropsTests extends GenericDbBase
{
    private PropsContainer prop1;
    private PropsContainer prop2;
    private PropsContainer prop3;
    private PriorityProps prioProps;

    @Before
    public void setUp() throws Exception
    {
        super.setUpAndEnterScope();

        prop1 = propsContainerFactory.getInstance("testInstanceName1");
        prop2 = propsContainerFactory.getInstance("testInstanceName2");
        prop3 = propsContainerFactory.getInstance("testInstanceName3");

        prop1.setProp("/a/1", "1");
        prop1.setProp("/b/1", "2");
        prop1.setProp("/b/2", "3");

        prop2.setProp("/a/1", "4");
        prop2.setProp("/a/2", "5");
        prop2.setProp("/b/1", "6");
        prop2.setProp("/b/3", "7");
        prop2.setProp("/b/a/1", "8");

        prop3.setProp("/c/1", "9");

        prioProps = new PriorityProps(prop1, prop2, prop3);
    }

    @Test
    public void basicTest() throws Exception
    {
        assertEquals("1", prioProps.getProp("a/1"));
        assertEquals("1", prioProps.getProp("/a/1"));
        assertEquals("1", prioProps.getProp("1", "a"));

        assertEquals("2", prioProps.getProp("/b/1"));
        assertEquals("3", prioProps.getProp("/b/2"));

        assertEquals("5", prioProps.getProp("/a/2"));
        assertEquals("7", prioProps.getProp("/b/3"));
        assertEquals("8", prioProps.getProp("/b/a/1"));

        assertEquals("9", prioProps.getProp("/c/1"));
    }

    @Test
    @SuppressWarnings("checkstyle:magicnumber")
    public void relativeMapTest() throws Exception
    {
        Map<String, String> map = prioProps.renderRelativeMap("b");
        System.out.println(map);
        assertEquals("2", map.get("1"));
        assertEquals("3", map.get("2"));
        assertEquals("7", map.get("3"));
        assertEquals("8", map.get("a/1"));
        assertEquals(4, map.size());
    }
}
