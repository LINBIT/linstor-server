/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.linbit.linstor.core;


import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;
import static org.junit.Assert.*;
/**
 *
 * @author rpeinthor
 */
public class ApiCallLoadingTest {

    @Test
    public void testClassPathExpand()
    {
        {
            String cp1 = "." + File.pathSeparator + "bin";
            List<String> p1 = LinStor.expandClassPath(cp1);
            ArrayList<String> a1 = new ArrayList<>();
            a1.add(Paths.get(".").toAbsolutePath().toString());
            a1.add(Paths.get("bin").toAbsolutePath().toString());
            assertArrayEquals(a1.toArray(), p1.toArray());
        }

        {
            String cp = "." + File.pathSeparator + "build" + File.separator + "libs" + File.separator + "*.jar";
            List<String> p = LinStor.expandClassPath(cp);
            ArrayList<String> a = new ArrayList<>();
            a.add(Paths.get(".").toAbsolutePath().toString());
            assertArrayEquals("*.jar is not supported", a.toArray(), p.toArray());
        }

        {
            Path jarpath = Paths.get("build/libs").toFile().listFiles()[0].toPath().toAbsolutePath();
            String cp = "." + File.pathSeparator + "build" + File.separator + "libs" + File.separator + "*";
            List<String> p = LinStor.expandClassPath(cp);
            ArrayList<String> a = new ArrayList<>();
            a.add(Paths.get(".").toAbsolutePath().toString());
            a.add(jarpath.toString());
            assertArrayEquals(a.toArray(), p.toArray());
        }

        {
            String cp = "." + File.pathSeparator + "*";
            List<String> p = LinStor.expandClassPath(cp);
            ArrayList<String> a = new ArrayList<>();
            a.add(Paths.get(".").toAbsolutePath().toString());
            assertArrayEquals(a.toArray(), p.toArray());
        }
    }
}
