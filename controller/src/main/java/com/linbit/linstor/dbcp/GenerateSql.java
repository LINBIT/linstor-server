package com.linbit.linstor.dbcp;

import com.linbit.ImplementationError;
import com.linbit.linstor.dbcp.migration.LinstorMigration;
import com.linbit.linstor.dbdrivers.DatabaseConstantsGenerator;
import com.linbit.linstor.dbdrivers.DatabaseConstantsGenerator.GeneratedCrdJavaClass;
import com.linbit.linstor.dbdrivers.DatabaseConstantsGenerator.GeneratedCrdResult;
import com.linbit.linstor.dbdrivers.DatabaseConstantsGenerator.GeneratedResources;

import static com.linbit.linstor.dbdrivers.derby.DbConstants.DATABASE_SCHEMA_NAME;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.dbcp2.ConnectionFactory;
import org.apache.commons.dbcp2.DriverManagerConnectionFactory;
import org.apache.commons.dbcp2.PoolableConnection;
import org.apache.commons.dbcp2.PoolableConnectionFactory;
import org.apache.commons.dbcp2.PoolingDataSource;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.flywaydb.core.Flyway;

public class GenerateSql {
    private static final Path CRD_INIT_RELATIVE_LOCATION_FROM_GITROOT = Paths.get(
        "server",
        "generated-resources",
        "crd-version.properties"
    );
    private static final Path GENERIC_VERSION_RELATIVE_LOCATION_FROM_GITROOT = Paths.get(
        "server",
        "generated-resources",
        "version-info.properties"
    );
    private static final String INI_KEY_VERSION = "crd.version";
    private static final String INI_KEY_GIT_TAG = "git.tag";

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.setProperty("user", "linstor");
        props.setProperty("password", "linstor");
        ConnectionFactory connFactory = new DriverManagerConnectionFactory("jdbc:h2:mem:testDB", props);
        PoolableConnectionFactory poolConnFactory = new PoolableConnectionFactory(connFactory, null);

        GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
        poolConfig.setMinIdle(10);
        poolConfig.setMaxIdle(100);
        poolConfig.setBlockWhenExhausted(true);
        poolConfig.setFairness(true);
        GenericObjectPool<PoolableConnection> connPool = new GenericObjectPool<>(poolConnFactory, poolConfig);

        poolConnFactory.setPool(connPool);
        poolConnFactory.setValidationQueryTimeout(60000);
        poolConnFactory.setMaxOpenPrepatedStatements(100);
        poolConnFactory.setMaxConnLifetimeMillis(3600000);
        poolConnFactory.setDefaultTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);

        PoolingDataSource<PoolableConnection> dataSource = new PoolingDataSource<>(connPool);

        Flyway flyway = Flyway.configure()
            .schemas(DATABASE_SCHEMA_NAME)
            .dataSource(dataSource)
            .table("FLYWAY_SCHEMA_HISTORY")
            // When migrations are added in branches they can be applied in different orders
            .outOfOrder(true)
            // Pass the DB type to the migrations
            .placeholders(ImmutableMap.of(LinstorMigration.PLACEHOLDER_KEY_DB_TYPE, "h2"))
            .locations(LinstorMigration.class.getPackage().getName())
            .load();

        flyway.baseline();
        flyway.migrate();

        DatabaseConstantsGenerator gen = new DatabaseConstantsGenerator(dataSource.getConnection());
        String gitRoot = getGitRoot();
        renderGeneratedDatabaseTables(gen, gitRoot);
        renderGeneratedKubernetesCustomResourceDefinitions(gen, gitRoot);

        dataSource.close();
    }

    private static void renderGeneratedDatabaseTables(DatabaseConstantsGenerator generator, String gitRoot)
        throws IOException
    {
        renderJavaFile(
            generator.renderSqlConsts(
                DatabaseConstantsGenerator.DFLT_DBDRIVERS_PACKAGE,
                DatabaseConstantsGenerator.DFLT_GEN_DB_TABLES_CLASS_NAME
            ),
            DatabaseConstantsGenerator.DFLT_DBDRIVERS_PACKAGE,
            DatabaseConstantsGenerator.DFLT_GEN_DB_TABLES_CLASS_NAME,
            gitRoot,
            "server"
        );
    }

    private static void renderGeneratedKubernetesCustomResourceDefinitions(
        DatabaseConstantsGenerator generator,
        String gitRoot
    )
        throws IOException
    {
        // String crdVersion = loadCrdVersion(gitRoot);
        String crdVersion = loadCurrentGitTag(gitRoot);
        GeneratedCrdResult result = generator.renderKubernetesCustomResourceDefinitions(
            DatabaseConstantsGenerator.DFLT_K8S_CRD_PACKAGE,
            DatabaseConstantsGenerator.DFLT_K8S_CRD_CLASS_NAME_FORMAT,
            crdVersion,
            loadAllCrdVersionNumbers(
                DatabaseConstantsGenerator.getYamlLocation("", "")
            )
        );

        for (GeneratedCrdJavaClass generatedCrdJavaClass : result.javaClasses)
        {
            renderJavaFile(
                generatedCrdJavaClass.javaCode,
                generatedCrdJavaClass.pkgName,
                generatedCrdJavaClass.clazzName,
                gitRoot,
                "server"
            );
        }

        for (GeneratedResources generatedResources : result.resources)
        {
            renderResourceFile(gitRoot, generatedResources.yamlLocation, generatedResources.content);
        }
        // rerenderCrdVersion(gitRoot, crdVersion);
    }

    private static String loadCurrentGitTag(String gitRoot)
    {
        String ret = null;

        ProcessBuilder pb = new ProcessBuilder("git describe --tags --abbrev=0".split(" "));
        pb.directory(Paths.get(gitRoot).toFile());
        try
        {
            Process process = pb.start();
            int ec = process.waitFor();
            if (ec != 0) {
                throw new ImplementationError("'" + pb.command() + "' returned with exit code: " + ec);
            }
            ret = new BufferedReader(
                new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8)
            )
                .lines()
                .collect(Collectors.joining("\n"))
                .replaceAll("[.-]", "_");
        }
        catch (IOException | InterruptedException exc)
        {
            throw new ImplementationError("Command failed: " + pb.command(), exc);
        }
        return ret;
    }

    private static boolean genCrdCurrentChanged(String gitRoot, GeneratedCrdResult result)
    {
        boolean ret = true;
        // find GenCrdCurrent
        for (GeneratedCrdJavaClass generatedCrdJavaClass : result.javaClasses)
        {
            if (generatedCrdJavaClass.clazzName.equals(DatabaseConstantsGenerator.DFLT_K8S_CRD_CLASS_NAME_CURRENT))
            {
                Path path = buildJavaPath(
                    gitRoot,
                    "server",
                    generatedCrdJavaClass.pkgName
                )
                    .resolve(generatedCrdJavaClass.clazzName + ".java");
                if (Files.exists(path))
                {
                    String checksumOld;
                    try
                    {
                        checksumOld = getCheckSum(new String(Files.readAllBytes(path)));
                    }
                    catch (IOException exc)
                    {
                        throw new ImplementationError(exc);
                    }
                    String checksumNew = getCheckSum(generatedCrdJavaClass.javaCode);

                    // System.out.println("old checksum: " + checksumOld);
                    // System.out.println("new checksum: " + checksumNew);

                    ret = !checksumNew.equals(checksumOld);
                }
                break;
            }
        }
        return ret;
    }

    private static String getCheckSum(String javaCodeRef)
    {
        String ret = null;
        MessageDigest digest;
        try
        {
            digest = MessageDigest.getInstance("SHA-256");
            byte[] encodedhash = digest.digest(javaCodeRef.getBytes(StandardCharsets.UTF_8));
            StringBuilder hexString = new StringBuilder(2 * encodedhash.length);
            for (int i = 0; i < encodedhash.length; i++)
            {
                String hex = Integer.toHexString(0xff & encodedhash[i]);
                if (hex.length() == 1)
                {
                    hexString.append('0');
                }
                hexString.append(hex);
            }
            ret = hexString.toString();
        }
        catch (NoSuchAlgorithmException exc)
        {
            throw new ImplementationError(exc);
        }
        return ret;
    }

    private static String getCheckSum(Path pathRef)
    {
        final String checksum;
        ProcessBuilder pb = new ProcessBuilder("sha256sum", pathRef.toAbsolutePath().toString());
        try
        {
            Process proc = pb.start();
            proc.waitFor();
            checksum = new BufferedReader(
                new InputStreamReader(proc.getInputStream(), StandardCharsets.UTF_8)
            )
                .lines()
                .collect(Collectors.joining("\n"));
        }
        catch (IOException | InterruptedException exc)
        {

            throw new ImplementationError("Failed to '" + pb.command() + "'", exc);
        }
        return checksum;
    }

    private static Set<String> loadAllCrdVersionNumbers(String yamlLocationRef) throws IOException
    {
        Path path = Paths.get(yamlLocationRef).getParent();
        Set<String> ret;
        if (Files.isDirectory(path))
        {
            ret = Files.list(path)
                .map(pathtmp -> pathtmp.getFileName().toString())
                .collect(Collectors.toSet());
        }
        else
        {
            ret = new HashSet<>();
        }
        return ret;
    }

    private static Set<String> loadAllCrdVersionNumbers(
        Path generatedCrdDir
    )
        throws IOException
    {
        if (!Files.exists(generatedCrdDir))
        {
            return Collections.emptySet();
        }
        return Files.list(generatedCrdDir)
            .map(path -> path.getFileName().toString())
            .filter(path ->
            {
                Matcher m = DatabaseConstantsGenerator.VERSION_PATTERN.matcher(path.toString());
                boolean ret = m.find();
                return ret;
            })
            .collect(Collectors.toSet());
    }

    private static void renderJavaFile(
        String javaClazzContentRef,
        String pkgNameRef,
        String javaClazzNameRef,
        String gitRoot,
        String prj
    )
        throws IOException
    {
        Path path = buildJavaPath(gitRoot, prj, pkgNameRef).resolve(javaClazzNameRef + ".java");
        Files.createDirectories(path.getParent()); // ensure parent folder exists
        Files.write(path, javaClazzContentRef.getBytes());
    }

    private static void renderResourceFile(
        String gitRoot,
        String relativePath,
        String content
    )
        throws IOException
    {
        Path path = Paths.get(gitRoot).resolve("controller/generated-resources").resolve(Paths.get(".", relativePath))
            .normalize();
        Files.createDirectories(path.getParent()); // ensure parent folder exists
        Files.write(path, content.getBytes());
    }

    private static Path buildJavaPath(String gitRootRef, String prj, String pkgNameRef)
    {
        return Paths.get(
            gitRootRef,
            prj,
            "generated-src",
            pkgNameRef.replaceAll("[.]", "/")
        );
    }

    private static String loadCrdVersion(String gitRoot) throws IOException
    {
        Properties iniConfig = new Properties();
        Path path = Paths.get(gitRoot).resolve(CRD_INIT_RELATIVE_LOCATION_FROM_GITROOT);
        String ret = null;
        if (Files.exists(path))
        {
            iniConfig.load(new FileInputStream(path.toFile()));
            ret = (String) iniConfig.get(INI_KEY_VERSION);
        }

        if (ret == null)
        {
            ret = "v1";
        }
        return ret;
    }

    private static void rerenderCrdVersion(String gitRoot, String currentVersion)
    {
        Path path = Paths.get(gitRoot).resolve(CRD_INIT_RELATIVE_LOCATION_FROM_GITROOT);
        String tag = loadGitTagFromGenericProperties(gitRoot);
        Properties iniConfig = new Properties();
        iniConfig.put(INI_KEY_VERSION, currentVersion);
        iniConfig.put(INI_KEY_GIT_TAG, tag);
        try
        {
            Files.createDirectories(path.getParent());
            iniConfig.store(new FileOutputStream(path.toFile()), null);
        }
        catch (IOException exc)
        {
            throw new ImplementationError("Failed to save crd.version!", exc);
        }
    }

    private static String loadGitTagFromGenericProperties(String gitRoot) throws ImplementationError
    {
        String tag = null;
        Properties props = new Properties();
        Path path = Paths.get(gitRoot).resolve(GENERIC_VERSION_RELATIVE_LOCATION_FROM_GITROOT);
        if (Files.exists(path))
        {
            try
            {
                props.load(new FileInputStream(path.toFile()));
                tag = (String) props.get(INI_KEY_GIT_TAG);
                if (tag != null)
                {
                    tag.replaceAll("[-.]", "_");
                }
            }
            catch (IOException exc)
            {
                throw new ImplementationError("Failed to load " + path);
            }
        }
        return tag;
    }

    private static String getGitRoot() throws IOException, InterruptedException
    {
        ProcessBuilder pb = new ProcessBuilder("git", "rev-parse", "--show-toplevel");
        Process p = pb.start();
        int rc = p.waitFor();
        if (rc != 0)
        {
            throw new RuntimeException("git base path checking failed:" + pb.toString());
        }
        final String gitRoot = new BufferedReader(new InputStreamReader(p.getInputStream(), StandardCharsets.UTF_8))
            .lines()
            .collect(Collectors.joining("\n"));
        return gitRoot;
    }

}
