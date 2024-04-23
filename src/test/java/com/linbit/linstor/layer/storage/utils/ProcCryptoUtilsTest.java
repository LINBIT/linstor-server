package com.linbit.linstor.layer.storage.utils;

import com.linbit.linstor.storage.ProcCryptoEntry;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.TestCase;

public class ProcCryptoUtilsTest extends TestCase
{

    private static String PROC_CRYPTO_OUTPUT = null;

    @Override
    public void setUp() throws IOException, URISyntaxException
    {
        final URI procCryptoURI = this.getClass()
            .getResource("/com/linbit/linstor/layer/storage/utils/proc_crypto.txt").toURI();
        PROC_CRYPTO_OUTPUT = new String(Files.readAllBytes(Paths.get(procCryptoURI)));
    }

    public void testParsing()
    {
        List<ProcCryptoEntry> cryptos = ProcCryptoUtils.parseProcCryptoString(PROC_CRYPTO_OUTPUT);
        assertEquals(77, cryptos.size());
        final ProcCryptoEntry last = cryptos.get(cryptos.size()-1);
        assertEquals("dh", last.getName());
        assertEquals("dh-generic", last.getDriver());
        assertEquals(ProcCryptoEntry.CryptoType.KPP, last.getType());
        assertEquals(100, last.getPriority());
    }

    public void testEmptyParsing()
    {
        List<ProcCryptoEntry> cryptos = ProcCryptoUtils.parseProcCryptoString("");
        assertEquals(0, cryptos.size());
    }

    public void testLocalProcCrypto() throws IOException
    {
        List<ProcCryptoEntry> cryptos = ProcCryptoUtils.parseProcCrypto();
        assertTrue(cryptos.size() > 0);
    }

    public void testSortAndFilter()
    {
        List<ProcCryptoEntry> cryptos = ProcCryptoUtils.parseProcCryptoString(PROC_CRYPTO_OUTPUT);
        List<ProcCryptoEntry> hashes = ProcCryptoUtils.getHashByPriority(cryptos);
        assertEquals(21, hashes.size());
        assertTrue(hashes.stream().allMatch(e -> e.getType() == ProcCryptoEntry.CryptoType.SHASH));
        assertEquals(300, hashes.get(0).getPriority());
    }

    public void testFindCommon()
    {
        ArrayList<ProcCryptoEntry> cryptoNode1 = new ArrayList<>();
        cryptoNode1.add(new ProcCryptoEntry("curve25519", "curve25519-x86", ProcCryptoEntry.CryptoType.KPP, 200));
        cryptoNode1.add(new ProcCryptoEntry("xchacha12", "xchacha12-simd", ProcCryptoEntry.CryptoType.SKCIPHER, 300));
        cryptoNode1.add(new ProcCryptoEntry("poly1305", "poly1305-simd", ProcCryptoEntry.CryptoType.SHASH, 300));
        cryptoNode1.add(new ProcCryptoEntry("blake2s-256", "blake2s-256-x86", ProcCryptoEntry.CryptoType.SHASH, 200));
        cryptoNode1.add(new ProcCryptoEntry("lzo-rle", "lzo-rle-scomp", ProcCryptoEntry.CryptoType.SCOMP, 0));

        ArrayList<ProcCryptoEntry> cryptoNode2 = new ArrayList<>();
        cryptoNode2.add(new ProcCryptoEntry("curve25519", "curve25519-x86", ProcCryptoEntry.CryptoType.KPP, 200));
        cryptoNode2.add(new ProcCryptoEntry("poly1305", "poly1305-simd", ProcCryptoEntry.CryptoType.SHASH, 300));
        cryptoNode2.add(new ProcCryptoEntry("blake2s-256", "blake2s-256-x86", ProcCryptoEntry.CryptoType.SHASH, 200));
        cryptoNode2.add(new ProcCryptoEntry("lzo-rle", "lzo-rle-scomp", ProcCryptoEntry.CryptoType.SCOMP, 0));

        ArrayList<ProcCryptoEntry> cryptoNode3 = new ArrayList<>();
        cryptoNode3.add(new ProcCryptoEntry("curve25519", "curve25519-x86", ProcCryptoEntry.CryptoType.KPP, 200));
        cryptoNode3.add(new ProcCryptoEntry("xchacha12", "xchacha12-simd", ProcCryptoEntry.CryptoType.SKCIPHER, 300));
        cryptoNode3.add(new ProcCryptoEntry("poly1305", "poly1305-simd", ProcCryptoEntry.CryptoType.SHASH, 300));
        cryptoNode3.add(new ProcCryptoEntry("blake2s-256", "blake2s-256-x86", ProcCryptoEntry.CryptoType.SHASH, 200));
        cryptoNode3.add(new ProcCryptoEntry("lzo-rle", "lzo-rle-scomp", ProcCryptoEntry.CryptoType.SCOMP, 0));
        cryptoNode3.add(new ProcCryptoEntry("crc32", "crc32-pclmul", ProcCryptoEntry.CryptoType.SHASH, 200));

        ProcCryptoEntry pce = ProcCryptoUtils.commonCryptoType(
            Collections.emptyMap(), ProcCryptoEntry.CryptoType.AEAD, Collections.emptyList());
        assertNull(pce);

        Map<String, List<ProcCryptoEntry>> cryptoMap = new HashMap<>();
        cryptoMap.put("linstor1", cryptoNode1);
        cryptoMap.put("linstor2", cryptoNode2);
        cryptoMap.put("linstor3", cryptoNode3);

        pce = ProcCryptoUtils.commonCryptoType(cryptoMap, ProcCryptoEntry.CryptoType.SHASH, Collections.emptyList());

        assertNotNull(pce);
        assertEquals("poly1305-simd", pce.getDriver());
    }

    public void testFindCommonPriority()
    {
        ArrayList<ProcCryptoEntry> cryptoNode1 = new ArrayList<>();
        cryptoNode1.add(new ProcCryptoEntry("curve25519", "curve25519-x86", ProcCryptoEntry.CryptoType.KPP, 200));
        cryptoNode1.add(new ProcCryptoEntry("xchacha12", "xchacha12-simd", ProcCryptoEntry.CryptoType.SKCIPHER, 300));
        cryptoNode1.add(new ProcCryptoEntry("poly1305", "poly1305-simd", ProcCryptoEntry.CryptoType.SHASH, 100));
        cryptoNode1.add(new ProcCryptoEntry("blake2s-256", "blake2s-256-x86", ProcCryptoEntry.CryptoType.SHASH, 200));
        cryptoNode1.add(new ProcCryptoEntry("lzo-rle", "lzo-rle-scomp", ProcCryptoEntry.CryptoType.SCOMP, 0));

        ArrayList<ProcCryptoEntry> cryptoNode2 = new ArrayList<>();
        cryptoNode2.add(new ProcCryptoEntry("curve25519", "curve25519-x86", ProcCryptoEntry.CryptoType.KPP, 200));
        cryptoNode2.add(new ProcCryptoEntry("poly1305", "poly1305-simd", ProcCryptoEntry.CryptoType.SHASH, 100));
        cryptoNode2.add(new ProcCryptoEntry("blake2s-256", "blake2s-256-x86", ProcCryptoEntry.CryptoType.SHASH, 200));
        cryptoNode2.add(new ProcCryptoEntry("lzo-rle", "lzo-rle-scomp", ProcCryptoEntry.CryptoType.SCOMP, 0));

        ArrayList<ProcCryptoEntry> cryptoNode3 = new ArrayList<>();
        cryptoNode3.add(new ProcCryptoEntry("curve25519", "curve25519-x86", ProcCryptoEntry.CryptoType.KPP, 200));
        cryptoNode3.add(new ProcCryptoEntry("xchacha12", "xchacha12-simd", ProcCryptoEntry.CryptoType.SKCIPHER, 300));
        cryptoNode3.add(new ProcCryptoEntry("poly1305", "poly1305-simd", ProcCryptoEntry.CryptoType.SHASH, 100));
        cryptoNode3.add(new ProcCryptoEntry("blake2s-256", "blake2s-256-x86", ProcCryptoEntry.CryptoType.SHASH, 200));
        cryptoNode3.add(new ProcCryptoEntry("lzo-rle", "lzo-rle-scomp", ProcCryptoEntry.CryptoType.SCOMP, 0));
        cryptoNode3.add(new ProcCryptoEntry("crc32", "crc32-pclmul", ProcCryptoEntry.CryptoType.SHASH, 200));

        Map<String, List<ProcCryptoEntry>> cryptoMap = new HashMap<>();
        cryptoMap.put("linstor1", cryptoNode1);
        cryptoMap.put("linstor2", cryptoNode2);
        cryptoMap.put("linstor3", cryptoNode3);

        ProcCryptoEntry pce = ProcCryptoUtils.commonCryptoType(
            cryptoMap, ProcCryptoEntry.CryptoType.SHASH, Collections.emptyList());

        assertNotNull(pce);
        assertEquals("blake2s-256-x86", pce.getDriver());
    }

    public void testFindCommonOneNode()
    {
        ArrayList<ProcCryptoEntry> cryptoNode1 = new ArrayList<>();
        cryptoNode1.add(new ProcCryptoEntry("curve25519", "curve25519-x86", ProcCryptoEntry.CryptoType.KPP, 200));
        cryptoNode1.add(new ProcCryptoEntry("xchacha12", "xchacha12-simd", ProcCryptoEntry.CryptoType.SKCIPHER, 300));
        cryptoNode1.add(new ProcCryptoEntry("poly1305", "poly1305-simd", ProcCryptoEntry.CryptoType.SHASH, 300));
        cryptoNode1.add(new ProcCryptoEntry("blake2s-256", "blake2s-256-x86", ProcCryptoEntry.CryptoType.SHASH, 200));
        cryptoNode1.add(new ProcCryptoEntry("lzo-rle", "lzo-rle-scomp", ProcCryptoEntry.CryptoType.SCOMP, 0));

        Map<String, List<ProcCryptoEntry>> cryptoMap = new HashMap<>();
        cryptoMap.put("linstor1", cryptoNode1);

        ProcCryptoEntry pce = ProcCryptoUtils.commonCryptoType(
            cryptoMap, ProcCryptoEntry.CryptoType.SHASH, Collections.emptyList());

        assertNotNull(pce);
        assertEquals("poly1305-simd", pce.getDriver());
    }

    public void testFindCommonNone()
    {
        ArrayList<ProcCryptoEntry> cryptoNode1 = new ArrayList<>();
        cryptoNode1.add(new ProcCryptoEntry("curve25519", "curve25519-x86", ProcCryptoEntry.CryptoType.KPP, 200));
        cryptoNode1.add(new ProcCryptoEntry("xchacha12", "xchacha12-simd", ProcCryptoEntry.CryptoType.SKCIPHER, 300));
        cryptoNode1.add(new ProcCryptoEntry("blake2s-256", "blake2s-256-x86", ProcCryptoEntry.CryptoType.SHASH, 200));
        cryptoNode1.add(new ProcCryptoEntry("lzo-rle", "lzo-rle-scomp", ProcCryptoEntry.CryptoType.SCOMP, 0));

        ArrayList<ProcCryptoEntry> cryptoNode2 = new ArrayList<>();
        cryptoNode2.add(new ProcCryptoEntry("curve25519", "curve25519-x86", ProcCryptoEntry.CryptoType.KPP, 200));
        cryptoNode2.add(new ProcCryptoEntry("poly1305", "poly1305-simd", ProcCryptoEntry.CryptoType.SHASH, 100));
        cryptoNode2.add(new ProcCryptoEntry("lzo-rle", "lzo-rle-scomp", ProcCryptoEntry.CryptoType.SCOMP, 0));

        ArrayList<ProcCryptoEntry> cryptoNode3 = new ArrayList<>();
        cryptoNode3.add(new ProcCryptoEntry("curve25519", "curve25519-x86", ProcCryptoEntry.CryptoType.KPP, 200));
        cryptoNode3.add(new ProcCryptoEntry("xchacha12", "xchacha12-simd", ProcCryptoEntry.CryptoType.SKCIPHER, 300));
        cryptoNode3.add(new ProcCryptoEntry("poly1305", "poly1305-simd", ProcCryptoEntry.CryptoType.SHASH, 100));
        cryptoNode3.add(new ProcCryptoEntry("blake2s-256", "blake2s-256-x86", ProcCryptoEntry.CryptoType.SHASH, 200));
        cryptoNode3.add(new ProcCryptoEntry("lzo-rle", "lzo-rle-scomp", ProcCryptoEntry.CryptoType.SCOMP, 0));
        cryptoNode3.add(new ProcCryptoEntry("crc32", "crc32-pclmul", ProcCryptoEntry.CryptoType.SHASH, 200));

        Map<String, List<ProcCryptoEntry>> cryptoMap = new HashMap<>();
        cryptoMap.put("linstor1", cryptoNode1);
        cryptoMap.put("linstor2", cryptoNode2);
        cryptoMap.put("linstor3", cryptoNode3);

        ProcCryptoEntry pce = ProcCryptoUtils.commonCryptoType(
            cryptoMap, ProcCryptoEntry.CryptoType.SHASH, Collections.emptyList());

        assertNull(pce);
    }

    public void testFindCommonAllowed()
    {
        ArrayList<ProcCryptoEntry> cryptoNode1 = new ArrayList<>();
        cryptoNode1.add(new ProcCryptoEntry("curve25519", "curve25519-x86", ProcCryptoEntry.CryptoType.KPP, 200));
        cryptoNode1.add(new ProcCryptoEntry("xchacha12", "xchacha12-simd", ProcCryptoEntry.CryptoType.SKCIPHER, 300));
        cryptoNode1.add(new ProcCryptoEntry("poly1305", "poly1305-simd", ProcCryptoEntry.CryptoType.SHASH, 100));
        cryptoNode1.add(new ProcCryptoEntry("blake2s-256", "blake2s-256-x86", ProcCryptoEntry.CryptoType.SHASH, 200));
        cryptoNode1.add(new ProcCryptoEntry("lzo-rle", "lzo-rle-scomp", ProcCryptoEntry.CryptoType.SCOMP, 0));
        cryptoNode1.add(new ProcCryptoEntry("crc32", "crc32-pclmul", ProcCryptoEntry.CryptoType.SHASH, 200));

        ArrayList<ProcCryptoEntry> cryptoNode2 = new ArrayList<>();
        cryptoNode2.add(new ProcCryptoEntry("curve25519", "curve25519-x86", ProcCryptoEntry.CryptoType.KPP, 200));
        cryptoNode2.add(new ProcCryptoEntry("poly1305", "poly1305-simd", ProcCryptoEntry.CryptoType.SHASH, 100));
        cryptoNode2.add(new ProcCryptoEntry("blake2s-256", "blake2s-256-x86", ProcCryptoEntry.CryptoType.SHASH, 200));
        cryptoNode2.add(new ProcCryptoEntry("lzo-rle", "lzo-rle-scomp", ProcCryptoEntry.CryptoType.SCOMP, 0));
        cryptoNode2.add(new ProcCryptoEntry("crc32", "crc32-pclmul", ProcCryptoEntry.CryptoType.SHASH, 200));

        ArrayList<ProcCryptoEntry> cryptoNode3 = new ArrayList<>();
        cryptoNode3.add(new ProcCryptoEntry("curve25519", "curve25519-x86", ProcCryptoEntry.CryptoType.KPP, 200));
        cryptoNode3.add(new ProcCryptoEntry("xchacha12", "xchacha12-simd", ProcCryptoEntry.CryptoType.SKCIPHER, 300));
        cryptoNode3.add(new ProcCryptoEntry("poly1305", "poly1305-simd", ProcCryptoEntry.CryptoType.SHASH, 100));
        cryptoNode3.add(new ProcCryptoEntry("blake2s-256", "blake2s-256-x86", ProcCryptoEntry.CryptoType.SHASH, 200));
        cryptoNode3.add(new ProcCryptoEntry("lzo-rle", "lzo-rle-scomp", ProcCryptoEntry.CryptoType.SCOMP, 0));
        cryptoNode3.add(new ProcCryptoEntry("crc32", "crc32-pclmul", ProcCryptoEntry.CryptoType.SHASH, 200));

        Map<String, List<ProcCryptoEntry>> cryptoMap = new HashMap<>();
        cryptoMap.put("linstor1", cryptoNode1);
        cryptoMap.put("linstor2", cryptoNode2);
        cryptoMap.put("linstor3", cryptoNode3);

        ProcCryptoEntry pce = ProcCryptoUtils.commonCryptoType(
            cryptoMap, ProcCryptoEntry.CryptoType.SHASH, Collections.singletonList("poly1305-simd"));

        assertNotNull(pce);
        assertEquals("poly1305-simd", pce.getDriver());

        pce = ProcCryptoUtils.commonCryptoType(
            cryptoMap, ProcCryptoEntry.CryptoType.SHASH, Arrays.asList("poly1305-simd", "crc32-pclmul"));

        assertNotNull(pce);
        assertEquals("crc32-pclmul", pce.getDriver());
    }

    public void testCryptoDriverSupported() {
        ArrayList<ProcCryptoEntry> cryptoNode1 = new ArrayList<>();
        cryptoNode1.add(new ProcCryptoEntry("curve25519", "curve25519-x86", ProcCryptoEntry.CryptoType.KPP, 200));
        cryptoNode1.add(new ProcCryptoEntry("xchacha12", "xchacha12-simd", ProcCryptoEntry.CryptoType.SKCIPHER, 300));
        cryptoNode1.add(new ProcCryptoEntry("poly1305", "poly1305-simd", ProcCryptoEntry.CryptoType.SHASH, 100));
        cryptoNode1.add(new ProcCryptoEntry("blake2s-256", "blake2s-256-x86", ProcCryptoEntry.CryptoType.SHASH, 200));
        cryptoNode1.add(new ProcCryptoEntry("lzo-rle", "lzo-rle-scomp", ProcCryptoEntry.CryptoType.SCOMP, 0));
        cryptoNode1.add(new ProcCryptoEntry("crc32", "crc32-intel", ProcCryptoEntry.CryptoType.SHASH, 200));

        ArrayList<ProcCryptoEntry> cryptoNode2 = new ArrayList<>();
        cryptoNode2.add(new ProcCryptoEntry("curve25519", "curve25519-x86", ProcCryptoEntry.CryptoType.KPP, 200));
        cryptoNode2.add(new ProcCryptoEntry("poly1305", "poly1305-simd", ProcCryptoEntry.CryptoType.SHASH, 100));
        cryptoNode2.add(new ProcCryptoEntry("blake2s-256", "blake2s-256-x86", ProcCryptoEntry.CryptoType.SHASH, 200));
        cryptoNode2.add(new ProcCryptoEntry("lzo-rle", "lzo-rle-scomp", ProcCryptoEntry.CryptoType.SCOMP, 0));
        cryptoNode2.add(new ProcCryptoEntry("crc32", "crc32-intel", ProcCryptoEntry.CryptoType.SHASH, 200));

        ArrayList<ProcCryptoEntry> cryptoNode3 = new ArrayList<>();
        cryptoNode3.add(new ProcCryptoEntry("curve25519", "curve25519-x86", ProcCryptoEntry.CryptoType.KPP, 200));
        cryptoNode3.add(new ProcCryptoEntry("xchacha12", "xchacha12-simd", ProcCryptoEntry.CryptoType.SKCIPHER, 300));
        cryptoNode3.add(new ProcCryptoEntry("poly1305", "poly1305-simd", ProcCryptoEntry.CryptoType.SHASH, 100));
        cryptoNode3.add(new ProcCryptoEntry("blake2s-256", "blake2s-256-x86", ProcCryptoEntry.CryptoType.SHASH, 200));
        cryptoNode3.add(new ProcCryptoEntry("lzo-rle", "lzo-rle-scomp", ProcCryptoEntry.CryptoType.SCOMP, 0));
        cryptoNode3.add(new ProcCryptoEntry("crc32", "crc32-pclmul", ProcCryptoEntry.CryptoType.SHASH, 200));

        Map<String, List<ProcCryptoEntry>> cryptoMap = new HashMap<>();
        cryptoMap.put("linstor1", cryptoNode1);
        cryptoMap.put("linstor2", cryptoNode2);
        cryptoMap.put("linstor3", cryptoNode3);

        assertFalse(
            ProcCryptoUtils.cryptoDriverSupported(cryptoMap, ProcCryptoEntry.CryptoType.SHASH, "xxx"));
        assertTrue(
            ProcCryptoUtils.cryptoDriverSupported(cryptoMap, ProcCryptoEntry.CryptoType.SHASH, "blake2s-256-x86"));
        assertFalse(
            ProcCryptoUtils.cryptoDriverSupported(cryptoMap, ProcCryptoEntry.CryptoType.SHASH, "crc32-pclmul"));
        assertTrue(
            ProcCryptoUtils.cryptoDriverSupported(cryptoMap, ProcCryptoEntry.CryptoType.SHASH, "crc32"));
        assertFalse(
            ProcCryptoUtils.cryptoDriverSupported(cryptoMap, ProcCryptoEntry.CryptoType.SHASH, "curve25519-x86"));
        assertTrue(
            ProcCryptoUtils.cryptoDriverSupported(cryptoMap, ProcCryptoEntry.CryptoType.SHASH, "poly1305"));
    }
}
