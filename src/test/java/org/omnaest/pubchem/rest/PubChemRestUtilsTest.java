package org.omnaest.pubchem.rest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Map;

import org.junit.Ignore;
import org.junit.Test;
import org.omnaest.pubchem.rest.domain.Synonyms;

/**
 * @see PubChemRestUtils
 * @author omnaest
 */
public class PubChemRestUtilsTest
{
    @Test
    @Ignore
    public void testNewInstance() throws Exception
    {
        Synonyms synonyms = PubChemRestUtils.newInstance()
                                            .fetchSynonyms("tryptophan")
                                            .get();
        assertEquals(6305, synonyms.getCid());
        assertTrue(synonyms.getSynonyms()
                           .containsAll(Arrays.asList("L-tryptophan", "Indole-3-alanine")));
    }

    @Test
    @Ignore
    public void testNonExisting() throws Exception
    {
        assertFalse(PubChemRestUtils.newInstance()
                                    .withLocalCache()
                                    .fetchSynonyms("non-existing")
                                    .isPresent());
    }

    @Test
    @Ignore
    public void testTitle() throws Exception
    {
        Map<String, String> result = PubChemRestUtils.newInstance()
                                                     //                                                .withLocalCache()
                                                     .fetchTitles("222", "280", "5957", "962", "1061", "6022", "57339278", "3080745", "977", "70678894",
                                                                  "70678937");
        assertEquals(11, result.size());
        assertEquals("3-Deoxy-D-erythro-hex-2-ulosonic acid 6-phosphate", result.get("3080745"));
        assertEquals("Ammonia", result.get("222"));
        assertEquals("Adenosine-5'-diphosphate", result.get("6022"));
        assertEquals("Water", result.get("962"));
        assertEquals("1-Deoxypentalenate", result.get("70678894"));
        assertEquals("Oxygen", result.get("977"));
        assertEquals("(+)-6-Endo-hydroxycamphor", result.get("57339278"));
        assertEquals("Carbon dioxide", result.get("280"));
        assertEquals("Pentalenate", result.get("70678937"));
        assertEquals("Phosphate", result.get("1061"));
        assertEquals("Adenosine-5'-triphosphate", result.get("5957"));
    }
}
