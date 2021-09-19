package org.omnaest.pubchem.rest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;

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
}
