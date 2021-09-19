package org.omnaest.pubchem.rest.domain;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Synonyms
{
    @JsonProperty("CID")
    private long cid;

    @JsonProperty("Synonym")
    private List<String> synonyms;

    public long getCid()
    {
        return this.cid;
    }

    public List<String> getSynonyms()
    {
        return this.synonyms;
    }

    @Override
    public String toString()
    {
        return "Synomyms [cid=" + this.cid + ", synonyms=" + this.synonyms + "]";
    }

}