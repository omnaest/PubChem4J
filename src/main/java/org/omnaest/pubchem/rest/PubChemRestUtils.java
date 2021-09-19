package org.omnaest.pubchem.rest;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.omnaest.pubchem.rest.domain.Synonyms;
import org.omnaest.utils.CacheUtils;
import org.omnaest.utils.JSONHelper;
import org.omnaest.utils.cache.Cache;
import org.omnaest.utils.rest.client.RestClient;
import org.omnaest.utils.rest.client.RestHelper.RESTAccessExeption;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;

public class PubChemRestUtils
{
    private static final Logger LOG = LoggerFactory.getLogger(PubChemRestUtils.class);

    public static PubChemRestAccessor newInstance()
    {
        return new PubChemRestAccessor()
        {
            private Cache  cache   = null;
            private String baseUrl = "https://pubchem.ncbi.nlm.nih.gov/rest/pug/";

            @Override
            public PubChemRestAccessor withCache(Cache cache)
            {
                this.cache = cache;
                return this;
            }

            @Override
            public PubChemRestAccessor withLocalCache()
            {
                return this.withCache(CacheUtils.newLocalJsonFolderCache("pubchem"));
            }

            @Override
            public Optional<Synonyms> fetchSynonyms(String compoundName)
            {
                RestClient restClient = this.newRestClient();

                String url = RestClient.urlBuilder()
                                       .setBaseUrl(this.baseUrl)
                                       .addPathToken("compound")
                                       .addPathToken("name")
                                       .addPathToken(compoundName)
                                       .addPathToken("synonyms")
                                       .addPathToken("JSON")
                                       .build();
                try
                {
                    LOG.info("Fetching pubchem synonyms for " + compoundName);
                    JsonNode response = restClient.requestGet(url, JsonNode.class);
                    JsonNode informationArray = response.findPath("Information");
                    return JSONHelper.toArrayNode(informationArray)
                                     .map(arrayNode -> arrayNode.get(0))
                                     .map(JSONHelper.toObjectWithTypeMapper(Synonyms.class));
                }
                catch (RESTAccessExeption e)
                {
                    if (e.getStatusCode() == 404)
                    {
                        LOG.info("Unable to find chemical synonym for " + compoundName);
                        return Optional.empty();
                    }
                    else
                    {
                        throw e;
                    }
                }
            }

            private RestClient newRestClient()
            {
                return RestClient.newJSONRestClient()
                                 .withCache(this.cache)
                                 .withRetry(5, 12, TimeUnit.SECONDS);
            }

        };
    }

    public static interface PubChemRestAccessor
    {
        public Optional<Synonyms> fetchSynonyms(String compoundName);

        public PubChemRestAccessor withCache(Cache cache);

        public PubChemRestAccessor withLocalCache();
    }

}
