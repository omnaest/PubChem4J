package org.omnaest.pubchem.rest;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.omnaest.pubchem.rest.domain.Synonyms;
import org.omnaest.utils.CacheUtils;
import org.omnaest.utils.JSONHelper;
import org.omnaest.utils.StreamUtils;
import org.omnaest.utils.cache.Cache;
import org.omnaest.utils.rest.client.RestClient;
import org.omnaest.utils.rest.client.RestHelper.RESTAccessExeption;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
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
                    LOG.debug("Fetching pubchem synonyms for " + compoundName);
                    return restClient.request()
                                     .toUrl(url)
                                     .getAnd(JsonNode.class)
                                     .handleStatusCode(404, holder -> null)
                                     .asOptional()
                                     .map(response -> response.findPath("Information"))
                                     .flatMap(informationArray -> JSONHelper.toArrayNode(informationArray)
                                                                            .map(arrayNode -> arrayNode.get(0))
                                                                            .map(JSONHelper.toObjectWithTypeMapper(Synonyms.class)));
                }
                catch (RESTAccessExeption e)
                {
                    if (e.getStatusCode() == 404)
                    {
                        LOG.error("Unable to find chemical synonym for " + compoundName);
                        return Optional.empty();
                    }
                    else
                    {
                        throw e;
                    }
                }
            }

            @Override
            public Optional<String> fetchTitle(String cid)
            {
                return this.fetchDescriptions(cid)
                           .filter(Description::hasTitle)
                           .map(Description::getTitle)
                           .findFirst();
            }

            @Override
            public Map<String, String> fetchTitles(String... cids)
            {
                return this.fetchTitles(Arrays.asList(cids));
            }

            @Override
            public Map<String, String> fetchTitles(Collection<String> cids)
            {
                return this.fetchDescriptions(cids)
                           .filter(Description::hasTitle)
                           .collect(Collectors.toMap(Description::getCid, Description::getTitle));
            }

            @Override
            public Stream<Description> fetchDescriptions(String... cid)
            {
                return this.fetchDescriptions(Arrays.asList(cid));
            }

            @Override
            public Stream<Description> fetchDescriptions(Collection<String> cids)
            {
                try
                {
                    return StreamUtils.framedNonNullAsList(10, Optional.ofNullable(cids)
                                                                       .orElse(Collections.emptyList())
                                                                       .stream()
                                                                       .distinct())
                                      .flatMap(cidBatch ->
                                      {

                                          RestClient restClient = this.newRestClient();

                                          String url = RestClient.urlBuilder()
                                                                 .setBaseUrl(this.baseUrl)
                                                                 .addPathToken("compound")
                                                                 .addPathToken("cid")
                                                                 .addPathToken(cidBatch.stream()
                                                                                       .collect(Collectors.joining(",")))
                                                                 .addPathToken("description")
                                                                 .addPathToken("JSON")
                                                                 .build();
                                          LOG.debug("Fetching pubchem descriptions for " + cidBatch);
                                          return restClient.request()
                                                           .toUrl(url)
                                                           .getAnd(JsonNode.class)
                                                           .handleStatusCode(400, holder -> null)
                                                           .asOptional()
                                                           .map(response -> response.findPath("Information"))
                                                           .flatMap(informationArray -> JSONHelper.toArrayNode(informationArray)
                                                                                                  .map(arrayNode -> StreamUtils.fromIterator(arrayNode.iterator())))
                                                           .orElse(Stream.empty())
                                                           .map(JSONHelper.toObjectWithTypeMapper(Description.class));
                                      });
                }
                catch (RESTAccessExeption e)
                {
                    if (e.getStatusCode() == 404)
                    {
                        LOG.error("Unable to find chemical descriptions for " + cids);
                        return Stream.empty();
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
                                 .withRetry(10, 12, TimeUnit.SECONDS);
            }

        };
    }

    public static interface PubChemRestAccessor
    {
        public Optional<Synonyms> fetchSynonyms(String compoundName);

        public PubChemRestAccessor withCache(Cache cache);

        public PubChemRestAccessor withLocalCache();

        public Optional<String> fetchTitle(String cid);

        public Map<String, String> fetchTitles(String... cids);

        public Map<String, String> fetchTitles(Collection<String> cids);

        public Stream<Description> fetchDescriptions(Collection<String> cids);

        public Stream<Description> fetchDescriptions(String... cid);
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Description
    {
        @JsonProperty("CID")
        private String cid;

        @JsonProperty("Title")
        private String title;

        @JsonProperty("Description")
        private String description;

        public boolean hasTitle()
        {
            return this.title != null;
        }

        public String getCid()
        {
            return this.cid;
        }

        public String getTitle()
        {
            return this.title;
        }

        public String getDescription()
        {
            return this.description;
        }

        @Override
        public String toString()
        {
            return "Description [cid=" + this.cid + ", title=" + this.title + ", description=" + this.description + "]";
        }

    }

}
