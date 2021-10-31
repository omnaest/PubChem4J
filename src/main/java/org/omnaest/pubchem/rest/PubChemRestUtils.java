package org.omnaest.pubchem.rest;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.commons.lang3.StringUtils;
import org.omnaest.pubchem.rest.PubChemRestUtils.Compound.CompoundEntry;
import org.omnaest.pubchem.rest.PubChemRestUtils.Compound.CompoundEntry.CompoundProperty;
import org.omnaest.pubchem.rest.PubChemRestUtils.Compound.CompoundEntry.OuterId;
import org.omnaest.pubchem.rest.PubChemRestUtils.Compound.CompoundEntry.OuterId.InnerId;
import org.omnaest.pubchem.rest.domain.Synonyms;
import org.omnaest.utils.CacheUtils;
import org.omnaest.utils.ComparatorUtils;
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
            private String baseUrl = "https://pubchem.ncbi.nlm.nih.gov/rest/pug";

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

            @Override
            public Optional<Compound> fetchCompoundByName(String compoundName)
            {
                RestClient restClient = this.newRestClient();

                String url = RestClient.urlBuilder()
                                       .setBaseUrl(this.baseUrl)
                                       .addPathToken("compound")
                                       .addPathToken("name")
                                       .addPathToken(compoundName)
                                       .addPathToken("JSON")
                                       .build();
                try
                {
                    LOG.debug("Fetching pubchem compound by name: " + compoundName);
                    return restClient.request()
                                     .toUrl(url)
                                     .getAnd(Compound.class)
                                     .handleStatusCode(404, holder -> null)
                                     .asOptional();
                }
                catch (RESTAccessExeption e)
                {
                    if (e.getStatusCode() == 404)
                    {
                        LOG.error("Unable to find chemical compound for " + compoundName);
                        return Optional.empty();
                    }
                    else
                    {
                        throw e;
                    }
                }
            }

            @Override
            public Optional<String> fetchCompoundCidByName(String compoundName)
            {
                RestClient restClient = this.newRestClient();

                String url = RestClient.urlBuilder()
                                       .setBaseUrl(this.baseUrl)
                                       .addPathToken("compound")
                                       .addPathToken("name")
                                       .addPathToken(compoundName)
                                       .addPathToken("cids")
                                       .addPathToken("JSON")
                                       .build();
                try
                {
                    LOG.debug("Fetching pubchem compound cid by name: " + compoundName);
                    return restClient.request()
                                     .toUrl(url)
                                     .getAnd(JsonNode.class)
                                     .handleStatusCode(404, holder -> null)
                                     .handleStatusCode(400, holder -> null)
                                     .asOptional()
                                     .map(node -> node.findPath("IdentifierList"))
                                     .map(node -> node.findPath("CID"))
                                     .flatMap(informationArray -> JSONHelper.toArrayNode(informationArray)
                                                                            .map(arrayNode -> arrayNode.get(0))
                                                                            .map(JSONHelper.toObjectWithTypeMapper(String.class)));
                }
                catch (RESTAccessExeption e)
                {
                    if (e.getStatusCode() == 404)
                    {
                        LOG.error("Unable to find chemical compound cid for " + compoundName);
                        return Optional.empty();
                    }
                    else
                    {
                        throw e;
                    }
                }
            }

            @Override
            public Optional<String> fetchCompoundParentCidByCid(String cid)
            {
                RestClient restClient = this.newRestClient();

                String url = RestClient.urlBuilder()
                                       .setBaseUrl(this.baseUrl)
                                       .addPathToken("compound")
                                       .addPathToken("cid")
                                       .addPathToken(cid)
                                       .addPathToken("cids")
                                       .addPathToken("JSON")
                                       .addQueryParameter("cids_type", "parent")
                                       .build();
                try
                {
                    LOG.debug("Fetching pubchem compound parent cid by cid: " + cid);
                    return restClient.request()
                                     .toUrl(url)
                                     .getAnd(JsonNode.class)
                                     .handleStatusCode(404, holder -> null)
                                     .handleStatusCode(400, holder -> null)
                                     .asOptional()
                                     .map(node -> node.findPath("IdentifierList"))
                                     .map(node -> node.findPath("CID"))
                                     .flatMap(informationArray -> JSONHelper.toArrayNode(informationArray)
                                                                            .map(arrayNode -> arrayNode.get(0))
                                                                            .map(JSONHelper.toObjectWithTypeMapper(String.class)));
                }
                catch (RESTAccessExeption e)
                {
                    if (e.getStatusCode() == 404)
                    {
                        LOG.error("Unable to find chemical parent cid for cid " + cid);
                        return Optional.empty();
                    }
                    else
                    {
                        throw e;
                    }
                }
            }

            @Override
            public Optional<CidAndName> fetchCidAndPrimaryNameByAnyName(String compoundName)
            {
                return this.fetchCidAndPrimaryNameByAnyName(compoundName, NameType.TRADITIONAL, NameType.PREFERRED);
            }

            @Override
            public Optional<CidAndName> fetchOldestCidAndPrimaryNameByAnyName(String compoundName)
            {
                return this.fetchOldestCidAndPrimaryNameByAnyName(compoundName, NameType.TRADITIONAL, NameType.PREFERRED);
            }

            @Override
            public Optional<CidAndName> fetchOldestCidAndPrimaryNameByAnyName(String compoundName, NameType... nameTypes)
            {
                Function<CompoundEntry, Long> cidExtractor = entry -> Optional.ofNullable(entry.getId())
                                                                              .map(OuterId::getId)
                                                                              .map(InnerId::getCid)
                                                                              .orElse(999999999999l);
                List<CompoundEntry> entries = this.fetchCompoundByName(compoundName)
                                                  .map(Compound::getEntries)
                                                  .orElse(Collections.emptyList())
                                                  .stream()
                                                  .sorted(ComparatorUtils.builder()
                                                                         .of(cidExtractor)
                                                                         .natural())
                                                  .collect(Collectors.toList());
                return this.determineCidAndPrimaryName(entries, nameTypes);
            }

            @Override
            public Optional<CidAndName> fetchCidAndPrimaryNameByAnyName(String compoundName, NameType... nameTypes)
            {
                List<CompoundEntry> entries = this.fetchCompoundByName(compoundName)
                                                  .map(Compound::getEntries)
                                                  .orElse(Collections.emptyList());
                return this.determineCidAndPrimaryName(entries, nameTypes);
            }

            private Optional<CidAndName> determineCidAndPrimaryName(List<CompoundEntry> entries, NameType... nameTypes)
            {
                return entries.stream()
                              .findFirst()
                              .flatMap(this.createCidAndPrimaryNameExtractor(entries.stream()
                                                                                    .skip(1)
                                                                                    .collect(Collectors.toList()),
                                                                             nameTypes));
            }

            private Function<CompoundEntry, Optional<CidAndName>> createCidAndPrimaryNameExtractor(List<CompoundEntry> parentEntries, NameType... nameTypes)
            {
                return entry ->
                {
                    String name = Optional.ofNullable(entry.getProps())
                                          .orElse(Collections.emptyList())
                                          .stream()
                                          .filter(property -> Optional.ofNullable(property)
                                                                      .map(CompoundProperty::getUrn)
                                                                      .map(urn -> StringUtils.equalsIgnoreCase("IUPAC Name", urn.getLabel()))
                                                                      .orElse(false))
                                          .filter(property -> Optional.ofNullable(property)
                                                                      .map(CompoundProperty::getUrn)
                                                                      .map(urn -> StringUtils.equalsAnyIgnoreCase(urn.getName(), Arrays.asList(nameTypes)
                                                                                                                                       .stream()
                                                                                                                                       .map(NameType::getIdentifier)
                                                                                                                                       .toArray(String[]::new)))
                                                                      .orElse(false))
                                          .sorted(ComparatorUtils.builder()
                                                                 .of(this.createNameTypePriorityFunction(nameTypes))
                                                                 .natural())
                                          .findFirst()
                                          .map(property -> property.getValue()
                                                                   .getSval())
                                          .orElse(null);
                    Optional<CidAndName> parent = this.determineCidAndPrimaryName(parentEntries, nameTypes);
                    return Optional.ofNullable(entry.getId())
                                   .map(OuterId::getId)
                                   .map(InnerId::getCid)
                                   .map(String::valueOf)
                                   .map(cid -> new CidAndName(cid, name, parent));
                };
            }

            private Function<CompoundProperty, Integer> createNameTypePriorityFunction(NameType... nameTypes)
            {
                return property -> Optional.ofNullable(property)
                                           .map(CompoundProperty::getUrn)
                                           .flatMap(urn -> IntStream.range(0, nameTypes.length)
                                                                    .filter(index ->
                                                                    {
                                                                        return nameTypes[index].matches(urn.getName());
                                                                    })
                                                                    .boxed()
                                                                    .findFirst())
                                           .get();
            }

        };
    }

    public static interface PubChemRestAccessor
    {
        public Optional<Synonyms> fetchSynonyms(String compoundName);

        public Optional<Compound> fetchCompoundByName(String compoundName);

        public Optional<CidAndName> fetchCidAndPrimaryNameByAnyName(String compoundName);

        public Optional<CidAndName> fetchOldestCidAndPrimaryNameByAnyName(String compoundName);

        public PubChemRestAccessor withCache(Cache cache);

        public PubChemRestAccessor withLocalCache();

        public Optional<String> fetchTitle(String cid);

        public Map<String, String> fetchTitles(String... cids);

        public Map<String, String> fetchTitles(Collection<String> cids);

        public Stream<Description> fetchDescriptions(Collection<String> cids);

        public Stream<Description> fetchDescriptions(String... cid);

        Optional<CidAndName> fetchCidAndPrimaryNameByAnyName(String compoundName, NameType... nameTypes);

        Optional<CidAndName> fetchOldestCidAndPrimaryNameByAnyName(String compoundName, NameType... nameTypes);

        Optional<String> fetchCompoundParentCidByCid(String cid);

        Optional<String> fetchCompoundCidByName(String compoundName);
    }

    public static enum NameType
    {
        TRADITIONAL("Traditional"), PREFERRED("Preferred");

        private String identifier;

        private NameType(String identifier)
        {
            this.identifier = identifier;
        }

        public String getIdentifier()
        {
            return this.identifier;
        }

        public boolean matches(String identifier)
        {
            return Optional.ofNullable(identifier)
                           .map(otherIdentifier -> otherIdentifier.equalsIgnoreCase(this.identifier))
                           .orElse(false);
        }
    }

    public static class CidAndName
    {
        private String               cid;
        private String               name;
        private Optional<CidAndName> parent;

        public CidAndName(String cid, String name, Optional<CidAndName> parent)
        {
            super();
            this.cid = cid;
            this.name = name;
            this.parent = parent;
        }

        public String getCid()
        {
            return this.cid;
        }

        public String getName()
        {
            return this.name;
        }

        public Optional<CidAndName> getParent()
        {
            return this.parent;
        }

        @Override
        public String toString()
        {
            StringBuilder builder = new StringBuilder();
            builder.append("CidAndName [cid=")
                   .append(this.cid)
                   .append(", name=")
                   .append(this.name)
                   .append("]");
            return builder.toString();
        }

    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Compound
    {
        @JsonProperty("PC_Compounds")
        private List<CompoundEntry> entries;

        public List<CompoundEntry> getEntries()
        {
            return this.entries;
        }

        @JsonIgnoreProperties(ignoreUnknown = true)
        public static class CompoundEntry
        {
            @JsonProperty
            private OuterId id;

            @JsonProperty
            private List<CompoundProperty> props;

            public OuterId getId()
            {
                return this.id;
            }

            public List<CompoundProperty> getProps()
            {
                return this.props;
            }

            public static class OuterId
            {
                @JsonProperty
                private InnerId id;

                public InnerId getId()
                {
                    return this.id;
                }

                public static class InnerId
                {
                    @JsonProperty
                    private long cid;

                    public long getCid()
                    {
                        return this.cid;
                    }

                }
            }

            @JsonIgnoreProperties(ignoreUnknown = true)
            public static class CompoundProperty
            {
                @JsonProperty
                private Urn urn;

                @JsonProperty
                private Value value;

                public Urn getUrn()
                {
                    return this.urn;
                }

                public Value getValue()
                {
                    return this.value;
                }

                @JsonIgnoreProperties(ignoreUnknown = true)
                public static class Value
                {
                    @JsonProperty
                    private String ival;

                    @JsonProperty
                    private String sval;

                    public String getIval()
                    {
                        return this.ival;
                    }

                    public String getSval()
                    {
                        return this.sval;
                    }

                }

                @JsonIgnoreProperties(ignoreUnknown = true)
                public static class Urn
                {
                    @JsonProperty
                    private String label;

                    @JsonProperty
                    private String name;

                    @JsonProperty
                    private int datatype;

                    @JsonProperty
                    private String release;

                    public String getLabel()
                    {
                        return this.label;
                    }

                    public String getName()
                    {
                        return this.name;
                    }

                    public int getDatatype()
                    {
                        return this.datatype;
                    }

                    public String getRelease()
                    {
                        return this.release;
                    }

                }
            }
        }
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
