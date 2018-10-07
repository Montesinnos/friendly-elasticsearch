package com.montesinnos.friendly.elasticsearch.search;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.SuggestBuilders;
import org.elasticsearch.search.suggest.SuggestionBuilder;
import org.elasticsearch.search.suggest.completion.CompletionSuggestion;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Use this helper to execute searches and fetches
 */
public class Search {
    private final RestHighLevelClient client;

    private final ObjectMapper objectMapper = new ObjectMapper().setSerializationInclusion(JsonInclude.Include.NON_NULL);

    public Search(final RestHighLevelClient client) {
        this.client = client;
    }

    /**
     * Fetches a record from an index
     *
     * @param index Index name
     * @param type  Type name
     * @param id    String ID
     * @return a JSON string with record information
     */
    public String fetch(final String index, final String type, final String id) {
        final GetRequest getRequest = new GetRequest(
                index,
                type,
                id);

        String sourceAsString = "{}";
        try {
            final GetResponse getResponse = client.get(getRequest, RequestOptions.DEFAULT);

            if (getResponse.isExists()) {
                sourceAsString = getResponse.getSourceAsString();
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return sourceAsString;
    }

    public boolean exist(final String index, final String type, final String id) {
        final GetRequest getRequest = new GetRequest(
                index,
                type,
                id);
        getRequest.fetchSourceContext(new FetchSourceContext(false));
        getRequest.storedFields("_none_");

        try {
            final boolean exists = client.exists(getRequest, RequestOptions.DEFAULT);
            return exists;
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public List<Map<String, Object>> autocomplete(final String index, final String input) {
        final SearchRequest searchRequest = new SearchRequest(index);
        final SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        final SuggestionBuilder completionSuggestionBuilder =
                SuggestBuilders.completionSuggestion("suggest").prefix(input);
        final SuggestBuilder suggestBuilder = new SuggestBuilder();
        final String suggestionFiledName = "suggestions";
        suggestBuilder.addSuggestion(suggestionFiledName, completionSuggestionBuilder);
        searchSourceBuilder.suggest(suggestBuilder);
        searchRequest.source(searchSourceBuilder);

        final List<Map<String, Object>> suggestions = new ArrayList<>();

        try {
            final SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
            final Suggest suggest = searchResponse.getSuggest();
            if (suggest != null) { //if suggestion was found
                final CompletionSuggestion termSuggestion = suggest.getSuggestion(suggestionFiledName);
                for (CompletionSuggestion.Entry entry : termSuggestion.getEntries()) {
                    for (CompletionSuggestion.Entry.Option option : entry) {
                        suggestions.add(option.getHit().getSourceAsMap());
                    }
                }
            }
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
        return suggestions;
    }

    public List<Map<String, Object>> find(final String index, QueryBuilder qb) {
        final SearchRequest searchRequest = new SearchRequest(index);
        final SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(qb);
        searchSourceBuilder.from(0);
        searchSourceBuilder.size(5);
        searchSourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = null;
        try {
            searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        final RestStatus status = searchResponse.status();
        final TimeValue took = searchResponse.getTook();
        final Boolean terminatedEarly = searchResponse.isTerminatedEarly();
        final boolean timedOut = searchResponse.isTimedOut();

        final int totalShards = searchResponse.getTotalShards();
        final int successfulShards = searchResponse.getSuccessfulShards();
        final int failedShards = searchResponse.getFailedShards();
        for (final ShardSearchFailure failure : searchResponse.getShardFailures()) {
            // failures should be handled here
        }
        final SearchHits hits = searchResponse.getHits();
        final long totalHits = hits.getTotalHits();
        final float maxScore = hits.getMaxScore();

        return Arrays.stream(hits.getHits())
                .map(SearchHit::getSourceAsMap)
                .collect(Collectors.toList());
    }
}
