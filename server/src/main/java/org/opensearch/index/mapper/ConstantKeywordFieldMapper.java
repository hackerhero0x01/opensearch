/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.Query;
import org.opensearch.OpenSearchParseException;
import org.opensearch.common.Nullable;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.index.fielddata.IndexFieldData;
import org.opensearch.index.fielddata.plain.ConstantIndexFieldData;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.QueryShardException;
import org.opensearch.search.aggregations.support.CoreValuesSourceType;
import org.opensearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Index specific field mapper
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class ConstantKeywordFieldMapper extends ParametrizedFieldMapper {

        public static final String CONTENT_TYPE = "constant_keyword";

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            if (!node.containsKey("value")) {
                throw new OpenSearchParseException("Field [" + name + "] is missing required parameter [value]");
            }
            Object value = node.remove("value");
            if (!(value instanceof String)) {
                throw new OpenSearchParseException("Field [" + name + "] is expected to be a string value");
            }
            return new Builder(name, (String) value);
        }
    }


    private static ConstantKeywordFieldMapper toType(FieldMapper in) {
        return (ConstantKeywordFieldMapper) in;
    }

    /**
     * Builder for the binary field mapper
     *
     * @opensearch.internal
     */
    public static class Builder extends ParametrizedFieldMapper.Builder {

        private final Parameter<String> value;

        public Builder(String name,  String value) {
            super(name);
            this.value = Parameter.stringParam("value", false, m -> toType(m).value, value);
        }

        @Override
        public List<Parameter<?>> getParameters() {
            return Arrays.asList(value);
        }

        @Override
        public ConstantKeywordFieldMapper build(BuilderContext context) {
            return new ConstantKeywordFieldMapper(
                name,
                new ConstantKeywordFieldMapper.ConstantKeywordFieldType(buildFullName(context), value.getValue()),
                multiFieldsBuilder.build(this, context),
                copyTo.build(),
                this
            );
        }
    }

    /**
     * Field type for Index field mapper
     *
     * @opensearch.internal
     */
    static final class ConstantKeywordFieldType extends ConstantFieldType {

        protected final String value;

        public ConstantKeywordFieldType(String name, String value) {
            super(name, Collections.emptyMap());
            this.value = value;
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        protected boolean matches(String searchValue, boolean caseInsensitive, QueryShardContext context) {
            System.out.println(value);

            return value.equals(searchValue);
        }

        @Override
        public Query existsQuery(QueryShardContext context) {
            return new MatchAllDocsQuery();
        }

        public Query termQueryCaseInsensitive(Object value, QueryShardContext context) {
            throw new QueryShardException(
                context,
                "Fields of type [" + typeName() + "], does not support case insensitive term queries"
            );
        }

        public Query prefixQuery(Object value, QueryShardContext context) {
            throw new QueryShardException(
                context,
                "Fields of type [" + typeName() + "], does not support prefix queries"
            );
        }

        public Query wildcardQuery(String value, @Nullable MultiTermQuery.RewriteMethod method, QueryShardContext context) {
            throw new QueryShardException(
                context,
                "Fields of type [" + typeName() + "], does not support wildcard queries"
            );
        }

        public Query wildcardQuery(
            String value,
            @Nullable MultiTermQuery.RewriteMethod method,
            boolean caseInsensitve,
            QueryShardContext context
        ) {
            System.out.println(("tes"));
            throw new QueryShardException(
                context,
                "Fields of type [" + typeName() + "], does not support wildcard queries"
            );
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName, Supplier<SearchLookup> searchLookup) {
            return new ConstantIndexFieldData.Builder(fullyQualifiedIndexName, name(), CoreValuesSourceType.BYTES);
        }

        @Override
        public ValueFetcher valueFetcher(QueryShardContext context, SearchLookup searchLookup, String format) {
            throw new UnsupportedOperationException("Cannot fetch values for internal field [" + name() + "].");
        }
    }

    private final String value;

    protected ConstantKeywordFieldMapper(
        String simpleName,
        MappedFieldType mappedFieldType,
        MultiFields multiFields,
        CopyTo copyTo,
        ConstantKeywordFieldMapper.Builder builder
    ) {
        super(simpleName, mappedFieldType, multiFields, copyTo);
        this.value = builder.value.getValue();
    }

    public ParametrizedFieldMapper.Builder getMergeBuilder() {
        return new ConstantKeywordFieldMapper.Builder(simpleName(), this.value).init(this);
    }

    @Override
    protected void parseCreateField(ParseContext context) throws IOException {

        final String value;
        if (context.externalValueSet()) {
            value = context.externalValue().toString();
        } else {
            value = context.parser().textOrNull();
        }
        if (value == null) {
            throw new IllegalArgumentException("constant keyword field [" + name() + "] must have a value");
        }

        if (!value.equals(fieldType().value)) {
            throw new IllegalArgumentException("constant keyword field [" + name() + "] must have a value of [" + this.value + "]");
        }

    }

    @Override
    public ConstantKeywordFieldMapper.ConstantKeywordFieldType fieldType() {
        return (ConstantKeywordFieldMapper.ConstantKeywordFieldType) super.fieldType();
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }
}
