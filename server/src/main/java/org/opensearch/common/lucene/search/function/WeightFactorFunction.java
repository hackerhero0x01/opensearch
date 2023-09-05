/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.common.lucene.search.function;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Explanation;
import org.opensearch.common.Nullable;
import org.opensearch.core.common.Strings;

import java.io.IOException;
import java.util.Objects;

/**
 * Weight factor function
 *
 * @opensearch.internal
 */
public class WeightFactorFunction extends ScoreFunction {

    private static final ScoreFunction SCORE_ONE = new ScoreOne(CombineFunction.MULTIPLY);
    private final ScoreFunction scoreFunction;
    private float weight = 1.0f;

    public WeightFactorFunction(float weight, ScoreFunction scoreFunction) {
        this(weight, scoreFunction, null);
    }

    public WeightFactorFunction(float weight, ScoreFunction scoreFunction, @Nullable String functionName) {
        super(CombineFunction.MULTIPLY);
        if (scoreFunction == null) {
            if (Strings.isNullOrEmpty(functionName)) {
                this.scoreFunction = SCORE_ONE;
            } else {
                this.scoreFunction = new ScoreOne(CombineFunction.MULTIPLY, functionName);
            }
        } else {
            this.scoreFunction = scoreFunction;
        }
        this.weight = weight;
    }

    public WeightFactorFunction(float weight) {
        this(weight, null, null);
    }

    public WeightFactorFunction(float weight, @Nullable String functionName) {
        this(weight, null, functionName);
    }

    @Override
    public LeafScoreFunction getLeafScoreFunction(LeafReaderContext ctx) throws IOException {
        final LeafScoreFunction leafFunction = scoreFunction.getLeafScoreFunction(ctx);
        return new LeafScoreFunction() {
            @Override
            public double score(int docId, float subQueryScore) throws IOException {
                return leafFunction.score(docId, subQueryScore) * getWeight();
            }

            @Override
            public Explanation explainScore(int docId, Explanation subQueryScore) throws IOException {
                Explanation functionExplanation = leafFunction.explainScore(docId, subQueryScore);
                return Explanation.match(
                    functionExplanation.getValue().floatValue() * (float) getWeight(),
                    "product of:",
                    functionExplanation,
                    explainWeight()
                );
            }
        };
    }

    @Override
    public boolean needsScores() {
        return scoreFunction.needsScores();
    }

    public Explanation explainWeight() {
        return Explanation.match(getWeight(), "weight");
    }

    @Override
    public float getWeight() {
        return weight;
    }

    public ScoreFunction getScoreFunction() {
        return scoreFunction;
    }

    @Override
    protected boolean doEquals(ScoreFunction other) {
        WeightFactorFunction weightFactorFunction = (WeightFactorFunction) other;
        return this.weight == weightFactorFunction.weight && Objects.equals(this.scoreFunction, weightFactorFunction.scoreFunction);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(weight, scoreFunction);
    }

    /**
     * A constant score of 1.0
     *
     * @opensearch.internal
     */
    private static class ScoreOne extends ScoreFunction {
        private final String functionName;

        protected ScoreOne(CombineFunction scoreCombiner) {
            this(scoreCombiner, null);
        }

        protected ScoreOne(CombineFunction scoreCombiner, @Nullable String functionName) {
            super(scoreCombiner);
            this.functionName = functionName;
        }

        @Override
        public LeafScoreFunction getLeafScoreFunction(LeafReaderContext ctx) {
            return new LeafScoreFunction() {
                @Override
                public double score(int docId, float subQueryScore) {
                    return 1.0;
                }

                @Override
                public Explanation explainScore(int docId, Explanation subQueryScore) {
                    return Explanation.match(
                        1.0f,
                        "constant score 1.0" + Functions.nameOrEmptyFunc(functionName) + " - no function provided"
                    );
                }
            };
        }

        @Override
        public boolean needsScores() {
            return false;
        }

        @Override
        protected boolean doEquals(ScoreFunction other) {
            return true;
        }

        @Override
        protected int doHashCode() {
            return 0;
        }
    }
}
