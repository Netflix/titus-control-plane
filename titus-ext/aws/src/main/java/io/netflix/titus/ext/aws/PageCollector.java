/*
 * Copyright 2017 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.netflix.titus.ext.aws;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.function.Function;

import io.netflix.titus.common.util.StringExt;
import io.netflix.titus.common.util.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

class PageCollector<R, T> {

    private static final Logger logger = LoggerFactory.getLogger(PageCollector.class);

    private final Function<String, R> requestFun;
    private final Function<R, Observable<Pair<List<T>, String>>> pageFun;

    PageCollector(Function<String, R> requestFun,
                  Function<R, Observable<Pair<List<T>, String>>> pageFun) {
        this.requestFun = requestFun;
        this.pageFun = pageFun;
    }

    Observable<List<T>> getAll() {
        return Observable.fromCallable((Callable<ArrayList<T>>) ArrayList::new).flatMap(loadedItems -> getPage(0, null, loadedItems));
    }

    private Observable<List<T>> getPage(int pageNumber, String currentToken, List<T> loadedItems) {
        R request = requestFun.apply(currentToken);
        return pageFun.apply(request).flatMap(pair -> {
            String nextToken = pair.getRight();
            loadedItems.addAll(pair.getLeft());

            logger.debug("Loading AWS page {} (loaded until now {})...", pageNumber, loadedItems.size());

            if (StringExt.isEmpty(nextToken)) {
                return Observable.just(loadedItems);
            }
            return getPage(pageNumber + 1, nextToken, loadedItems);
        });
    }
}
