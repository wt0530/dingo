/*
 * Copyright 2021 DataCanvas
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

package io.dingodb.meta;

import io.dingodb.common.log.LogUtils;
import lombok.extern.slf4j.Slf4j;

import java.util.Iterator;
import java.util.ServiceLoader;

public interface SequenceServiceProvider {

    @Slf4j
    class Impl {
        private static final Impl INSTANCE = new Impl();

        private final SequenceServiceProvider provider;

        public Impl() {
            Iterator<SequenceServiceProvider> iterator = ServiceLoader.load(SequenceServiceProvider.class).iterator();
            this.provider = iterator.next();
            if (iterator.hasNext()) {
                LogUtils.warn(log, "Load multi sequence service provider, use {}.", provider.getClass().getName());
            }
        }
    }

    SequenceService get();

    static SequenceServiceProvider getDefault() {
        return SequenceServiceProvider.Impl.INSTANCE.provider;
    }
}
