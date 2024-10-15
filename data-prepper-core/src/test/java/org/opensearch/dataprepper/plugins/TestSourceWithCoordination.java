/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins;

import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.Source;
import org.opensearch.dataprepper.model.source.coordinator.SourceCoordinator;
import org.opensearch.dataprepper.model.source.coordinator.UsesSourceCoordination;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@DataPrepperPlugin(name = "test_source", pluginType = Source.class)
public class TestSourceWithCoordination implements Source<Record<String>>, UsesSourceCoordination {
    public static final List<Record<String>> TEST_DATA = Stream.of("TEST")
            .map(Record::new).collect(Collectors.toList());
    private boolean isStopRequested;
    private boolean failSourceForTest;

    private SourceCoordinator sourceCoordinator;

    public TestSourceWithCoordination() {
        this.isStopRequested = false;
        this.failSourceForTest = false;
    }

    public TestSourceWithCoordination(final boolean failSourceForTest) {
        this.isStopRequested = false;
        this.failSourceForTest = failSourceForTest;
    }

    @Override
    public void start(Buffer<Record<String>> buffer) {
        if(failSourceForTest) {
            throw new RuntimeException("Source is expected to fail");
        }
        final Iterator<Record<String>> iterator = TEST_DATA.iterator();
        while (iterator.hasNext() && !isStopRequested) {
            try {
                buffer.write(iterator.next(), 1_000);
            } catch (TimeoutException e) {
                throw new RuntimeException("Timed out writing to buffer");
            }
        }
    }

    @Override
    public void stop() {
        isStopRequested = true;
    }

    @Override
    public void setSourceCoordinator(final SourceCoordinator sourceCoordinator) {
        this.sourceCoordinator = sourceCoordinator;
    }

    @Override
    public Class<?> getPartitionProgressStateClass() {
        return Object.class;
    }
}
