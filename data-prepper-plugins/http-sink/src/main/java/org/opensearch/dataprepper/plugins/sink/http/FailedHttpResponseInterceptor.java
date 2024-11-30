/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.http;

import org.apache.hc.core5.http.EntityDetails;
import org.apache.hc.core5.http.HttpResponse;
import org.apache.hc.core5.http.HttpResponseInterceptor;
import org.apache.hc.core5.http.protocol.HttpContext;
import org.opensearch.dataprepper.plugins.sink.http.configuration.HttpSinkConfiguration;

import java.io.IOException;
import java.util.List;

public class FailedHttpResponseInterceptor implements HttpResponseInterceptor {

    public static final int STATUS_CODE_200 = 200;

    private final String url;
    private final List<Integer> retryableCodes;

  public FailedHttpResponseInterceptor(List<Integer> retryableCodes,  final String url){
    this.retryableCodes = retryableCodes;
    this.url = url;
    }

    @Override
    public void process(HttpResponse response, EntityDetails entity, HttpContext context) throws IOException {
        if (retryableCodes.contains(response.getCode())) {
            throw new IOException(String.format("url:  %s , status code: %s", url,response.getCode()));
        }
    }
}
