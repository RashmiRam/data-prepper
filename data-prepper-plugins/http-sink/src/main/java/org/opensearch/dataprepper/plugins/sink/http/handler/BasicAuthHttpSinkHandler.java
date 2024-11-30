/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.http.handler;

import org.apache.hc.client5.http.auth.AuthScope;
import org.apache.hc.client5.http.auth.UsernamePasswordCredentials;
import org.apache.hc.client5.http.impl.auth.BasicCredentialsProvider;
import org.apache.hc.client5.http.io.HttpClientConnectionManager;
import org.opensearch.dataprepper.plugins.sink.http.FailedHttpResponseInterceptor;
import org.opensearch.dataprepper.plugins.sink.http.util.HttpSinkUtil;

import java.util.List;

/**
 * * This class handles Basic Authentication
 */
public class BasicAuthHttpSinkHandler implements MultiAuthHttpSinkHandler {

    private final HttpClientConnectionManager httpClientConnectionManager;

    private final String username;

    private final String password;
    private final List<Integer> retryableCodes;

  public BasicAuthHttpSinkHandler(final String username,
                                    final String password,
                                    final HttpClientConnectionManager httpClientConnectionManager,
                                    final List<Integer> retryableCodes){
        this.httpClientConnectionManager = httpClientConnectionManager;
        this.username = username;
        this.password = password;
    this.retryableCodes = retryableCodes;
  }

    @Override
    public HttpAuthOptions authenticate(final HttpAuthOptions.Builder  httpAuthOptionsBuilder) {
        final BasicCredentialsProvider provider = new BasicCredentialsProvider();
        AuthScope authScope = new AuthScope(HttpSinkUtil.getHttpHostByURL(HttpSinkUtil.getURLByUrlString(httpAuthOptionsBuilder.getUrl())));
        provider.setCredentials(authScope, new UsernamePasswordCredentials(username, password.toCharArray()));
        httpAuthOptionsBuilder.setHttpClientBuilder(httpAuthOptionsBuilder.build().getHttpClientBuilder()
                .setConnectionManager(httpClientConnectionManager)
                .addResponseInterceptorLast(new FailedHttpResponseInterceptor(retryableCodes, httpAuthOptionsBuilder.getUrl()))
                .setDefaultCredentialsProvider(provider));
        return httpAuthOptionsBuilder.build();
    }
}
