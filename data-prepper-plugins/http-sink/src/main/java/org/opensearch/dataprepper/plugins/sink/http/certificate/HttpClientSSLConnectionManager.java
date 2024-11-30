/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.http.certificate;

import org.apache.hc.client5.http.config.ConnectionConfig;
import org.apache.hc.client5.http.config.TlsConfig;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManagerBuilder;
import org.apache.hc.client5.http.io.HttpClientConnectionManager;
import org.apache.hc.client5.http.ssl.NoopHostnameVerifier;
import org.apache.hc.client5.http.ssl.SSLConnectionSocketFactory;
import org.apache.hc.client5.http.ssl.SSLConnectionSocketFactoryBuilder;
import org.apache.hc.client5.http.ssl.TrustAllStrategy;
import org.apache.hc.core5.http.io.SocketConfig;
import org.apache.hc.core5.http.ssl.TLS;
import org.apache.hc.core5.ssl.SSLContextBuilder;
import org.apache.hc.core5.ssl.SSLContexts;
import org.apache.hc.core5.ssl.TrustStrategy;
import org.apache.hc.core5.util.TimeValue;
import org.apache.hc.core5.util.Timeout;
import org.opensearch.dataprepper.plugins.sink.http.configuration.HttpSinkConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;

/**
 * This class implements SSL certs authentication
 *
 */
public class HttpClientSSLConnectionManager {

    private static final Logger LOG = LoggerFactory.getLogger(HttpClientSSLConnectionManager.class);

    /**
     * This method creates HttpClientConnectionManager for SSL certs authentication
     * @param sinkConfiguration HttpSinkConfiguration
     * @param providerFactory CertificateProviderFactory
     * @return HttpClientConnectionManager
     */
    public HttpClientConnectionManager createHttpClientConnectionManager(final HttpSinkConfiguration sinkConfiguration,
                                                                         final CertificateProviderFactory providerFactory){
        final SSLContext sslContext = sinkConfiguration.getSslCertificateFile() != null ?
                getCAStrategy(new ByteArrayInputStream(providerFactory.getCertificateProvider().getCertificate().getCertificate().getBytes(StandardCharsets.UTF_8))) : getTrustAllStrategy();
        SSLConnectionSocketFactory sslSocketFactory = SSLConnectionSocketFactoryBuilder.create()
                .setSslContext(sslContext)
                .build();
       return PoolingHttpClientConnectionManagerBuilder.create()
           .setDefaultSocketConfig(SocketConfig.custom().setSoTimeout(Timeout.of(sinkConfiguration.getSocketTimeout())).build())
           .setDefaultConnectionConfig(ConnectionConfig.custom().setSocketTimeout(Timeout.of(sinkConfiguration.getSocketTimeout())).setConnectTimeout(Timeout.of(sinkConfiguration.getConnectTimeout())).setValidateAfterInactivity(TimeValue.of(sinkConfiguration.getValidateAfterInactivity())).build())
           .setMaxConnTotal(sinkConfiguration.getPoolMaxConnections())
           .setMaxConnPerRoute(sinkConfiguration.getPoolMaxPerRoute())
           .setSSLSocketFactory(sslSocketFactory)
                .setDefaultTlsConfig(TlsConfig.custom()
                        .setHandshakeTimeout(Timeout.ofSeconds(30))
                        .setSupportedProtocols(TLS.V_1_3)
                        .build())
                .build();
    }

    private SSLContext getCAStrategy(final InputStream certificate) {
        try {
            CertificateFactory factory = CertificateFactory.getInstance("X.509");
            Certificate trustedCa;
            trustedCa = factory.generateCertificate(certificate);
            KeyStore trustStore = KeyStore.getInstance("pkcs12");
            trustStore.load(null, null);
            trustStore.setCertificateEntry("ca", trustedCa);
            SSLContextBuilder sslContextBuilder = SSLContexts.custom()
                    .loadTrustMaterial(trustStore, null);
            return sslContextBuilder.build();
        } catch (Exception ex) {
            throw new RuntimeException(ex.getMessage(), ex);
        }
    }

    private SSLContext getTrustAllStrategy() {
        final TrustStrategy trustStrategy = new TrustAllStrategy();
        try {
            return SSLContexts.custom().loadTrustMaterial(null, trustStrategy).build();
        } catch (Exception ex) {
            throw new RuntimeException(ex.getMessage(), ex);
        }
    }

    public HttpClientConnectionManager createHttpClientConnectionManagerWithoutValidation(HttpSinkConfiguration httpSinkConfiguration)  throws NoSuchAlgorithmException, KeyStoreException, KeyManagementException {
        {
            return PoolingHttpClientConnectionManagerBuilder.create()
                .setDefaultConnectionConfig(ConnectionConfig.custom().setSocketTimeout(Timeout.of(httpSinkConfiguration.getSocketTimeout())).setConnectTimeout(Timeout.of(httpSinkConfiguration.getConnectTimeout())).setValidateAfterInactivity(TimeValue.of(httpSinkConfiguration.getValidateAfterInactivity())).build())
                .setMaxConnTotal(httpSinkConfiguration.getPoolMaxConnections())
                .setMaxConnPerRoute(httpSinkConfiguration.getPoolMaxPerRoute())
                .setDefaultSocketConfig(SocketConfig.custom().setSoTimeout(Timeout.of(httpSinkConfiguration.getSocketTimeout())).build())

                    .setSSLSocketFactory(SSLConnectionSocketFactoryBuilder.create()
                            .setSslContext(SSLContextBuilder.create()
                                    .loadTrustMaterial(TrustAllStrategy.INSTANCE)
                                    .build())
                            .setHostnameVerifier(NoopHostnameVerifier.INSTANCE)
                            .build())
                    .build();
        }
    }
}
