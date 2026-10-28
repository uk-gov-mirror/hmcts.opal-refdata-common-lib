package uk.gov.hmcts.opal.refdata.config;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;

import com.azure.core.credential.TokenCredential;
import com.azure.messaging.servicebus.ServiceBusClientBuilder;
import com.azure.messaging.servicebus.ServiceBusSessionReceiverClient;
import com.azure.messaging.servicebus.models.ServiceBusReceiveMode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RefDataServiceBusSessionReceiverConfigTest {

    private final RefDataServiceBusSessionReceiverConfig config = new RefDataServiceBusSessionReceiverConfig();
    private RefDataServiceBusProperties properties;

    @BeforeEach
    void setUp() {
        properties = new RefDataServiceBusProperties();
        properties.setTopicName("ref-data-topic");
        properties.setSubscriptionName("ref-data-subscription");
        properties.setTryTimeoutSeconds(5L);
    }

    @Test
    void shouldConfigureManagedIdentityAuthentication() {
        // Given
        properties.setFullyQualifiedNamespace("ref-data.servicebus.windows.net");
        ServiceBusClientBuilder builder = mockBuilder();

        // When
        config.refDataServiceBusSessionReceiverClient(properties, "", builder);

        // Then
        verify(builder).credential(eq("ref-data.servicebus.windows.net"), any(TokenCredential.class));
        verify(builder, never()).connectionString(anyString());
    }

    @Test
    void shouldConfigureConnectionStringAuthentication() {
        // Given
        String connectionString = "Endpoint=sb://ref-data.servicebus.windows.net/;"
            + "SharedAccessKeyName=test;SharedAccessKey=dGVzdA==";
        ServiceBusClientBuilder builder = mockBuilder();

        // When
        config.refDataServiceBusSessionReceiverClient(properties, connectionString, builder);

        // Then
        verify(builder).connectionString(connectionString);
        verify(builder, never()).credential(anyString(), any(TokenCredential.class));
    }

    @Test
    void shouldRejectMissingAuthenticationConfiguration() {
        // When / Then
        assertThatThrownBy(() -> config.refDataServiceBusSessionReceiverClient(
            properties, "", mock(ServiceBusClientBuilder.class)))
            .isInstanceOf(IllegalStateException.class)
            .hasMessage(RefDataServiceBusSessionReceiverConfig.NO_AUTHENTICATION_PROPERTY_SET);
    }

    private static ServiceBusClientBuilder mockBuilder() {
        ServiceBusClientBuilder builder = mock(ServiceBusClientBuilder.class);
        ServiceBusClientBuilder.ServiceBusSessionReceiverClientBuilder sessionBuilder =
            mock(ServiceBusClientBuilder.ServiceBusSessionReceiverClientBuilder.class);
        when(builder.sessionReceiver()).thenReturn(sessionBuilder);
        when(sessionBuilder.topicName(anyString())).thenReturn(sessionBuilder);
        when(sessionBuilder.subscriptionName(anyString())).thenReturn(sessionBuilder);
        when(sessionBuilder.disableAutoComplete()).thenReturn(sessionBuilder);
        when(sessionBuilder.receiveMode(any(ServiceBusReceiveMode.class))).thenReturn(sessionBuilder);
        when(sessionBuilder.buildClient()).thenReturn(mock(ServiceBusSessionReceiverClient.class));
        return builder;
    }
}
