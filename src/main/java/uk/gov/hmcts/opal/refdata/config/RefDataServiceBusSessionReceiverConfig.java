package uk.gov.hmcts.opal.refdata.config;

import com.azure.core.amqp.AmqpRetryOptions;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.messaging.servicebus.ServiceBusClientBuilder;
import com.azure.messaging.servicebus.ServiceBusSessionReceiverClient;
import com.azure.messaging.servicebus.models.ServiceBusReceiveMode;
import java.time.Duration;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

@Configuration
@EnableScheduling
@ConditionalOnProperty(prefix = "opal.ref-data.service-bus", name = "consumer-enabled", havingValue = "true")
@EnableConfigurationProperties(RefDataServiceBusProperties.class)
public class RefDataServiceBusSessionReceiverConfig {

    static final String NO_AUTHENTICATION_PROPERTY_SET = "No authentication property set";

    @Bean
    public ServiceBusClientBuilder serviceBusClientBuilder() {
        return new ServiceBusClientBuilder();
    }

    @Bean(name = "refDataServiceBusSessionReceiverClient", destroyMethod = "close")
    public ServiceBusSessionReceiverClient refDataServiceBusSessionReceiverClient(
        RefDataServiceBusProperties refDataServiceBusProperties,
        @Value("${opal.common.service-bus.connection-string:}") String connectionString,
        ServiceBusClientBuilder builder) {

        String fullyQualifiedNamespace = refDataServiceBusProperties.getFullyQualifiedNamespace();

        //namespace take precedence over connection string
        if (fullyQualifiedNamespace != null && !fullyQualifiedNamespace.isBlank()) {
            builder.credential(
                fullyQualifiedNamespace,
                new DefaultAzureCredentialBuilder().build()
            );
        } else {
            if (connectionString != null && !connectionString.isBlank()) {
                builder.connectionString(connectionString);
            } else {
                throw new IllegalStateException(NO_AUTHENTICATION_PROPERTY_SET);
            }
        }

        // This sets the timeout for an individual AMQP operation, such as receiving, completing, abandoning,
        // or renewing a message. It is also used when acceptNextSession() waits for an available session.
        AmqpRetryOptions retryOptions = new AmqpRetryOptions()
            .setTryTimeout(Duration.ofSeconds(refDataServiceBusProperties.getTryTimeoutSeconds()));
        builder.retryOptions(retryOptions);

        return builder
            .sessionReceiver()
            .topicName(refDataServiceBusProperties.getTopicName())
            .subscriptionName(refDataServiceBusProperties.getSubscriptionName())
            .disableAutoComplete()
            .receiveMode(ServiceBusReceiveMode.PEEK_LOCK)
            .buildClient();
    }
}
