package uk.gov.hmcts.opal.refdata.config;

import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.PositiveOrZero;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@ConfigurationProperties(prefix = "opal.ref-data.service-bus")
@Validated
@Data
public class RefDataServiceBusProperties {

    /** Azure Service Bus namespace used with managed identity authentication. */
    private String fullyQualifiedNamespace;

    /** Topic from which ref-data messages are consumed. */
    private String topicName;

    /** Subscription on the topic from which ref-data messages are consumed. */
    private String subscriptionName;

    /** Enables the scheduled ref-data Service Bus consumer. */
    @NotNull
    private Boolean consumerEnabled;

    /** Maximum number of messages requested from one Service Bus session per poll. */
    @Positive
    private Integer maxMessagesPerSession;

    /** Delay in seconds before the first scheduled poll starts. */
    @PositiveOrZero
    private Long initialDelaySeconds;

    /** Delay in seconds between the end of one scheduled poll and the start of the next. */
    @Positive
    private Long fixedDelaySeconds;

    /** Maximum time in seconds that a receive operation waits for messages.
     *  If fewer than maxMessagesPerSession in the queue then puller will wait this long
     */
    @Positive
    private Long receiveTimeoutSeconds;

    /** Maximum time in seconds allowed for one AMQP operation attempt before it times out. */
    @Positive
    private Long tryTimeoutSeconds;

    @AssertTrue(message = "when consumer enabled, topic-name must be set")
    public boolean isTopicConfigured() {
        return !Boolean.TRUE.equals(consumerEnabled) || (topicName != null && !topicName.isBlank());
    }

    @AssertTrue(message = "when consumer enabled, subscription-name must be set")
    public boolean isSubscriptionConfigured() {
        return !Boolean.TRUE.equals(consumerEnabled)
            || (subscriptionName != null && !subscriptionName.isBlank());
    }

    @AssertTrue(message = "when consumer enabled, max-messages-per-session must be set")
    public boolean isMaxMessagesPerSessionConfigured() {
        return !Boolean.TRUE.equals(consumerEnabled) || maxMessagesPerSession != null;
    }

    @AssertTrue(message = "when consumer enabled, initial-delay-seconds must be set")
    public boolean isInitialDelaySecondsConfigured() {
        return !Boolean.TRUE.equals(consumerEnabled) || initialDelaySeconds != null;
    }

    @AssertTrue(message = "when consumer enabled, fixed-delay-seconds must be set")
    public boolean isFixedDelaySecondsConfigured() {
        return !Boolean.TRUE.equals(consumerEnabled) || fixedDelaySeconds != null;
    }

    @AssertTrue(message = "when consumer enabled, receive-timeout-seconds must be set")
    public boolean isReceiveTimeoutSecondsConfigured() {
        return !Boolean.TRUE.equals(consumerEnabled) || receiveTimeoutSeconds != null;
    }

    @AssertTrue(message = "when consumer enabled, try-timeout-seconds must be set")
    public boolean isTryTimeoutSecondsConfigured() {
        return !Boolean.TRUE.equals(consumerEnabled) || tryTimeoutSeconds != null;
    }
}
