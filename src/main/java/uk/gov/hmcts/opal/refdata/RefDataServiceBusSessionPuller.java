package uk.gov.hmcts.opal.refdata;

import com.azure.core.amqp.exception.AmqpException;
import com.azure.core.util.IterableStream;
import com.azure.messaging.servicebus.ServiceBusException;
import com.azure.messaging.servicebus.ServiceBusFailureReason;
import com.azure.messaging.servicebus.ServiceBusReceivedMessage;
import com.azure.messaging.servicebus.ServiceBusReceiverClient;
import com.azure.messaging.servicebus.ServiceBusSessionReceiverClient;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import uk.gov.hmcts.opal.refdata.config.RefDataServiceBusProperties;

/**
 * Scheduled consumer for ref-data messages received from an Azure Service Bus session-enabled subscription.
 *
 * <p>Each scheduled poll accepts the next available session, receives up to the configured maximum number of
 * messages, and delegates their payloads to a {@link RefDataMessageProcessor}. Successfully processed messages are
 * completed; messages whose processing fails are abandoned so that Service Bus can make them available for retry.
 * The consumer is created only when the {@code opal.ref-data.service-bus.consumer-enabled} property is {@code true}.
 */
@Slf4j(topic = "opal.RefDataServiceBusSessionPuller")
@Component
@RequiredArgsConstructor
@ConditionalOnProperty(prefix = "opal.ref-data.service-bus", name = "consumer-enabled", havingValue = "true")
public class RefDataServiceBusSessionPuller {

    private final ServiceBusSessionReceiverClient sessionReceiverClient;
    private final RefDataMessageProcessor processor;
    private final RefDataServiceBusProperties properties;

    @Scheduled(
        initialDelayString = "${opal.ref-data.service-bus.initial-delay-seconds}",
        fixedDelayString = "${opal.ref-data.service-bus.fixed-delay-seconds}",
        timeUnit = TimeUnit.SECONDS
    )
    public void pullSessionMessages() {
        log.info("Pulling ref-data Service Bus session messages");

        ServiceBusReceiverClient client = acquireClient();
        if (client != null) {
            processSessionMessages(client);
        }
    }

    private ServiceBusReceiverClient acquireClient() {
        ServiceBusReceiverClient sessionReceiver;
        try {
            sessionReceiver = sessionReceiverClient.acceptNextSession();
            if (sessionReceiver == null) {
                // Should not occur in normal running, so defensive coding here.
                log.debug("No active ref-data Service Bus message sessions found");
            }
            return sessionReceiver;
        } catch (ServiceBusException ex) {
            if (ex.getReason() == ServiceBusFailureReason.MESSAGING_ENTITY_NOT_FOUND) {
                log.error("Ref-data Service Bus queue, topic, or subscription could not be found", ex);
            } else {
                log.error("Unable to communicate with ref-data Service Bus. reason={} isTransient={}",
                    ex.getReason(), ex.isTransient(), ex);
            }
        } catch (AmqpException e) {
            log.error("Error while pulling ref-data Service Bus session messages", e);
        } catch (IllegalStateException ex) {
            if (noSessionAsNoMessageYet(ex)) {
                // This happens if at startup there is no message in the queue, so can be ignored.
                log.info("No session was acquired before the operation timed out.");
            } else {
                log.error("Unexpected state while pulling ref-data messages", ex);
            }
        }
        return null;
    }

    private void processSessionMessages(ServiceBusReceiverClient sessionReceiver) {

        try (sessionReceiver) {
            String sessionId = sessionReceiver.getSessionId();
            log.info("Acquired ref-data Service Bus session lock. sessionId={}", sessionId);

            IterableStream<ServiceBusReceivedMessage> messages = sessionReceiver.receiveMessages(
                properties.getMaxMessagesPerSession(),
                Duration.ofSeconds(properties.getReceiveTimeoutSeconds())
            );

            for (ServiceBusReceivedMessage message : messages) {
                processMessage(sessionReceiver, message);
            }
        } catch (ServiceBusException ex) {
            log.error("Unable to communicate with ref-data Service Bus. reason={} isTransient={}",
                ex.getReason(), ex.isTransient(), ex);
        } catch (AmqpException ex) {
            log.info("AMQP failure while processing ref-data Service Bus session messages", ex);
        } catch (RuntimeException ex) {
            log.error("Unexpected failure while pulling ref-data Service Bus messages", ex);
            throw ex;
        }
    }

    private void processMessage(ServiceBusReceiverClient sessionReceiver, ServiceBusReceivedMessage message) {

        try {
            log.info("Processing Service Bus message: serviceBusMessageId={}", message.getMessageId());
            processor.processMessage(message.getBody().toString());
        } catch (RuntimeException ex) {
            log.warn("Process failed for serviceBusMessageId={}", message.getMessageId(), ex);
            abandonMessage(sessionReceiver, message, ex, message.getMessageId());
            throw ex;
        }

        completeMessage(sessionReceiver, message);
    }

    private static void completeMessage(ServiceBusReceiverClient sessionReceiver, ServiceBusReceivedMessage message) {
        try {
            sessionReceiver.complete(message);
            log.info("Complete succeeded for serviceBusMessageId={}", message.getMessageId());
        } catch (RuntimeException ex) {
            log.warn("Complete failed for serviceBusMessageId={}", message.getMessageId(), ex);
            throw ex;
        }
    }

    private static void abandonMessage(ServiceBusReceiverClient sessionReceiver, ServiceBusReceivedMessage message,
        RuntimeException ex, String messageId) {
        try {
            sessionReceiver.abandon(message);
            log.info("Abandon succeeded for messageId={} ", messageId);
        } catch (RuntimeException abandonException) {
            ex.addSuppressed(abandonException);
            log.warn("Abandon failed for messageId={}", messageId, ex);
        }
    }

    private boolean noSessionAsNoMessageYet(Exception ex) {
        return ex.getCause() instanceof java.util.concurrent.TimeoutException
            && ex.getMessage() != null
            && ex.getMessage().contains("com.microsoft:timeout");
    }
}
