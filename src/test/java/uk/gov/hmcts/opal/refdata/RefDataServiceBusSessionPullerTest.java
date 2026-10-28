package uk.gov.hmcts.opal.refdata;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.azure.core.util.BinaryData;
import com.azure.core.util.IterableStream;
import com.azure.messaging.servicebus.ServiceBusException;
import com.azure.messaging.servicebus.ServiceBusReceivedMessage;
import com.azure.messaging.servicebus.ServiceBusReceiverClient;
import com.azure.messaging.servicebus.ServiceBusSessionReceiverClient;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import uk.gov.hmcts.opal.refdata.config.RefDataServiceBusProperties;

class RefDataServiceBusSessionPullerTest {

    private final ServiceBusSessionReceiverClient sessionReceiverClient = mock(ServiceBusSessionReceiverClient.class);
    private final ServiceBusReceiverClient sessionReceiver = mock(ServiceBusReceiverClient.class);
    private final RefDataMessageProcessor processor = mock(RefDataMessageProcessor.class);
    private final RefDataServiceBusProperties properties = mock(RefDataServiceBusProperties.class);
    private RefDataServiceBusSessionPuller puller;

    @BeforeEach
    void setUp() {
        puller = new RefDataServiceBusSessionPuller(sessionReceiverClient, processor, properties);
        when(properties.getMaxMessagesPerSession()).thenReturn(10);
        when(properties.getReceiveTimeoutSeconds()).thenReturn(5L);
    }

    @Test
    void shouldProcessAndCompleteMessagesInTheNextSession() {
        // Given
        ServiceBusReceivedMessage message = mockMessage("message-1", "payload");
        when(sessionReceiverClient.acceptNextSession()).thenReturn(sessionReceiver);
        when(sessionReceiver.getSessionId()).thenReturn("session-1");
        when(sessionReceiver.receiveMessages(10, Duration.ofSeconds(5)))
            .thenReturn(new IterableStream<>(List.of(message)));

        // When
        puller.pullSessionMessages();

        // Then
        verify(processor).processMessage("payload");
        verify(sessionReceiver).complete(message);
        verify(sessionReceiver).close();
    }

    @Test
    void shouldAbandonMessageWhenProcessingFails() {
        // Given
        ServiceBusReceivedMessage message = mockMessage("message-1", "payload");
        RuntimeException processingFailure = new RuntimeException("processing failed");
        when(sessionReceiverClient.acceptNextSession()).thenReturn(sessionReceiver);
        when(sessionReceiver.getSessionId()).thenReturn("session-1");
        when(sessionReceiver.receiveMessages(10, Duration.ofSeconds(5)))
            .thenReturn(new IterableStream<>(List.of(message)));
        doThrow(processingFailure).when(processor).processMessage("payload");

        // When
        assertThatThrownBy(() -> puller.pullSessionMessages())
            .isSameAs(processingFailure);

        // Then
        verify(sessionReceiver).abandon(message);
        verify(sessionReceiver, never()).complete(message);
        verify(sessionReceiver).close();
    }

    @Test
    void shouldReturnWhenThereIsNoActiveSession() {
        // Given
        when(sessionReceiverClient.acceptNextSession()).thenReturn(null);

        // When
        puller.pullSessionMessages();

        // Then
        verify(processor, never()).processMessage(anyString());
    }

    @Test
    void shouldHandleServiceBusException() {
        // Given
        ServiceBusException exception = mock(ServiceBusException.class);
        when(sessionReceiverClient.acceptNextSession()).thenThrow(exception);

        // When
        puller.pullSessionMessages();

        // Then
        verify(processor, never()).processMessage(anyString());
    }

    @Test
    void shouldHandleTimeoutWhileWaitingForSession() {
        // Given
        IllegalStateException exception = new IllegalStateException(new TimeoutException());
        when(sessionReceiverClient.acceptNextSession()).thenThrow(exception);

        // When
        puller.pullSessionMessages();

        // Then
        verify(processor, never()).processMessage(anyString());
    }

    @Test
    void shouldHandleUnexpectedIllegalStateException() {
        // Given
        when(sessionReceiverClient.acceptNextSession()).thenThrow(new IllegalStateException("unexpected state"));

        // When
        puller.pullSessionMessages();

        // Then
        verify(processor, never()).processMessage(anyString());
    }

    @Test
    void shouldHandleIllegalStateExceptionWithNoMessage() {
        // Given
        when(sessionReceiverClient.acceptNextSession()).thenThrow(new IllegalStateException(new TimeoutException()));

        // When
        puller.pullSessionMessages();

        // Then
        verify(processor, never()).processMessage(anyString());
    }

    @Test
    void shouldRethrowUnexpectedRuntimeException() {
        // Given
        RuntimeException exception = new RuntimeException("unexpected failure");
        when(sessionReceiverClient.acceptNextSession()).thenThrow(exception);

        // When
        assertThatThrownBy(() -> puller.pullSessionMessages())
            .isSameAs(exception);

        // Then
        verify(sessionReceiverClient).acceptNextSession();
    }

    @Test
    void shouldRethrowWhenCompletingMessageFails() {
        // Given
        ServiceBusReceivedMessage message = mockMessage("message-1", "payload");
        RuntimeException exception = new RuntimeException("complete failed");
        when(sessionReceiverClient.acceptNextSession()).thenReturn(sessionReceiver);
        when(sessionReceiver.getSessionId()).thenReturn("session-1");
        when(sessionReceiver.receiveMessages(10, Duration.ofSeconds(5)))
            .thenReturn(new IterableStream<>(List.of(message)));
        doThrow(exception).when(sessionReceiver).complete(message);

        // When
        assertThatThrownBy(() -> puller.pullSessionMessages())
            .isSameAs(exception);

        // Then
        verify(processor).processMessage("payload");
        verify(sessionReceiver).close();
    }

    @Test
    void shouldRetainProcessingFailureWhenAbandoningMessageFails() {
        // Given
        ServiceBusReceivedMessage message = mockMessage("message-1", "payload");
        RuntimeException processingFailure = new RuntimeException("processing failed");
        RuntimeException abandonFailure = new RuntimeException("abandon failed");
        when(sessionReceiverClient.acceptNextSession()).thenReturn(sessionReceiver);
        when(sessionReceiver.getSessionId()).thenReturn("session-1");
        when(sessionReceiver.receiveMessages(10, Duration.ofSeconds(5)))
            .thenReturn(new IterableStream<>(List.of(message)));
        doThrow(processingFailure).when(processor).processMessage("payload");
        doThrow(abandonFailure).when(sessionReceiver).abandon(message);

        // When
        assertThatThrownBy(() -> puller.pullSessionMessages())
            .isSameAs(processingFailure)
            .hasSuppressedException(abandonFailure);

        // Then
        verify(sessionReceiver).close();
    }

    private static ServiceBusReceivedMessage mockMessage(String messageId, String body) {
        ServiceBusReceivedMessage message = mock(ServiceBusReceivedMessage.class);
        when(message.getMessageId()).thenReturn(messageId);
        when(message.getBody()).thenReturn(BinaryData.fromString(body));
        return message;
    }
}
