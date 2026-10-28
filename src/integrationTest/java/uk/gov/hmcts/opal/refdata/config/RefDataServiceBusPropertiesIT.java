package uk.gov.hmcts.opal.refdata.config;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.bind.validation.BindValidationException;
import org.springframework.context.ConfigurableApplicationContext;

class RefDataServiceBusPropertiesIT {

    @Test
    void shouldPreventApplicationStartupWhenARequiredPropertyIsMissing() {
        assertThatThrownBy(() -> startApplicationWithoutTopicName())
            .hasRootCauseInstanceOf(BindValidationException.class)
            .hasStackTraceContaining("topic-name must be set");
    }

    private static void startApplicationWithoutTopicName() {
        try (ConfigurableApplicationContext ignored = new SpringApplication(TestApplication.class).run(
            "--opal.ref-data.service-bus.consumer-enabled=true",
            "--opal.ref-data.service-bus.subscription-name=ref-data-subscription",
            "--opal.ref-data.service-bus.max-messages-per-session=10",
            "--opal.ref-data.service-bus.initial-delay-seconds=0",
            "--opal.ref-data.service-bus.fixed-delay-seconds=10",
            "--opal.ref-data.service-bus.receive-timeout-seconds=10",
            "--opal.ref-data.service-bus.try-timeout-seconds=10",
            "--opal.ref-data.service-bus.fully-qualified-namespace=ref-data.servicebus.windows.net"
        )) {
            // The application must fail while creating the validated properties bean.
        }
    }

    @SpringBootApplication(excludeName = {
        "org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration",
        "org.springframework.boot.hibernate.autoconfigure.HibernateJpaAutoConfiguration"
    })
    static class TestApplication {
    }
}
