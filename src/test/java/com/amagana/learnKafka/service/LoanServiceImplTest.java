package com.amagana.learnKafka.service;


import com.amagana.learnKafka.domain.Loan;
import com.amagana.learnKafka.dto.LoanDTO;
import com.amagana.learnKafka.service.impl.LoamServiceImpl;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.verify;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers
@Slf4j
public class LoanServiceImplTest {

    private static final String LOAN_TOPIC = "bank-loan";

    @Autowired
    KafkaTemplate<String, Loan> kafkaTemplate;
    @Container
    static final KafkaContainer kafkaContainer =
            new KafkaContainer(DockerImageName.parse("apache/kafka:latest"));

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", kafkaContainer::getBootstrapServers);
    }

    Loan loan;

    @BeforeEach
    void setUp() {
        loan =
                Loan.builder()
                        .id(1L)
                        .amount(1000)
                        .interest(5)
                        .build();
    }

    @MockitoSpyBean
    LoamServiceImpl loanService;

    @Test
    @DisplayName("Should publish new Loan inside the topic")
    void should_publish_loan_event() {

        loanService.createLoan(LoanDTO.builder()
                        .id(1L)
                        .userId(1)
                        .amount(1000)
                        .interest(3.0)
                .build()
                );

        Map<String, Object> props =
                KafkaTestUtils.consumerProps(
                        kafkaContainer.getBootstrapServers(),
                        "true",
                        "true"
                );

        Consumer<String, Loan> consumer =
                new DefaultKafkaConsumerFactory<>(
                        props,
                        new StringDeserializer(),
                        new JsonDeserializer<>(Loan.class)
                ).createConsumer();

        consumer.subscribe(
                List.of("bank-loan")
        );


        ConsumerRecords<String, Loan> records = KafkaTestUtils.getRecords(
                consumer,
                Duration.ofSeconds(30),
                5
        );

        assertNotNull(records);
        assertThat(records.count()).isEqualTo(30);
    }

    @Test
    void should_consume_loan() {

        kafkaTemplate.send(
                LOAN_TOPIC,
                loan.getTransactionId(),
                loan
        );

        await()
                .atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() ->
                        assertThat(
                                loanService.getLoan()
                        ).hasSize(1)
                );
    }

    @Test
    void should_retry_failed_message() {

        kafkaTemplate.send(
                LOAN_TOPIC,
                UUID.randomUUID().toString(),
                loan
        );

        await()
                .atMost(30, TimeUnit.SECONDS)
                .untilAsserted(() -> {

                    verify(loanService,
                            atLeast(1))
                            .processLoan(
                                    any(),
                                    any(),
                                    any(),
                                    any()
                            );
                });
    }
}
