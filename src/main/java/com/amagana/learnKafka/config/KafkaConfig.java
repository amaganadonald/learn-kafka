package com.amagana.learnKafka.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaConfig {

    @Bean
    public NewTopic loanTopic() {
        return new NewTopic("bank-loan", 3, (short) 1);
    }

    @Bean
    public NewTopic bankLoanTopic() {
        return new NewTopic("bank-loan-detected", 3, (short) 1);
    }
}
