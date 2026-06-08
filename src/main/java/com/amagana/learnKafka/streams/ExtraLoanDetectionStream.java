package com.amagana.learnKafka.streams;

import com.amagana.learnKafka.domain.Loan;
import com.amagana.learnKafka.serdes.LoanSerde;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.support.serializer.JsonSerde;

@Configuration
@EnableKafkaStreams
@Slf4j
public class ExtraLoanDetectionStream {

    @Bean
    public KStream<String, Loan> extraLoanDetection(StreamsBuilder builder) {
        log.info("Determining extra loan activated");
//        var loanSerde = new JsonSerde<>(Loan.class);
        KStream<String, Loan> loanKStream = builder.stream("bank-loan", Consumed.with(Serdes.String(), new LoanSerde()));
        loanKStream.filter((key, value) -> value.getAmount() > 50000)
                //.map((key, loan)-> KeyValue.pair(loan.getTransactionId(), loan.getAmount()))
                .peek((key, value) -> log.warn("ALERT transaction: {} Extra Loan Detected: {}", key, value))
                .to("bank-loan-detected",  Produced.with(Serdes.String(), new LoanSerde()));
//        loanDetected.to("bank-loan-detected");
        return loanKStream;
    }
}
