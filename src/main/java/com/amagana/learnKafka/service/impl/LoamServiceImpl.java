package com.amagana.learnKafka.service.impl;

import com.amagana.learnKafka.domain.Loan;
import com.amagana.learnKafka.dto.LoanDTO;
import com.amagana.learnKafka.enums.LoanStatus;
import com.amagana.learnKafka.service.LoanService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Service
@Slf4j
public class LoamServiceImpl implements LoanService {

    private final KafkaTemplate<String, Loan> kafkaTemplate;
    private static final String LOAN_TOPIC = "bank-loan";
    List<Loan> loans = new ArrayList<>();

    public LoamServiceImpl(KafkaTemplate<String, Loan> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    public List<Loan> getLoan() {
        return loans;
    }

    @Override
    public Loan cancelLoan(Long id) {
        return getLoan(id, LoanStatus.CANCELLED);
    }

    @Override
    public Loan rejectLoan(Long id) {
        return getLoan(id, LoanStatus.REJECTED);
    }

    private Loan getLoan(Long id, LoanStatus rejected) {
        Loan loan = loans.stream().
                filter(loan1 -> loan1.getId() == id)
                .findFirst()
                .orElseThrow(() -> new RuntimeException("Loan does not exist"));
        loan.setStatus(rejected);
        return loan;
    }

    @Override
    public String createLoan(LoanDTO loanDTO) {
        for (int i =1; i < 30; i++) {
            String transactionId = UUID.randomUUID().toString().split("-")[0];
            double amount = loanDTO.amount() * i - (loanDTO.interest() * loanDTO.amount() / 100);
            Loan loan = Loan.builder()
                    .amount(amount)
                    .id(loanDTO.id())
                    .status(LoanStatus.PENDING)
                    .interest(loanDTO.interest())
                    .transactionId(transactionId)
                    .build();
            kafkaTemplate.send(LOAN_TOPIC, loan.getTransactionId(), loan)
                    .whenComplete((res, e) -> {
                        if (e != null) {
                            log.info("error occur when process loan");
                        } else {
                            log.info("success occur when process loan");
                        }
                    });
        }
        return "Transaction sent to Kafka";
    }

    @RetryableTopic(attempts = "4", backoff = @Backoff(delay = 1000, multiplier = 2, maxDelay = 10000),
    exclude = {NullPointerException.class})
    @KafkaListener(topics = LOAN_TOPIC, groupId = "credit-risk-group")
    public void processLoan(Loan loan, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic, Acknowledgment ack,
                            ConsumerRecord<String, String> record) {
        log.info("Process loan {} from topic: {}", loan,  topic);
        log.info(
                "partition={} offset={} value={}",
                record.partition(),
                record.offset(),
                loan);
        List<Double> exceptionAmounts = List.of(114800.0, 79800.0, 124800.0, 84800.0);
        if(exceptionAmounts.contains(loan.getAmount())) {
            throw new RuntimeException("Invalid amount");
        }
        loans.add(loan);
        ack.acknowledge();
    }
    @DltHandler
    public void listenDeadLetterTopic(Loan loan, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                                      @Header(KafkaHeaders.OFFSET) Long offset) {
        log.info("Dead Letter Topic receive : {}, from {},  offset: {}",loan, topic, offset);
    }
}
