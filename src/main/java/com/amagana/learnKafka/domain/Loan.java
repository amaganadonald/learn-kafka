package com.amagana.learnKafka.domain;

import com.amagana.learnKafka.enums.LoanStatus;
import lombok.*;

@AllArgsConstructor
@NoArgsConstructor
@Setter
@Getter
@ToString
@Builder
public class Loan {

    private long id;
    private int userId;
    private double amount;
    private double interest;
    private LoanStatus status;
    private String transactionId;
}
