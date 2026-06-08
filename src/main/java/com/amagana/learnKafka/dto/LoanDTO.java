package com.amagana.learnKafka.dto;

import lombok.Builder;

@Builder
public record LoanDTO(long id, double amount, int userId, double interest) {
}
