package com.amagana.learnKafka.service;

import com.amagana.learnKafka.domain.Loan;
import com.amagana.learnKafka.dto.LoanDTO;

import java.util.List;

public interface LoanService {

    List<Loan> getLoan();
    Loan cancelLoan(Long id);
    Loan rejectLoan(Long id);
    String createLoan(LoanDTO loanDTO);
}
