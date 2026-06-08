package com.amagana.learnKafka.controller;

import com.amagana.learnKafka.domain.Loan;
import com.amagana.learnKafka.dto.LoanDTO;
import com.amagana.learnKafka.service.LoanService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("api/v1/loan")
public class LoanController {

    private final LoanService loanService;

    public LoanController(LoanService loanService) {
        this.loanService = loanService;
    }

    @GetMapping
    public ResponseEntity<List<Loan>> getLoan() {
        return ResponseEntity.status(HttpStatus.OK).body(loanService.getLoan());
    }

    @PostMapping
    public ResponseEntity<String> createLoan(@RequestBody LoanDTO loanDTO) {
        return ResponseEntity.status(HttpStatus.CREATED).body(loanService.createLoan(loanDTO));
    }
}
