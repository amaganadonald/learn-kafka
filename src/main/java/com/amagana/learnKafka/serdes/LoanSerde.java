package com.amagana.learnKafka.serdes;

import com.amagana.learnKafka.domain.Loan;
import org.apache.kafka.common.serialization.Serdes;

public class LoanSerde extends Serdes.WrapperSerde<Loan> {

    public LoanSerde() {
        super(new LoanSerializer(), new LoanDeserializer());
    }
}
