package com.amagana.learnKafka.serdes;

import com.amagana.learnKafka.domain.Loan;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

public class LoanDeserializer implements Deserializer<Loan> {

    private final ObjectMapper objectMapper = new ObjectMapper();
    
    @Override
    public Loan deserialize(String s, byte[] bytes) {
        try {
            return objectMapper.readValue(bytes, Loan.class);
        }  catch (Exception e) {
            throw new SerializationException(e.getMessage());
        }
    }
}
