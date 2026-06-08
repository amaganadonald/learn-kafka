package com.amagana.learnKafka.serdes;

import com.amagana.learnKafka.domain.Loan;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

public class LoanSerializer implements Serializer<Loan> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public byte[] serialize(String topic, Loan loan)  {
        try {
            return objectMapper.writeValueAsBytes(loan);
        } catch (Exception e) {
            throw new SerializationException("Error serializing loan", e);
        }
    }
}
