package org.example.notifyservice.consumer;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.example.notifyservice.service.EmailService;

@Component
public class RegistrationCreatedConsumer {

    private final EmailService emailService;

    public RegistrationCreatedConsumer(EmailService emailService) {
        this.emailService = emailService;
    }

    @KafkaListener(topics = "${notification.kafka.order-created-topic:registration-created}")
    public void consume(String email) {
        if (email == null) {
            throw new IllegalArgumentException("Participant email must not be null");
        }
        emailService.sendOrderCreatedEmail(email);
    }
}
