package org.example.registrationservice.models.services.impl;

import lombok.RequiredArgsConstructor;
import org.example.registrationservice.exceptions.EventServiceException;
import org.example.registrationservice.models.constants.RegistrationStatus;
import org.example.registrationservice.models.dto.requests.CreateRegistrationDetailRequest;
import org.example.registrationservice.models.dto.requests.CreateRegistrationRequest;
import org.example.registrationservice.models.dto.responses.EventResponse;
import org.example.registrationservice.models.dto.responses.RegistrationDetailResponse;
import org.example.registrationservice.models.dto.responses.RegistrationResponse;
import org.example.registrationservice.models.entities.Registration;
import org.example.registrationservice.models.entities.RegistrationDetail;
import org.example.registrationservice.models.repositories.RegistrationDetailRepository;
import org.example.registrationservice.models.repositories.RegistrationRepository;
import org.example.registrationservice.models.services.RegistrationService;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.List;

@Service
@RequiredArgsConstructor
public class RegistrationServiceImpl implements RegistrationService {
    private static final String REGISTRATION_CREATED_TOPIC = "registration-created";

    private final RegistrationRepository registrationRepository;
    private final RegistrationDetailRepository registrationDetailRepository;
    private final EventGatewayService eventGatewayService;
    private final KafkaTemplate<String, String> kafkaTemplate;

    @Override
    @Transactional
    public RegistrationResponse createRegistration(CreateRegistrationRequest request) {
        List<RegistrationDetail> details = new ArrayList<>();
        double totalAmount = 0;

        for (CreateRegistrationDetailRequest item : request.items()) {
            if (item.quantity() == null || item.quantity() <= 0) {
                throw new IllegalArgumentException("Ticket quantity must be greater than zero");
            }

            EventResponse event = eventGatewayService.getEventById(item.eventId());
            if (event == null || event.ticketPrice() == null || event.availableTickets() == null) {
                throw new EventServiceException("Event service returned incomplete event data");
            }
            if (item.quantity() > event.availableTickets()) {
                throw new IllegalArgumentException(
                        "Requested quantity exceeds available tickets for event " + item.eventId());
            }

            double lineTotal = event.ticketPrice() * item.quantity();
            totalAmount += lineTotal;
            details.add(RegistrationDetail.builder()
                    .eventId(item.eventId())
                    .quantity(item.quantity())
                    .ticketPrice(event.ticketPrice())
                    .lineTotal(lineTotal)
                    .build());
        }

        Registration registration = registrationRepository.save(Registration.builder()
                .participantName(request.participantName())
                .participantEmail(request.participantEmail())
                .totalAmount(totalAmount)
                .status(RegistrationStatus.PENDING)
                .build());

        details.forEach(detail -> detail.setRegistration(registration));
        List<RegistrationDetail> savedDetails = registrationDetailRepository.saveAll(details);

        kafkaTemplate.send(REGISTRATION_CREATED_TOPIC, request.participantEmail());

        return new RegistrationResponse(
                registration.getId(),
                registration.getParticipantName(),
                registration.getParticipantEmail(),
                registration.getTotalAmount(),
                registration.getStatus(),
                savedDetails.stream()
                        .map(detail -> new RegistrationDetailResponse(
                                detail.getId(),
                                detail.getEventId(),
                                detail.getQuantity(),
                                detail.getTicketPrice(),
                                detail.getLineTotal()))
                        .toList());
    }
}
