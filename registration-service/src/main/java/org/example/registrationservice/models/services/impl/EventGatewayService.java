package org.example.registrationservice.models.services.impl;

import lombok.RequiredArgsConstructor;
import feign.FeignException;
import org.example.registrationservice.exceptions.EventNotFoundException;
import org.example.registrationservice.exceptions.EventServiceException;
import org.example.registrationservice.clients.EventClient;
import org.example.registrationservice.models.dto.responses.EventResponse;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class EventGatewayService {
    private final EventClient eventClient;

    @CircuitBreaker(name = "eventService", fallbackMethod = "getEventFallback")
    public EventResponse getEventById(Long eventId) {
        return eventClient.getEventById(eventId);
    }

    public EventResponse getEventFallback(Long eventId, Exception exception) {
        Throwable cause = exception;
        while (cause != null) {
            if (cause instanceof FeignException feignException && feignException.status() == 404) {
                throw new EventNotFoundException(eventId);
            }
            cause = cause.getCause();
        }
        throw new EventServiceException("Event service is currently unavailable", exception);
    }
}
