package com.endava.internship.cryptomarket.confservice.api.exceptionhandlers;

import com.endava.internship.cryptomarket.confservice.business.exceptions.ExceptionResponses;
import com.endava.internship.cryptomarket.confservice.business.model.ApiError;
import com.endava.internship.cryptomarket.confservice.business.model.UserDto;
import com.endava.internship.cryptomarket.confservice.data.model.User;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.HttpMediaTypeNotSupportedException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.context.request.WebRequest;

import javax.validation.ConstraintViolation;
import javax.validation.ConstraintViolationException;
import javax.validation.metadata.ConstraintDescriptor;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.http.HttpStatus.BAD_REQUEST;
import static org.springframework.http.HttpStatus.FORBIDDEN;
import static org.springframework.http.HttpStatus.INTERNAL_SERVER_ERROR;
import static org.springframework.http.HttpStatus.NOT_ACCEPTABLE;
import static org.springframework.http.MediaType.APPLICATION_JSON;

@ExtendWith(MockitoExtension.class)
class ApiExceptionHandlerTest {

    ApiExceptionHandler exceptionHandler;

    @BeforeEach
    void setUp() {
        exceptionHandler = new ApiExceptionHandler();
    }

    private static Stream<Arguments> objectsForConstraintHandler() {
        final String username = "user";
        final User user = User.builder().username(username).build();
        final Optional<User> optionalUser = Optional.of(user);
        final UserDto userDto = UserDto.of(user);
        final Object objects = new Object[]{user};
        final Object optionalObjects = new Object[]{optionalUser};
        return Stream.of(
                Arguments.of(username, username),
                Arguments.of(user, username),
                Arguments.of(optionalUser, username),
                Arguments.of(Optional.empty(), null),
                Arguments.of(userDto, username),
                Arguments.of(objects, username),
                Arguments.of(optionalObjects, username)
        );
    }

    @ParameterizedTest
    @MethodSource("objectsForConstraintHandler")
    void whenHandleConstraintExceptionOfString_thenPrepareResponse(Object invalidValue, String username) {
        ExceptionResponses responses = ExceptionResponses.SELF_AMEND;

        ConstraintDescriptor descriptor = mock(ConstraintDescriptor.class);
        when(descriptor.getAttributes()).thenReturn(Map.of("response", responses));
        ConstraintViolation<?> constraintViolation = mock(ConstraintViolation.class);
        when(constraintViolation.getInvalidValue()).thenReturn(invalidValue);
        when(constraintViolation.getConstraintDescriptor()).thenReturn(descriptor);
        ConstraintViolationException exception = new ConstraintViolationException(Set.of(constraintViolation));
        WebRequest request = mock(WebRequest.class);
        final HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.setContentType(APPLICATION_JSON);
        ApiError body = responses.buildApiError(username);
        HttpStatus status = FORBIDDEN;
        ResponseEntity<Object> expectedResponse = new ResponseEntity<>(body, httpHeaders, status);

        ResponseEntity<Object> responseEntity = exceptionHandler.handleConstraintException(exception, request);

        assertThat(responseEntity).isEqualTo(expectedResponse);
    }

    @Test
    void whenHandleException_thenPrepareInternalServerErrorResponse() {
        RuntimeException exception = mock(RuntimeException.class);
        WebRequest request = mock(WebRequest.class);
        final HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.setContentType(APPLICATION_JSON);
        ExceptionResponses responses = ExceptionResponses.INTERNAL_SERVER_ERROR;
        ApiError body = responses.buildApiError(null);
        HttpStatus status = INTERNAL_SERVER_ERROR;
        ResponseEntity<Object> expectedResponse = new ResponseEntity<>(body, httpHeaders, status);

        ResponseEntity<Object> responseEntity = exceptionHandler.handleConstraintException(exception, request);

        assertThat(responseEntity).isEqualTo(expectedResponse);
    }

    @Test
    void whenHandleHttpMediaTypeNotSupported_thenPrepareNotAcceptableContentResponse() {
        HttpMediaTypeNotSupportedException exception = mock(HttpMediaTypeNotSupportedException.class);
        WebRequest request = mock(WebRequest.class);
        final HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.setContentType(APPLICATION_JSON);
        ExceptionResponses responses = ExceptionResponses.NOT_ACCEPTABLE_CONTENT;
        ApiError body = responses.buildApiError(null);
        HttpStatus status = NOT_ACCEPTABLE;
        ResponseEntity<Object> expectedResponse = new ResponseEntity<>(body, httpHeaders, status);

        ResponseEntity<Object> responseEntity = exceptionHandler.handleHttpMediaTypeNotSupported(exception, httpHeaders, status, request);

        assertThat(responseEntity).isEqualTo(expectedResponse);
    }

    @Test
    void whenHandleHttpMessageNotReadable_thenPrepareJsonMalformedResponse() {
        HttpMessageNotReadableException exception = mock(HttpMessageNotReadableException.class);
        WebRequest request = mock(WebRequest.class);
        final HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.setContentType(APPLICATION_JSON);
        ExceptionResponses responses = ExceptionResponses.JSON_MALFORMED;
        ApiError body = responses.buildApiError(null);
        HttpStatus status = BAD_REQUEST;
        ResponseEntity<Object> expectedResponse = new ResponseEntity<>(body, httpHeaders, status);

        ResponseEntity<Object> responseEntity = exceptionHandler.handleHttpMessageNotReadable(exception, httpHeaders, status, request);

        assertThat(responseEntity).isEqualTo(expectedResponse);
    }

    @Test
    void whenHandleMethodArgumentNotValid_thenPrepareValidationFailureResponse() {
        MethodArgumentNotValidException exception = mock(MethodArgumentNotValidException.class);
        WebRequest request = mock(WebRequest.class);
        final HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.setContentType(APPLICATION_JSON);
        ExceptionResponses responses = ExceptionResponses.REQUEST_OBJECT_VALIDATION_FAILURE;
        ApiError body = responses.buildApiError(null);
        HttpStatus status = BAD_REQUEST;
        ResponseEntity<Object> expectedResponse = new ResponseEntity<>(body, httpHeaders, status);

        ResponseEntity<Object> responseEntity = exceptionHandler.handleMethodArgumentNotValid(exception, httpHeaders, status, request);

        assertThat(responseEntity).isEqualTo(expectedResponse);
    }

}
