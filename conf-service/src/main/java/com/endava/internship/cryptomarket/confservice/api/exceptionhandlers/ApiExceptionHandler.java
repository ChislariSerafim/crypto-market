package com.endava.internship.cryptomarket.confservice.api.exceptionhandlers;

import com.endava.internship.cryptomarket.confservice.business.exceptions.ExceptionResponses;
import com.endava.internship.cryptomarket.confservice.business.model.ApiError;
import com.endava.internship.cryptomarket.confservice.business.model.UserDto;
import com.endava.internship.cryptomarket.confservice.data.model.User;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.HttpMediaTypeNotSupportedException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

import javax.validation.ConstraintViolation;
import javax.validation.ConstraintViolationException;
import java.util.Optional;

import static com.endava.internship.cryptomarket.confservice.business.exceptions.ExceptionResponses.JSON_MALFORMED;
import static com.endava.internship.cryptomarket.confservice.business.exceptions.ExceptionResponses.NOT_ACCEPTABLE_CONTENT;
import static com.endava.internship.cryptomarket.confservice.business.exceptions.ExceptionResponses.REQUEST_OBJECT_VALIDATION_FAILURE;
import static org.springframework.http.HttpStatus.BAD_REQUEST;
import static org.springframework.http.HttpStatus.INTERNAL_SERVER_ERROR;
import static org.springframework.http.HttpStatus.NOT_ACCEPTABLE;
import static org.springframework.http.MediaType.APPLICATION_JSON;

@ControllerAdvice
public class ApiExceptionHandler extends ResponseEntityExceptionHandler {

    @ExceptionHandler(ConstraintViolationException.class)
    public ResponseEntity<Object> handleConstraintException(final ConstraintViolationException ex, final WebRequest request) {
        final ConstraintViolation<?> constraintViolation = ex.getConstraintViolations().iterator().next();

        final ExceptionResponses response = (ExceptionResponses) constraintViolation.getConstraintDescriptor().getAttributes().get("response");
        final Object invalidValue = extractInvalidObject(constraintViolation.getInvalidValue());
        final String messageParam = extractMessageParam(invalidValue);

        final ApiError body = response.buildApiError(messageParam);
        final HttpStatus status = HttpStatus.valueOf(Integer.parseInt(body.getStatus().substring(0, 3)));
        final HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.setContentType(MediaType.APPLICATION_JSON);

        return handleExceptionInternal(ex, body, httpHeaders, status, request);
    }

    @ExceptionHandler(RuntimeException.class)
    public ResponseEntity<Object> handleConstraintException(final RuntimeException ex, final WebRequest request) {
        ExceptionResponses serviceError = ExceptionResponses.INTERNAL_SERVER_ERROR;
        ApiError body = serviceError.buildApiError(null);
        final HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.setContentType(MediaType.APPLICATION_JSON);
        return handleExceptionInternal(ex, body, httpHeaders, INTERNAL_SERVER_ERROR, request);
    }

    @Override
    protected ResponseEntity<Object> handleHttpMediaTypeNotSupported(HttpMediaTypeNotSupportedException ex,
            HttpHeaders headers,
            HttpStatus status,
            WebRequest request) {
        ApiError body = NOT_ACCEPTABLE_CONTENT.buildApiError();
        headers.setContentType(APPLICATION_JSON);
        return handleExceptionInternal(ex, body, headers, NOT_ACCEPTABLE, request);
    }

    @Override
    protected ResponseEntity<Object> handleHttpMessageNotReadable(HttpMessageNotReadableException ex,
            HttpHeaders headers,
            HttpStatus status,
            WebRequest request) {
        ApiError body = JSON_MALFORMED.buildApiError();
        headers.setContentType(APPLICATION_JSON);
        return handleExceptionInternal(ex, body, headers, BAD_REQUEST, request);
    }

    @Override
    protected ResponseEntity<Object> handleMethodArgumentNotValid(MethodArgumentNotValidException ex,
            HttpHeaders headers,
            HttpStatus status,
            WebRequest request) {
        ApiError body = REQUEST_OBJECT_VALIDATION_FAILURE.buildApiError();
        headers.setContentType(APPLICATION_JSON);
        return handleExceptionInternal(ex, body, headers, BAD_REQUEST, request);

    }

    private Object extractInvalidObject(Object invalidObject) {
        if (invalidObject instanceof Object[]) {
            Object[] parameters = (Object[]) invalidObject;
            int lastParameter = parameters.length - 1;
            return parameters[lastParameter];
        }

        return invalidObject;
    }

    private String extractMessageParam(final Object invalidValue) {
        if (invalidValue instanceof Optional<?>) {
            Optional<User> optional = ((Optional<User>) invalidValue);
            return optional.map(User::getUsername).orElse(null);
        }

        if (invalidValue instanceof User) {
            return ((User) invalidValue).getUsername();
        }

        if (invalidValue instanceof UserDto) {
            return ((UserDto) invalidValue).getUsername();
        }

        return (String) invalidValue;
    }
}
