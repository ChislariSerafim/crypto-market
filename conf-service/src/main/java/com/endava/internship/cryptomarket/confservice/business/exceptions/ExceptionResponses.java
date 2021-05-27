package com.endava.internship.cryptomarket.confservice.business.exceptions;

import com.endava.internship.cryptomarket.confservice.business.model.ApiError;
import lombok.Getter;

import static javax.servlet.http.HttpServletResponse.SC_BAD_REQUEST;
import static javax.servlet.http.HttpServletResponse.SC_FORBIDDEN;
import static javax.servlet.http.HttpServletResponse.SC_INTERNAL_SERVER_ERROR;
import static javax.servlet.http.HttpServletResponse.SC_NOT_ACCEPTABLE;
import static javax.servlet.http.HttpServletResponse.SC_NOT_FOUND;
import static javax.servlet.http.HttpServletResponse.SC_UNAUTHORIZED;

public enum ExceptionResponses {
    NONEXISTENT_USER_NOT_AUTHORIZED(SC_UNAUTHORIZED, "Unauthorized", "Requester %s does not exist.", 1000),
    AUTHENTICATION_FAILURE(SC_UNAUTHORIZED, "Unauthorized", "Authentication failure. Please provide header 'Requester-Username'.", 1000),
    USER_SUSPND_ACCESS_FORBIDDEN(SC_FORBIDDEN, "Forbidden", "Requester %s is currently suspended. Please contact the administrator.", 2100),
    INACTIV_USER_AMEND(SC_BAD_REQUEST, "Bad request", "User %s is inactive and cannot be amended.", 2200),
    INACTIV_USER_CREATE(SC_BAD_REQUEST, "Bad request", "Unable to create user %s with initial status INACTV.", 2300),
    USER_NOT_ALLOWED_REMOVE(SC_FORBIDDEN, "Forbidden", "Requester %s is not allowed to remove users.", 3100),
    ADMIN_NOT_ALLOWED_CREATE_ADMIN(SC_FORBIDDEN, "Forbidden", "Requester %s is not allowed to create other administrators.", 3200),
    OPERAT_NOT_ALLOWED_CREATE_ADMIN_OPERAT(SC_FORBIDDEN, "Forbidden", "Requester %s is not allowed to create other operators or administrators.", 3300),
    USER_NOT_FOUND(SC_NOT_FOUND, "Not found", "User %s does not exist.", 4100),
    SELF_AMEND(SC_FORBIDDEN, "Forbidden", "Requester %s is not allowed to amend itself.", 4200),
    DIFFERENT_USERNAME(SC_BAD_REQUEST, "Bad request", "Username must be the same URL path variable and request object property username.", 4300),
    NOT_ACCEPTABLE_CONTENT(SC_NOT_ACCEPTABLE, "Not acceptable", "For endpoints with request objects, request Header Content-Type is not application/json or null", 5000),
    JSON_MALFORMED(SC_BAD_REQUEST, "Bad request", "Request object JSON malformed.", 5100),
    USER_NOT_CHANGED(SC_BAD_REQUEST, "Bad request", "Request object requires at least one property to be amended. Username cannot be amended.", 5200),
    USER_MISSING_PROPERTY(SC_BAD_REQUEST, "Bad request", "Request object missing required properties.", 5300),
    USER_ILLEGAL_STATE(SC_BAD_REQUEST, "Bad request", "Request object validation failure.", 5400),
    REQUEST_OBJECT_VALIDATION_FAILURE(SC_BAD_REQUEST, "Bad request", "Request object validation failure.", 5400),
    USER_ALREADY_EXISTS(SC_BAD_REQUEST, "Bad request", "User username %s already taken.", 5400),
    INTERNAL_SERVER_ERROR(SC_INTERNAL_SERVER_ERROR, "Internal server error", "Unknown internal error", 9999);

    @Getter
    private final int httpStatus;
    private final String statusSuffix;
    private final String messageTemplate;
    private final int errorCode;

    ExceptionResponses(final int httpStatus, final String statusSuffix, final String messageTemplate, final int errorCode) {
        this.httpStatus = httpStatus;
        this.statusSuffix = statusSuffix;
        this.messageTemplate = messageTemplate;
        this.errorCode = errorCode;
    }

    public ApiError buildApiError(String messageParam) {
        final String status = httpStatus + " " + statusSuffix;
        final String message = String.format(messageTemplate, messageParam);
        return new ApiError(status, message, errorCode);
    }

    public ApiError buildApiError() {
        return buildApiError(null);
    }
}
