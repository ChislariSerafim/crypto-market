package com.endava.internship.cryptomarket.confservice.integration;

import com.endava.internship.cryptomarket.confservice.business.exceptions.ExceptionResponses;
import com.endava.internship.cryptomarket.confservice.business.model.UserDto;
import io.restassured.http.Method;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import static com.endava.internship.cryptomarket.confservice.business.exceptions.ExceptionResponses.ADMIN_NOT_ALLOWED_CREATE_ADMIN;
import static com.endava.internship.cryptomarket.confservice.business.exceptions.ExceptionResponses.AUTHENTICATION_FAILURE;
import static com.endava.internship.cryptomarket.confservice.business.exceptions.ExceptionResponses.DIFFERENT_USERNAME;
import static com.endava.internship.cryptomarket.confservice.business.exceptions.ExceptionResponses.INACTIV_USER_AMEND;
import static com.endava.internship.cryptomarket.confservice.business.exceptions.ExceptionResponses.INACTIV_USER_CREATE;
import static com.endava.internship.cryptomarket.confservice.business.exceptions.ExceptionResponses.JSON_MALFORMED;
import static com.endava.internship.cryptomarket.confservice.business.exceptions.ExceptionResponses.NONEXISTENT_USER_NOT_AUTHORIZED;
import static com.endava.internship.cryptomarket.confservice.business.exceptions.ExceptionResponses.NOT_ACCEPTABLE_CONTENT;
import static com.endava.internship.cryptomarket.confservice.business.exceptions.ExceptionResponses.OPERAT_NOT_ALLOWED_CREATE_ADMIN_OPERAT;
import static com.endava.internship.cryptomarket.confservice.business.exceptions.ExceptionResponses.SELF_AMEND;
import static com.endava.internship.cryptomarket.confservice.business.exceptions.ExceptionResponses.USER_ALREADY_EXISTS;
import static com.endava.internship.cryptomarket.confservice.business.exceptions.ExceptionResponses.USER_ILLEGAL_STATE;
import static com.endava.internship.cryptomarket.confservice.business.exceptions.ExceptionResponses.USER_MISSING_PROPERTY;
import static com.endava.internship.cryptomarket.confservice.business.exceptions.ExceptionResponses.USER_NOT_ALLOWED_REMOVE;
import static com.endava.internship.cryptomarket.confservice.business.exceptions.ExceptionResponses.USER_NOT_CHANGED;
import static com.endava.internship.cryptomarket.confservice.business.exceptions.ExceptionResponses.USER_NOT_FOUND;
import static com.endava.internship.cryptomarket.confservice.business.exceptions.ExceptionResponses.USER_SUSPND_ACCESS_FORBIDDEN;
import static com.endava.internship.cryptomarket.confservice.data.model.Roles.ADMIN;
import static com.endava.internship.cryptomarket.confservice.data.model.Roles.OPERAT;
import static com.endava.internship.cryptomarket.confservice.data.model.Status.ACTIVE;
import static com.endava.internship.cryptomarket.confservice.data.model.Status.INACTV;
import static io.restassured.http.Method.DELETE;
import static io.restassured.http.Method.GET;
import static io.restassured.http.Method.POST;
import static io.restassured.http.Method.PUT;

@RequiredArgsConstructor
@Getter
public enum ApiErrorTest {
    TEST1(POST, "http://localhost:8080/conf-service/users", "admin",
            "text/plain", NOT_ACCEPTABLE_CONTENT, "Should be a request object", ""),
    TEST2(PUT, "http://localhost:8080/conf-service/users/operat1", "admin",
            "text/plain", NOT_ACCEPTABLE_CONTENT, "Should be a request object", ""),
    TEST3(GET, "http://localhost:8080/conf-service/users", null, "application/json", AUTHENTICATION_FAILURE,
            new UserDto("operat", "operat@gmail.com", OPERAT, ACTIVE, null, null, null), ""),
    TEST4(GET, "http://localhost:8080/conf-service/users/operat1", null, "application/json", AUTHENTICATION_FAILURE,
            new UserDto("operat", "operat@gmail.com", OPERAT, ACTIVE, null, null, null), ""),
    TEST5(POST, "http://localhost:8080/conf-service/users", null, "application/json", AUTHENTICATION_FAILURE,
            new UserDto("operat", "operat@gmail.com", OPERAT, ACTIVE, null, null, null), ""),
    TEST6(PUT, "http://localhost:8080/conf-service/users/operat1", null, "application/json", AUTHENTICATION_FAILURE,
            new UserDto("operat", "operat@gmail.com", OPERAT, ACTIVE, null, null, null), ""),
    TEST7(DELETE, "http://localhost:8080/conf-service/users/operat1", null, "application/json", AUTHENTICATION_FAILURE,
            new UserDto("operat", "operat@gmail.com", OPERAT, ACTIVE, null, null, null), ""),
    TEST8(GET, "http://localhost:8080/conf-service/users", "operat", "application/json", NONEXISTENT_USER_NOT_AUTHORIZED,
            new UserDto("operat", "operat@gmail.com", OPERAT, ACTIVE, null, null, null), "operat"),
    TEST9(GET, "http://localhost:8080/conf-service/users/operat1", "operat", "application/json", NONEXISTENT_USER_NOT_AUTHORIZED,
            new UserDto("operat", "operat@gmail.com", OPERAT, ACTIVE, null, null, null), "operat"),
    TEST10(POST, "http://localhost:8080/conf-service/users", "operat", "application/json", NONEXISTENT_USER_NOT_AUTHORIZED,
            new UserDto("operat", "operat@gmail.com", OPERAT, ACTIVE, null, null, null), "operat"),
    TEST11(PUT, "http://localhost:8080/conf-service/users/operat1", "operat", "application/json", NONEXISTENT_USER_NOT_AUTHORIZED,
            new UserDto("operat", "operat@gmail.com", OPERAT, ACTIVE, null, null, null), "operat"),
    TEST12(DELETE, "http://localhost:8080/conf-service/users/operat1", "operat", "application/json", NONEXISTENT_USER_NOT_AUTHORIZED,
            new UserDto("operat", "operat@gmail.com", OPERAT, ACTIVE, null, null, null), "operat"),
    TEST13(GET, "http://localhost:8080/conf-service/users", "client1", "application/json", NONEXISTENT_USER_NOT_AUTHORIZED,
            new UserDto("operat", "operat@gmail.com", OPERAT, ACTIVE, null, null, null), "client1"),
    TEST14(GET, "http://localhost:8080/conf-service/users/operat1", "client1", "application/json", NONEXISTENT_USER_NOT_AUTHORIZED,
            new UserDto("operat", "operat@gmail.com", OPERAT, ACTIVE, null, null, null), "client1"),
    TEST15(POST, "http://localhost:8080/conf-service/users", "client1", "application/json", NONEXISTENT_USER_NOT_AUTHORIZED,
            new UserDto("operat", "operat@gmail.com", OPERAT, ACTIVE, null, null, null), "client1"),
    TEST16(PUT, "http://localhost:8080/conf-service/users/operat1", "client1", "application/json", NONEXISTENT_USER_NOT_AUTHORIZED,
            new UserDto("operat", "operat@gmail.com", OPERAT, ACTIVE, null, null, null), "client1"),
    TEST17(DELETE, "http://localhost:8080/conf-service/users/operat1", "client1", "application/json", NONEXISTENT_USER_NOT_AUTHORIZED,
            new UserDto("operat", "operat@gmail.com", OPERAT, ACTIVE, null, null, null), "client1"),
    TEST18(GET, "http://localhost:8080/conf-service/users", "operat4", "application/json", NONEXISTENT_USER_NOT_AUTHORIZED,
            new UserDto("operat", "operat@gmail.com", OPERAT, ACTIVE, null, null, null), "operat4"),
    TEST19(GET, "http://localhost:8080/conf-service/users/operat1", "operat4", "application/json", NONEXISTENT_USER_NOT_AUTHORIZED,
            new UserDto("operat", "operat@gmail.com", OPERAT, ACTIVE, null, null, null), "operat4"),
    TEST20(POST, "http://localhost:8080/conf-service/users", "operat4", "application/json", NONEXISTENT_USER_NOT_AUTHORIZED,
            new UserDto("operat", "operat@gmail.com", OPERAT, ACTIVE, null, null, null), "operat4"),
    TEST21(PUT, "http://localhost:8080/conf-service/users/operat1", "operat4", "application/json", NONEXISTENT_USER_NOT_AUTHORIZED,
            new UserDto("operat", "operat@gmail.com", OPERAT, ACTIVE, null, null, null), "operat4"),
    TEST22(DELETE, "http://localhost:8080/conf-service/users/operat1", "operat4", "application/json", NONEXISTENT_USER_NOT_AUTHORIZED,
            new UserDto("operat", "operat@gmail.com", OPERAT, ACTIVE, null, null, null), "operat4"),
    TEST23(GET, "http://localhost:8080/conf-service/users", "operat3", "application/json", USER_SUSPND_ACCESS_FORBIDDEN,
            new UserDto("operat", "operat@gmail.com", OPERAT, ACTIVE, null, null, null), "operat3"),
    TEST24(GET, "http://localhost:8080/conf-service/users/operat1", "operat3", "application/json", USER_SUSPND_ACCESS_FORBIDDEN,
            new UserDto("operat", "operat@gmail.com", OPERAT, ACTIVE, null, null, null), "operat3"),
    TEST25(POST, "http://localhost:8080/conf-service/users", "operat3", "application/json", USER_SUSPND_ACCESS_FORBIDDEN,
            new UserDto("operat", "operat@gmail.com", OPERAT, ACTIVE, null, null, null), "operat3"),
    TEST26(PUT, "http://localhost:8080/conf-service/users/operat1", "operat3", "application/json", USER_SUSPND_ACCESS_FORBIDDEN,
            new UserDto("operat", "operat@gmail.com", OPERAT, ACTIVE, null, null, null), "operat3"),
    TEST27(DELETE, "http://localhost:8080/conf-service/users/operat1", "operat3", "application/json", USER_SUSPND_ACCESS_FORBIDDEN,
            new UserDto("operat", "operat@gmail.com", OPERAT, ACTIVE, null, null, null), "operat3"),
    TEST28(DELETE, "http://localhost:8080/conf-service/users/operat2", "operat1", "application/json", USER_NOT_ALLOWED_REMOVE,
            new UserDto("operat2", "operat@gmail.com", OPERAT, ACTIVE, null, null, null), "operat1"),
    TEST29(GET, "http://localhost:8080/conf-service/users/operator", "admin", "application/json", USER_NOT_FOUND,
            new UserDto("operator", "operat@gmail.com", OPERAT, ACTIVE, null, null, null), "operator"),
    TEST30(PUT, "http://localhost:8080/conf-service/users/operator", "admin", "application/json", USER_NOT_FOUND,
            new UserDto("operator", "operat@gmail.com", OPERAT, ACTIVE, null, null, null), "operator"),
    TEST31(DELETE, "http://localhost:8080/conf-service/users/operator", "admin", "application/json", USER_NOT_FOUND,
            new UserDto("operator", "operat@gmail.com", OPERAT, ACTIVE, null, null, null), "operator"),
    TEST32(PUT, "http://localhost:8080/conf-service/users/operat1", "operat1", "application/json", SELF_AMEND,
            new UserDto("operat1", "operat@gmail.com", OPERAT, ACTIVE, null, null, null), "operat1"),
    TEST33(POST, "http://localhost:8080/conf-service/users", "operat1", "application/json",
            JSON_MALFORMED, "Corrupted JSON", ""),
    TEST34(PUT, "http://localhost:8080/conf-service/users/operat2", "operat1", "application/json",
            JSON_MALFORMED, "Corrupted JSON", ""),
    TEST35(POST, "http://localhost:8080/conf-service/users", "admin", "application/json", ADMIN_NOT_ALLOWED_CREATE_ADMIN,
            new UserDto("newadmin", "admin@gmail.com", ADMIN, ACTIVE, null, null, null), "admin"),
    TEST36(POST, "http://localhost:8080/conf-service/users", "operat1", "application/json", OPERAT_NOT_ALLOWED_CREATE_ADMIN_OPERAT,
            new UserDto("newadmin", "admin@gmail.com", ADMIN, ACTIVE, null, null, null), "operat1"),
    TEST37(POST, "http://localhost:8080/conf-service/users", "operat1", "application/json", OPERAT_NOT_ALLOWED_CREATE_ADMIN_OPERAT,
            new UserDto("newoperat", "operat@gmail.com", OPERAT, ACTIVE, null, null, null), "operat1"),
    TEST38(PUT, "http://localhost:8080/conf-service/users/operat1", "admin", "application/json", DIFFERENT_USERNAME,
            new UserDto("theOperat", "operat@gmail.com", OPERAT, ACTIVE, null, null, null), ""),
    TEST39(PUT, "http://localhost:8080/conf-service/users/operat1", "admin", "application/json", USER_NOT_CHANGED,
            new UserDto("operat1", null, null, null, null, null, null), ""),
    TEST40(POST, "http://localhost:8080/conf-service/users", "admin", "application/json", USER_MISSING_PROPERTY,
            new UserDto("operat1", null, null, null, null, null, null), ""),
    TEST41(POST, "http://localhost:8080/conf-service/users", "admin", "application/json", USER_ILLEGAL_STATE,
            new UserDto("operat8", "invalid email", OPERAT, ACTIVE, null, null, null), ""),
    TEST42(PUT, "http://localhost:8080/conf-service/users/operat1", "admin", "application/json", USER_ILLEGAL_STATE,
            new UserDto("operat1", "invalid email", OPERAT, ACTIVE, null, null, null), ""),
    TEST43(PUT, "http://localhost:8080/conf-service/users/operat4", "admin", "application/json", INACTIV_USER_AMEND,
            new UserDto("operat4", "operat@gmail.com", OPERAT, ACTIVE, null, null, null), "operat4"),
    TEST44(POST, "http://localhost:8080/conf-service/users", "admin", "application/json", INACTIV_USER_CREATE,
            new UserDto("operat5", "operat@gmail.com", OPERAT, INACTV, null, null, null), "operat5"),
    TEST45(POST, "http://localhost:8080/conf-service/users", "admin", "application/json", USER_ALREADY_EXISTS,
            new UserDto("operat1", "operat@gmail.com", OPERAT, ACTIVE, null, null, null), "operat1");

    private final Method method;
    private final String urlPath;
    private final String RequesterUsername;
    private final String contentType;
    private final ExceptionResponses exceptionResponses;
    private final Object messageParam;
    private final String exceptionParam;
}
