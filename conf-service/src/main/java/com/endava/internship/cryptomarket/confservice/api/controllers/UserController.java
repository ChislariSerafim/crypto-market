package com.endava.internship.cryptomarket.confservice.api.controllers;

import com.endava.internship.cryptomarket.confservice.business.UserService;
import com.endava.internship.cryptomarket.confservice.business.model.UserDto;
import com.endava.internship.cryptomarket.confservice.business.validators.annotations.RequesterAuthorized;
import com.endava.internship.cryptomarket.confservice.business.validators.annotations.RequesterNotNull;
import com.endava.internship.cryptomarket.confservice.data.model.User;
import lombok.RequiredArgsConstructor;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Optional;

import static org.springframework.http.HttpStatus.ACCEPTED;
import static org.springframework.http.HttpStatus.CREATED;
import static org.springframework.http.HttpStatus.NO_CONTENT;
import static org.springframework.http.MediaType.APPLICATION_JSON_VALUE;
import static org.springframework.http.MediaType.TEXT_PLAIN_VALUE;

@RestController
@RequestMapping("/users")
@Validated
@RequiredArgsConstructor
public class UserController {

    private final UserService userService;

    @GetMapping(produces = APPLICATION_JSON_VALUE)
    public List<UserDto> getUsers(
            @RequesterNotNull
            @RequesterAuthorized
            @RequestHeader(value = "Requester-Username", required = false)
                    Optional<User> requestUser) {
        return userService.getAllUsers(requestUser.get());
    }

    @GetMapping(value = "/{username}", produces = APPLICATION_JSON_VALUE)
    public UserDto getUser(
            @PathVariable
                    String username,

            @RequesterNotNull
            @RequesterAuthorized
            @RequestHeader(value = "Requester-Username", required = false)
                    Optional<User> requestUser) {
        return userService.getUser(username, requestUser.get());
    }

    @PostMapping(consumes = APPLICATION_JSON_VALUE, produces = TEXT_PLAIN_VALUE)
    @ResponseStatus(CREATED)
    public String postUser(
            @RequestBody
                    UserDto userDTO,

            @RequesterNotNull
            @RequesterAuthorized
            @RequestHeader(value = "Requester-Username", required = false)
                    Optional<User> requestUser) {
        userService.createUser(userDTO, requestUser.get());


        return "User created";
    }

    @PutMapping(value = "/{username}", consumes = APPLICATION_JSON_VALUE, produces = TEXT_PLAIN_VALUE)
    @ResponseStatus(ACCEPTED)
    public String putUser(
            @PathVariable
                    String username,

            @RequestBody
                    UserDto userDTO,

            @RequesterNotNull
            @RequesterAuthorized
            @RequestHeader(value = "Requester-Username", required = false)
                    Optional<User> requestUser) {
        userService.amendUser(username, userDTO, requestUser.get());

        return "Accepted";
    }

    @DeleteMapping(value = "/{username}")
    @ResponseStatus(NO_CONTENT)
    public void deleteUser(
            @PathVariable
                    String username,

            @RequesterNotNull
            @RequesterAuthorized
            @RequestHeader(value = "Requester-Username", required = false)
                    Optional<User> requestUser) {
        userService.deleteUser(username, requestUser.get());
    }

}
