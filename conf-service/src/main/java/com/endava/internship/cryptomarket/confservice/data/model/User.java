package com.endava.internship.cryptomarket.confservice.data.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.ToString;

import java.time.LocalDateTime;

import static lombok.AccessLevel.PRIVATE;

@Getter
@Setter
@Builder
@EqualsAndHashCode(of = "username")
@AllArgsConstructor(access = PRIVATE)
@RequiredArgsConstructor
@ToString
public class User {

    private final String username;

    private String email;

    private Roles role;

    private Status status;

    private LocalDateTime createdOn;

    private LocalDateTime updatedOn;

    private String updatedBy;

}
