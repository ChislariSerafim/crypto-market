package com.endava.internship.cryptomarket.confservice.data.model;

import lombok.*;

import java.time.LocalDateTime;

import static lombok.AccessLevel.PRIVATE;

@Getter
@Setter
@Builder
@EqualsAndHashCode(of = "username")
@AllArgsConstructor(access = PRIVATE)
public class User {

    private final String username;

    private String email;

    private Roles role;

    private Status status;

    private LocalDateTime createdOn;

    private LocalDateTime updatedOn;

    private String updatedBy;

}
