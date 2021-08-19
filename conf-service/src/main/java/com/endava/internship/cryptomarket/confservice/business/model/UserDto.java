package com.endava.internship.cryptomarket.confservice.business.model;

import com.endava.internship.cryptomarket.confservice.business.validators.annotations.CreatedUserNotInactv;
import com.endava.internship.cryptomarket.confservice.business.validators.annotations.NonExistent;
import com.endava.internship.cryptomarket.confservice.business.validators.annotations.ValidAmendRequest;
import com.endava.internship.cryptomarket.confservice.business.validators.annotations.ValidCreateRequest;
import com.endava.internship.cryptomarket.confservice.business.validators.annotations.ValidEmail;
import com.endava.internship.cryptomarket.confservice.business.validators.annotations.ValidUsername;
import com.endava.internship.cryptomarket.confservice.business.validators.orders.NonExistentOrder5400;
import com.endava.internship.cryptomarket.confservice.business.validators.orders.UserStatusOrder2200_2300;
import com.endava.internship.cryptomarket.confservice.business.validators.orders.ValidAmendOrder5200;
import com.endava.internship.cryptomarket.confservice.business.validators.orders.ValidCreateOrder5300;
import com.endava.internship.cryptomarket.confservice.business.validators.orders.ValidUserOrder5400;
import com.endava.internship.cryptomarket.confservice.data.model.Roles;
import com.endava.internship.cryptomarket.confservice.data.model.Status;
import com.endava.internship.cryptomarket.confservice.data.model.User;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.LocalDateTime;

import static java.util.Objects.nonNull;


@Getter
@Setter
@Builder
@AllArgsConstructor
@NoArgsConstructor
@EqualsAndHashCode(of = "username")
@JsonInclude(JsonInclude.Include.NON_NULL)
@ValidAmendRequest(groups = ValidAmendOrder5200.class)
@ValidCreateRequest(groups = ValidCreateOrder5300.class)
@CreatedUserNotInactv(groups = UserStatusOrder2200_2300.class)
public class UserDto {

    @ValidUsername(groups = ValidUserOrder5400.class)
    @NonExistent(groups = NonExistentOrder5400.class)
    private String username;

    @ValidEmail(groups = ValidUserOrder5400.class)
    private String email;

    private Roles role;

    private Status status;

    private LocalDateTime createdOn;

    private LocalDateTime updatedOn;

    private String updatedBy;

    public static UserDto of(User user) {
        return new UserDto(user.getUsername(), user.getEmail(),
                user.getRole(), user.getStatus(), null, null, null);
    }

    public static UserDto ofDetailedUser(User user) {
        return new UserDto(user.getUsername(), user.getEmail(), user.getRole(), user.getStatus(),
                user.getCreatedOn(), user.getUpdatedOn(), user.getUpdatedBy());
    }

    public User toUser(){
        return User.builder().username(username).email(email).status(status).role(role)
                .createdOn(createdOn).updatedOn(updatedOn).updatedBy(updatedBy).build();
    }

    public boolean validForAmendRequest() {
        return nonNull(username)
                && (nonNull(email) || nonNull(status) || nonNull(role));
    }
    public boolean validForCreateRequest() {
        return (nonNull(username) && nonNull(email) && nonNull(status) && nonNull(role));
    }


}
