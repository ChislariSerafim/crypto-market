package com.endava.internship.cryptomarket.confservice.business;

import com.endava.internship.cryptomarket.confservice.business.model.UserDto;
import com.endava.internship.cryptomarket.confservice.business.validators.annotations.AdminCannotCreateAdmin;
import com.endava.internship.cryptomarket.confservice.business.validators.annotations.AmendedUserNotInactv;
import com.endava.internship.cryptomarket.confservice.business.validators.annotations.OperatorCannotCreateOperatorOrAdmin;
import com.endava.internship.cryptomarket.confservice.business.validators.annotations.RequesterCannotSelfAmend;
import com.endava.internship.cryptomarket.confservice.business.validators.annotations.RequesterNotOperat;
import com.endava.internship.cryptomarket.confservice.business.validators.annotations.RequesterNotSuspended;
import com.endava.internship.cryptomarket.confservice.business.validators.annotations.SameUserInPathAndRequestBody;
import com.endava.internship.cryptomarket.confservice.business.validators.annotations.UsernameNotTaken;
import com.endava.internship.cryptomarket.confservice.business.validators.orders.AmendRequestOrder;
import com.endava.internship.cryptomarket.confservice.business.validators.orders.CreateRequestOrder;
import com.endava.internship.cryptomarket.confservice.business.validators.orders.DifferentRequesterOrder4200;
import com.endava.internship.cryptomarket.confservice.business.validators.orders.RequesterAccessOrder2100;
import com.endava.internship.cryptomarket.confservice.business.validators.orders.RequesterNotOperatOrder3100;
import com.endava.internship.cryptomarket.confservice.business.validators.orders.SameUsernameOrder4300;
import com.endava.internship.cryptomarket.confservice.business.validators.orders.ServiceValidationOrder;
import com.endava.internship.cryptomarket.confservice.business.validators.orders.UserRolesOrder3200_3300;
import com.endava.internship.cryptomarket.confservice.business.validators.orders.UserStatusOrder2200_2300;
import com.endava.internship.cryptomarket.confservice.business.validators.orders.UsernameNotTakenOrder4100;
import com.endava.internship.cryptomarket.confservice.data.model.User;
import org.springframework.validation.annotation.Validated;

import javax.validation.Valid;
import java.util.List;
import java.util.Optional;

@Validated(ServiceValidationOrder.class)
public interface UserService {

    List<UserDto> getAllUsers(
            @RequesterNotSuspended(groups = RequesterAccessOrder2100.class)
                    User requestUser);

    UserDto getUser(
            @UsernameNotTaken(groups = UsernameNotTakenOrder4100.class)
                    String username,

            @RequesterNotSuspended(groups = RequesterAccessOrder2100.class)
                    User requestUser);

    @Validated(CreateRequestOrder.class)
    @AdminCannotCreateAdmin(groups = UserRolesOrder3200_3300.class)
    @OperatorCannotCreateOperatorOrAdmin(groups = UserRolesOrder3200_3300.class)
    void createUser(
            @Valid
                    UserDto user,

            @RequesterNotSuspended(groups = RequesterAccessOrder2100.class)
                    User requestUser);

    @Validated(AmendRequestOrder.class)
    @RequesterCannotSelfAmend(groups = DifferentRequesterOrder4200.class)
    @SameUserInPathAndRequestBody(groups = SameUsernameOrder4300.class)
    void amendUser(
            @UsernameNotTaken(groups = UsernameNotTakenOrder4100.class)
            @AmendedUserNotInactv(groups = UserStatusOrder2200_2300.class)
                    String username,

            @Valid
                    UserDto newUser,

            @RequesterNotSuspended(groups = RequesterAccessOrder2100.class)
                    User requestUser);

    void deleteUser(
            @UsernameNotTaken(groups = UsernameNotTakenOrder4100.class)
                    String username,

            @RequesterNotSuspended(groups = RequesterAccessOrder2100.class)
            @RequesterNotOperat(groups = RequesterNotOperatOrder3100.class)
                    User requestUser);

    Optional<User> getRequesterUser(String username);

    boolean userExists(User user);

}
