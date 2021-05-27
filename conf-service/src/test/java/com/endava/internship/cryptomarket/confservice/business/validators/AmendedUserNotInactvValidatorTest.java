package com.endava.internship.cryptomarket.confservice.business.validators;

import com.endava.internship.cryptomarket.confservice.business.UserService;
import com.endava.internship.cryptomarket.confservice.data.model.User;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import javax.validation.ConstraintValidatorContext;
import java.util.Optional;
import java.util.stream.Stream;

import static com.endava.internship.cryptomarket.confservice.data.model.Status.ACTIVE;
import static com.endava.internship.cryptomarket.confservice.data.model.Status.INACTV;
import static com.endava.internship.cryptomarket.confservice.data.model.Status.SUSPND;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class AmendedUserNotInactvValidatorTest {

    @Mock
    private UserService userService;

    @InjectMocks
    private AmendedUserNotInactvValidator validator;

    @Mock
    private ConstraintValidatorContext context;

    private static final String username = "user";

    private static Stream<Arguments> amendTests() {
        return Stream.of(
                Arguments.of(Optional.of(User.builder().username(username).status(INACTV).build()), false),
                Arguments.of(Optional.of(User.builder().username(username).status(SUSPND).build()), true),
                Arguments.of(Optional.of(User.builder().username(username).status(ACTIVE).build()), true)
        );
    }

    @ParameterizedTest
    @MethodSource("amendTests")
    void whenCheckIfValid_thenValidate(Optional<User> user, boolean response) {
        when(userService.getRequesterUser(username)).thenReturn(user);

        assertThat(validator.isValid(username, context)).isEqualTo(response);
    }

}
