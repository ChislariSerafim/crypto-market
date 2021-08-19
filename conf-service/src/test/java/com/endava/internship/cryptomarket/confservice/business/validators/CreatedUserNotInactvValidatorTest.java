package com.endava.internship.cryptomarket.confservice.business.validator;

import com.endava.internship.cryptomarket.confservice.business.model.UserDto;
import com.endava.internship.cryptomarket.confservice.business.validators.CreatedUserNotInactvValidator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import javax.validation.ConstraintValidatorContext;
import java.util.stream.Stream;

import static com.endava.internship.cryptomarket.confservice.data.model.Roles.ADMIN;
import static com.endava.internship.cryptomarket.confservice.data.model.Roles.OPERAT;
import static com.endava.internship.cryptomarket.confservice.data.model.Status.ACTIVE;
import static com.endava.internship.cryptomarket.confservice.data.model.Status.INACTV;
import static com.endava.internship.cryptomarket.confservice.data.model.Status.SUSPND;
import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(MockitoExtension.class)
class CreatedUserNotInactvValidatorTest {

    @Mock
    ConstraintValidatorContext context;

    private CreatedUserNotInactvValidator validator;

    private static Stream<Arguments> amendTests() {
        return Stream.of(
                Arguments.of(new UserDto("user", "user@gmail.com", OPERAT,
                        INACTV, null, null, null), false),
                Arguments.of(new UserDto("user", "user@gmail.com", OPERAT,
                        SUSPND, null, null, null), true),
                Arguments.of(new UserDto("user", "user@gmail.com", ADMIN,
                        ACTIVE, null, null, null), true)
        );
    }

    @BeforeEach
    void setUp() {
        validator = new CreatedUserNotInactvValidator();
    }

    @ParameterizedTest
    @MethodSource("amendTests")
    void whenCheckIfValid_thenValidate(UserDto userDTO, boolean response) {
        assertThat(validator.isValid(userDTO, context)).isEqualTo(response);
    }

}
