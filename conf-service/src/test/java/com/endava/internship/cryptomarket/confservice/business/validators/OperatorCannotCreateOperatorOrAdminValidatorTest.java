package com.endava.internship.cryptomarket.confservice.business.validators;

import com.endava.internship.cryptomarket.confservice.business.model.UserDto;
import com.endava.internship.cryptomarket.confservice.data.model.Roles;
import com.endava.internship.cryptomarket.confservice.data.model.User;
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
import static com.endava.internship.cryptomarket.confservice.data.model.Roles.CLIENT;
import static com.endava.internship.cryptomarket.confservice.data.model.Roles.OPERAT;
import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(MockitoExtension.class)
class OperatorCannotCreateOperatorOrAdminValidatorTest {

    private OperatorCannotCreateOperatorOrAdminValidator validator;

    @Mock
    private ConstraintValidatorContext context;

    @BeforeEach
    void setUp() {
        validator = new OperatorCannotCreateOperatorOrAdminValidator();
    }

    private static Stream<Arguments> createTests() {
        return Stream.of(
                Arguments.of(OPERAT, ADMIN, false),
                Arguments.of(OPERAT, OPERAT, false),
                Arguments.of(ADMIN, CLIENT, true),
                Arguments.of(OPERAT, CLIENT, true)
        );
    }

    @ParameterizedTest
    @MethodSource("createTests")
    void whenCheckIfValid_thenValidate(Roles reqRole, Roles createRole, boolean response) {
        User requester = User.builder().username("requester").role(reqRole).build();
        UserDto user = new UserDto("user", null, createRole,
                null, null, null, null);

        Object[] arguments = new Object[]{user, requester};

        assertThat(validator.isValid(arguments, context)).isEqualTo(response);
    }

}
