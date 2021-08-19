package com.endava.internship.cryptomarket.confservice.business.validators;

import com.endava.internship.cryptomarket.confservice.business.UserService;
import com.endava.internship.cryptomarket.confservice.business.validators.annotations.NonExistent;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;

@Component
@RequiredArgsConstructor
public class NonExistentValidator implements ConstraintValidator<NonExistent, String> {

    private final UserService userService;

    public boolean isValid(final String username, final ConstraintValidatorContext constraintValidatorContext) {
        return userService.getRequesterUser(username).isEmpty();
    }
}
