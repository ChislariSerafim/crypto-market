package com.endava.internship.cryptomarket.confservice.business.validators.annotations;

import com.endava.internship.cryptomarket.confservice.business.exceptions.ExceptionResponses;
import com.endava.internship.cryptomarket.confservice.business.validators.CreatedUserNotInactvValidator;

import javax.validation.Constraint;
import javax.validation.Payload;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static com.endava.internship.cryptomarket.confservice.business.exceptions.ExceptionResponses.INACTIV_USER_CREATE;

@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Constraint(validatedBy = CreatedUserNotInactvValidator.class)
public @interface CreatedUserNotInactv {
    ExceptionResponses response() default INACTIV_USER_CREATE;

    String message() default "";

    Class<?>[] groups() default {};

    Class<? extends Payload>[] payload() default {};
}
