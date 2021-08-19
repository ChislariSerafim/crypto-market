package com.endava.internship.cryptomarket.confservice.business.validators.annotations;

import com.endava.internship.cryptomarket.confservice.business.exceptions.ExceptionResponses;
import com.endava.internship.cryptomarket.confservice.business.validators.ValidAmendRequestValidator;

import javax.validation.Constraint;
import javax.validation.Payload;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static com.endava.internship.cryptomarket.confservice.business.exceptions.ExceptionResponses.USER_NOT_CHANGED;

@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Constraint(validatedBy = ValidAmendRequestValidator.class)
public @interface ValidAmendRequest {
    ExceptionResponses response() default USER_NOT_CHANGED;

    String message() default "";

    Class<?>[] groups() default {};

    Class<? extends Payload>[] payload() default {};
}
