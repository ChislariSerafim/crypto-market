package com.endava.internship.cryptomarket.confservice.business.validators.annotations;

import com.endava.internship.cryptomarket.confservice.business.exceptions.ExceptionResponses;
import com.endava.internship.cryptomarket.confservice.business.validators.RequesterNotOperatValidator;

import javax.validation.Constraint;
import javax.validation.Payload;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static com.endava.internship.cryptomarket.confservice.business.exceptions.ExceptionResponses.USER_NOT_ALLOWED_REMOVE;

@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.PARAMETER)
@Constraint(validatedBy = RequesterNotOperatValidator.class)
public @interface RequesterNotOperat {

    ExceptionResponses response() default USER_NOT_ALLOWED_REMOVE;

    String message() default "";

    Class<?>[] groups() default {};

    Class<? extends Payload>[] payload() default {};

}
