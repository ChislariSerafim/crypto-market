package com.endava.internship.cryptomarket.confservice.business.validators.annotations;

import com.endava.internship.cryptomarket.confservice.business.exceptions.ExceptionResponses;
import com.endava.internship.cryptomarket.confservice.business.validators.RequesterCannotSelfAmendValidator;

import javax.validation.Constraint;
import javax.validation.Payload;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static com.endava.internship.cryptomarket.confservice.business.exceptions.ExceptionResponses.SELF_AMEND;

@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
@Constraint(validatedBy = RequesterCannotSelfAmendValidator.class)
public @interface RequesterCannotSelfAmend {

    ExceptionResponses response() default SELF_AMEND;

    String message() default "";

    Class<?>[] groups() default {};

    Class<? extends Payload>[] payload() default {};

}
