package com.endava.internship.cryptomarket.confservice.business.validators.annotations;

import com.endava.internship.cryptomarket.confservice.business.exceptions.ExceptionResponses;
import com.endava.internship.cryptomarket.confservice.business.validators.SameUserInPathAndRequestBodyValidator;

import javax.validation.Constraint;
import javax.validation.Payload;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static com.endava.internship.cryptomarket.confservice.business.exceptions.ExceptionResponses.DIFFERENT_USERNAME;

@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
@Constraint(validatedBy = SameUserInPathAndRequestBodyValidator.class)
public @interface SameUserInPathAndRequestBody {

    ExceptionResponses response() default DIFFERENT_USERNAME;

    String message() default "";

    Class<?>[] groups() default {};

    Class<? extends Payload>[] payload() default {};

}
