package com.endava.internship.cryptomarket.confservice.application;

import com.endava.internship.cryptomarket.confservice.api.datahandlers.StringToUserConverter;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Configuration;
import org.springframework.format.FormatterRegistry;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Configuration
@EnableWebMvc
@RequiredArgsConstructor
public class WebMvcConfiguration implements WebMvcConfigurer {

    private final StringToUserConverter stringToUserConverter;

    @Override
    public void addFormatters(FormatterRegistry registry) {
        registry.addConverter(stringToUserConverter);
    }

}
