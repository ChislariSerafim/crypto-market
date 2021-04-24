package com.endava.internship.cryptomarket.confservice.business.mappers;

import com.endava.internship.cryptomarket.confservice.business.model.UserDto;
import com.endava.internship.cryptomarket.confservice.data.model.User;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;

@Mapper
public interface UserMapper {

    UserDto entityToDetailedDto(User user);

    @Mapping(target = "createdOn", ignore = true)
    @Mapping(target = "updatedOn", ignore = true)
    @Mapping(target = "updatedBy", ignore = true)
    UserDto entityToDto(User user);

    @Mapping(target = "createdOn", ignore = true)
    @Mapping(target = "updatedOn", ignore = true)
    @Mapping(target = "updatedBy", ignore = true)
    User dtoToEntity(UserDto user);

}
