package com.endava.internship.cryptomarket.confservice.business.mappers;

import com.endava.internship.cryptomarket.confservice.business.model.UserDto;
import com.endava.internship.cryptomarket.confservice.data.model.User;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mapstruct.factory.Mappers;

import java.time.LocalDateTime;

import static com.endava.internship.cryptomarket.confservice.data.model.Roles.OPERAT;
import static com.endava.internship.cryptomarket.confservice.data.model.Status.ACTIVE;
import static java.time.LocalDateTime.now;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

class UserMapperTest {

    private UserMapper userMapper;
    private User testUser;
    private UserDto testDetailedUserDto;
    private final LocalDateTime TIME = now();

    @BeforeEach
    void setUp() {
        userMapper = Mappers.getMapper(UserMapper.class);
        testUser = User.builder().username("user").email("email@gmail.com")
                .role(OPERAT).status(ACTIVE).createdOn(TIME).updatedOn(TIME).updatedBy("User").build();
         testDetailedUserDto = UserDto.builder().username("user").email("email@gmail.com")
                .role(OPERAT).status(ACTIVE).createdOn(TIME).updatedOn(TIME).updatedBy("User").build();
    }

    @Test
    void whenConvertNullUser_thenReturnNull(){
        assertThat(userMapper.entityToDto(null)).isNull();
    }

    @Test
    void whenDetailedConvertNullUser_thenReturnNull(){
        assertThat(userMapper.entityToDetailedDto(null)).isNull();
    }

    @Test
    void whenConvertNullUserDto_thenReturnNull(){
        assertThat(userMapper.dtoToEntity(null)).isNull();
    }

    @Test
    void whenConvertUserDto_thenReturnCorrectUser(){
        User user = userMapper.dtoToEntity(testDetailedUserDto);

        assertAll(
                () -> assertThat(user.getUsername()).isEqualTo(testDetailedUserDto.getUsername()),
                () -> assertThat(user.getEmail()).isEqualTo(testDetailedUserDto.getEmail()),
                () -> assertThat(user.getRole()).isEqualTo(testDetailedUserDto.getRole()),
                () -> assertThat(user.getStatus()).isEqualTo(testDetailedUserDto.getStatus()),
                () -> assertThat(user.getUpdatedBy()).isNull(),
                () -> assertThat(user.getCreatedOn()).isNull(),
                () -> assertThat(user.getUpdatedBy()).isNull()
        );
    }

    @Test
    void whenDetailedConvertUser_thenReturnCorrectUser(){
        UserDto userDto = userMapper.entityToDetailedDto(testUser);

        assertAll(
                () -> assertThat(userDto.getUsername()).isEqualTo(testUser.getUsername()),
                () -> assertThat(userDto.getEmail()).isEqualTo(testUser.getEmail()),
                () -> assertThat(userDto.getRole()).isEqualTo(testUser.getRole()),
                () -> assertThat(userDto.getStatus()).isEqualTo(testUser.getStatus()),
                () -> assertThat(userDto.getUpdatedBy()).isEqualTo(testUser.getUpdatedBy()),
                () -> assertThat(userDto.getCreatedOn()).isEqualTo(testUser.getCreatedOn()),
                () -> assertThat(userDto.getUpdatedBy()).isEqualTo(testUser.getUpdatedBy())
        );
    }

    @Test
    void whenConvertUser_thenReturnCorrectUser(){
        UserDto userDto = userMapper.entityToDto(testUser);

        assertAll(
                () -> assertThat(userDto.getUsername()).isEqualTo(testUser.getUsername()),
                () -> assertThat(userDto.getEmail()).isEqualTo(testUser.getEmail()),
                () -> assertThat(userDto.getRole()).isEqualTo(testUser.getRole()),
                () -> assertThat(userDto.getStatus()).isEqualTo(testUser.getStatus()),
                () -> assertThat(userDto.getUpdatedBy()).isNull(),
                () -> assertThat(userDto.getCreatedOn()).isNull(),
                () -> assertThat(userDto.getUpdatedBy()).isNull()
        );
    }

}
