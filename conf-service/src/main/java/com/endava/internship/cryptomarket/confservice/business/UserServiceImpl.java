package com.endava.internship.cryptomarket.confservice.business;

import com.endava.internship.cryptomarket.confservice.business.mappers.UserMapper;
import com.endava.internship.cryptomarket.confservice.business.model.UserDto;
import com.endava.internship.cryptomarket.confservice.data.UserRepository;
import com.endava.internship.cryptomarket.confservice.data.model.User;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.time.LocalDateTime.now;
import static java.util.Objects.nonNull;

@Component
@RequiredArgsConstructor
public class UserServiceImpl implements UserService {

    private final UserRepository userRepository;

    private final UserMapper userMapper;

    @Override
    public List<UserDto> getAllUsers(
            User requester) {
        List<User> users = userRepository.getAll();
        return users.stream().map(userMapper::entityToDto).collect(Collectors.toList());
    }

    @Override
    public UserDto getUser(
            String username,
            User requester) {
        User user = userRepository.get(username).get();
        return userMapper.entityToDetailedDto(user);
    }

    @Override
    public void createUser(
            UserDto newUser,
            User requester) {
        User user = userMapper.dtoToEntity(newUser);
        user.setCreatedOn(now());
        user.setUpdatedBy(requester.getUsername());
        userRepository.save(user);
    }

    @Override
    public void amendUser(
            String username,
            UserDto newUser,
            User requestUser) {
        User oldUser = userRepository.get(username).get();

        modify(oldUser, newUser, requestUser);
    }

    @Override
    public void deleteUser(
            String username,
            User requester) {
        userRepository.delete(username);
    }

    @Override
    public Optional<User> getRequesterUser(String username) {
        return userRepository.get(username);
    }

    @Override
    public boolean userExists(User user){
        return userRepository.exists(user);
    }

    private void modify(User user, UserDto newUser, User requester) {
        boolean hasChanged = false;
        if (nonNull(newUser.getEmail()) && !user.getEmail().equals(newUser.getEmail())) {
            user.setEmail(newUser.getEmail());
            hasChanged = true;
        }
        if (nonNull(newUser.getRole()) && user.getRole() != newUser.getRole()) {
            user.setRole(newUser.getRole());
            hasChanged = true;
        }
        if (nonNull(newUser.getStatus()) && user.getStatus() != newUser.getStatus()) {
            user.setStatus(newUser.getStatus());
            hasChanged = true;
        }
        if (hasChanged) {
            user.setUpdatedBy(requester.getUsername());
            user.setUpdatedOn(now());
        }
    }

}
