package com.endava.internship.cryptomarket.confservice.data.dbutils;

import java.sql.PreparedStatement;
import java.sql.SQLException;

@FunctionalInterface
public interface StatementEntityConsumer<T> {

    void accept(PreparedStatement preparedStatement, T Entity) throws SQLException;
}
