package com.sulakov.services;

import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class DbDataManger {
    private static final Logger logger = Logger.getLogger(DbDataManger.class);
    private static final ConnectionProvider CONNECTION_PROVIDER = new ConnectionProvider();

    private DbDataManger() {
    }

    public static void saveCountryData(String name, int cases, int todayCases, int deaths, int todayDeaths, int recovered,
                                       int active, int critical, int casesPerOneMillion, int deathsPerOneMillion,
                                       int totalTests, int testsPerOneMillion) {
        try (Connection connection = CONNECTION_PROVIDER.getDbConnection()) {
            int rowCount;
            String sql = "update country_statistic\n" +
                    "        set cases               = ?\n" +  //1
                    "          , todayCases          = ?\n" +  //2
                    "          , deaths              = ?\n" +  //3
                    "          , todayDeaths         = ?\n" +  //4
                    "          , recovered           = ?\n" +  //5
                    "          , active              = ?\n" +  //6
                    "          , critical            = ?\n" +  //7
                    "          , casesPerOneMillion  = ?\n" +  //8
                    "          , deathsPerOneMillion = ?\n" +  //9
                    "          , totalTests          = ?\n" +  //10
                    "          , testsPerOneMillion  = ?\n" +  //11
                    "          , lastdate            = now()\n" +
                    "     where name = ?\n" + //12
                    "       and cases               <> ?\n" + //13
                    "       and todayCases          <> ?\n" + //14
                    "       and deaths              <> ?\n" + //15
                    "       and todayDeaths         <> ?\n" + //16
                    "       and recovered           <> ?\n" + //17
                    "       and active              <> ?\n" + //18
                    "       and critical            <> ?\n" + //19
                    "       and casesPerOneMillion  <> ?\n" + //20
                    "       and deathsPerOneMillion <> ?\n" + //21
                    "       and totalTests          <> ?\n" + //22
                    "       and testsPerOneMillion  <> ?";    //23
            PreparedStatement updateStatement = connection.prepareStatement(sql);
            updateStatement.setInt(1, cases);
            updateStatement.setInt(2, todayCases);
            updateStatement.setInt(3, deaths);
            updateStatement.setInt(4, todayDeaths);
            updateStatement.setInt(5, recovered);
            updateStatement.setInt(6, active);
            updateStatement.setInt(7, critical);
            updateStatement.setInt(8, casesPerOneMillion);
            updateStatement.setInt(9, deathsPerOneMillion);
            updateStatement.setInt(10,totalTests);
            updateStatement.setInt(11, testsPerOneMillion);
            updateStatement.setString(12, name);
            updateStatement.setInt(13, cases);
            updateStatement.setInt(14, todayCases);
            updateStatement.setInt(15, deaths);
            updateStatement.setInt(16, todayDeaths);
            updateStatement.setInt(17, recovered);
            updateStatement.setInt(18, active);
            updateStatement.setInt(19, critical);
            updateStatement.setInt(20, casesPerOneMillion);
            updateStatement.setInt(21, deathsPerOneMillion);
            updateStatement.setInt(22, totalTests);
            updateStatement.setInt(23, testsPerOneMillion);

            rowCount = updateStatement.executeUpdate();

            if (rowCount == 0) {
                sql = "insert into country_statistic\n" +
                        "(  name              \n" +  //1
                        ",  cases             \n" +  //2
                        " , todayCases        \n" +  //3
                        " , deaths            \n" +  //4
                        " , todayDeaths       \n" +  //5
                        " , recovered         \n" +  //6
                        " , active            \n" +  //7
                        " , critical          \n" +  //8
                        " , casesPerOneMillion\n" +  //9
                        " , deathsPerOneMillio\n" +  //10
                        " , totalTests        \n" +  //11
                        " , testsPerOneMillion\n" +  //12
                        " , lastdate )" +
                        "values \n" +
                        "(? \n" +
                        ",? \n" +
                        ",? \n" +
                        ",? \n" +
                        ",? \n" +
                        ",? \n" +
                        ",? \n" +
                        ",? \n" +
                        ",? \n" +
                        ",? \n" +
                        ",? \n" +
                        ",? \n" +
                        ",now())";
                PreparedStatement insertStatement = connection.prepareStatement(sql);
                insertStatement.setString(1, name);
                insertStatement.setInt(2, cases);
                insertStatement.setInt(3, todayCases);
                insertStatement.setInt(4, deaths);
                insertStatement.setInt(5, todayDeaths);
                insertStatement.setInt(6, recovered);
                insertStatement.setInt(7, active);
                insertStatement.setInt(8, critical);
                insertStatement.setInt(9, casesPerOneMillion);
                insertStatement.setInt(10, deathsPerOneMillion);
                insertStatement.setInt(11, totalTests);
                insertStatement.setInt(12, testsPerOneMillion);
                if (rowCount == 0) {
                    logger.error("[DB] Error inserting country stat for " + name);
                }
            }
        } catch (SQLException e) {
            logger.error("[DB] Error while save country date", e);
        }
    }

    public static void saveUser(int tgrm_id, String first_name, String last_name, String user_name) {
        try (Connection connection = CONNECTION_PROVIDER.getDbConnection()) {
            int rowCount;
            String sql = "update users\n" +
                    "        set first_name   = ?\n" +
                    "           ,last_name    = ?\n" +
                    "           ,user_name    = ?\n" +
                    "           ,lastdate     = sysdate()\n" +
                    "      where tgrm_user_id = ?";
            PreparedStatement statement = connection.prepareStatement(sql);
            statement.setString(1, first_name);
            statement.setString(2, last_name);
            statement.setString(3, user_name);
            statement.setInt(4, tgrm_id);

            rowCount = statement.executeUpdate();
            if (rowCount == 0) {
                sql = "insert into users\n" +
                      " (tgrm_user_id\n" +
                      " ,first_name\n" +
                      " ,last_name\n" +
                      " ,user_name\n" +
                      " ,lastdate\n" +
                      " )\n" +
                      " values\n" +
                      " (\n" +
                      " ?\n" +
                      " ,?\n" +
                      " ,?\n" +
                      " ,?\n" +
                      " ,sysdate()\n" +
                      " )";
                PreparedStatement preparedStatement = connection.prepareStatement(sql);
                preparedStatement.setInt(1, tgrm_id);
                preparedStatement.setString(2, first_name);
                preparedStatement.setString(3, last_name);
                preparedStatement.setString(4, user_name);
                rowCount = preparedStatement.executeUpdate();
                if (rowCount == 0) {
                    logger.error("[DB] Error adding user");
                }
            }
        } catch (SQLException e) {
            logger.error("[DB] Error while save user data", e);
        }
    }
}
