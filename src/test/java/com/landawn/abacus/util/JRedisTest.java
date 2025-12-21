/*
 * Copyright (c) 2015, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.util;

import java.sql.Time;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.landawn.abacus.cache.JRedis;

@Tag("2025")
public class JRedisTest {
    protected static final String FIRST_NAME = "firstName";
    protected static final String MIDDLE_NAME = "MN";
    protected static final String LAST_NAME = "lastName";
    private static JRedis<Object> client = new JRedis<>("hqd-billing-01:6379");

    /**
     *
     *
     * @return
     */
    public static Account createAccount() {
        return createAccount(FIRST_NAME, LAST_NAME);
    }

    /**
     *
     *
     * @param firstName
     * @param lastName
     * @return
     */
    public static Account createAccount(String firstName, String lastName) {
        Account account = new Account();
        account.setGui(Strings.uuid());
        account.setFirstName(firstName);
        account.setMiddleName(MIDDLE_NAME);
        account.setLastName(lastName);
        account.setEmailAddress(account.getGui() + "@email");
        account.setBirthDate(Dates.currentTimestamp());

        return account;
    }

    /**
     */
    @Test
    public void testSetGetBigArray() {
        int loops = 10;
        int objectNum = 10000;
        int exp = 60 * 1000;
        List<String> keyList = new ArrayList<>();
        long startTime = System.currentTimeMillis();
        Time now = new Time(System.currentTimeMillis());

        for (int i = 0; i < loops; i++) {
            Object[][] a = new Object[objectNum][5];

            for (int k = 0; k < objectNum; k++) {
                a[k][0] = (i * objectNum) + k;
                a[k][1] = "firstName" + (i * objectNum) + k;
                a[k][2] = "lastName" + (i * objectNum) + k;
                a[k][3] = now;
                a[k][4] = null;
            }

            String key = Seid.of("id", i).toString();
            keyList.add(key);

            client.set(key, a, exp);
        }

        N.println("Take " + (System.currentTimeMillis() - startTime) + " milliseconds to set " + loops + " Object[" + objectNum + "] to redis. ");

        startTime = System.currentTimeMillis();

        Object[][] a = null;

        for (String key : keyList) {
            a = (Object[][]) client.get(key);
        }

        N.println("Take " + (System.currentTimeMillis() - startTime) + " milliseconds to get " + loops + " Object[" + objectNum + "] from redis. ");

        N.println(Arrays.toString(a[0]));
    }

    /**
     */
    @Test
    public void testSetGetManyObjects() {
        int loops = 10;
        int objectNum = 10000;
        int exp = 60 * 1000;
        List<String> keyList = new ArrayList<>();
        long startTime = System.currentTimeMillis();
        Account[] accounts = null;

        for (int i = 0; i < loops; i++) {
            accounts = new Account[objectNum];

            for (int k = 0; k < objectNum; k++) {
                accounts[k] = createAccount();
            }

            String key = Seid.of("id", i).toString();
            keyList.add(key);

            client.set(key, accounts, exp);
        }

        N.println("Take " + (System.currentTimeMillis() - startTime) + " milliseconds to set " + loops + " objects from redis. ");

        startTime = System.currentTimeMillis();

        for (String key : keyList) {
            accounts = (Account[]) client.get(key);
        }

        N.println("Take " + (System.currentTimeMillis() - startTime) + " milliseconds to get " + loops + " objects from redis. ");

        // N.println(Arrays.toString(accounts));
    }

    /**
     */
    @Test
    public void testSetGetOneObject() {
        int loops = 10000;
        Account account = null;
        int exp = 60 * 1000;
        List<String> keyList = new ArrayList<>();
        long startTime = System.currentTimeMillis();

        for (int i = 0; i < loops; i++) {
            account = createAccount();

            String key = Seid.of("id", i).toString();
            keyList.add(key);

            client.set(key, account, exp);
        }

        N.println("Take " + (System.currentTimeMillis() - startTime) + " milliseconds to set " + loops + " object from redis. ");

        startTime = System.currentTimeMillis();

        for (String key : keyList) {
            account = (Account) client.get(key);
        }

        N.println("Take " + (System.currentTimeMillis() - startTime) + " milliseconds to get " + loops + " object from redis. ");

        N.println(account);
    }
}
