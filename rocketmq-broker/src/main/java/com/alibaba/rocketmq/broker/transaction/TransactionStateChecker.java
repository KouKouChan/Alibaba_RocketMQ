/*
 * Copyright (c) 2015. All Rights Reserved.
 */
package com.alibaba.rocketmq.broker.transaction;

public interface TransactionStateChecker {

    /**
     * <p>
     *  This method would perform transaction state check.
     * </p>
     *
     * <p>
     *     If the transaction has been lagged behind for specified time, this method will send check-transaction-state
     *     request to other online producer instance of same producer group.
     * </p>
     */
    void check();

}
