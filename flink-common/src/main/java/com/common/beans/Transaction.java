package com.common.beans;

import lombok.Data;

import java.util.Date;

/**
 * @author zt
 */
@Data
public class Transaction {
    private String txId;

    private String txOp;

    private Date ts;
}
