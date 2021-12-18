/*
 * Name:   Encapsulation of the product, quantity, and timestamp values
 * Author: Bhaskar S
 * Date:   12/10/2021
 * Blog:   https://www.polarsparc.com
 */

package com.polarsparc.kstreams.model;

public class QtyEvent {
    private String product;
    private int quantity;
    private long timestamp;

    public QtyEvent() {
        this.product = null;
        this.quantity = 0;
        this.timestamp = 0;
    }

    public QtyEvent(String p, int q) {
        this.product = p;
        this.quantity = q;
        this.timestamp = 0;
    }

    public String getProduct() {
        return product;
    }

    public void setProduct(String p) {
        this.product = p;
    }

    public int getQuantity() {
        return quantity;
    }

    public void setQuantity(int q) {
        this.quantity = q;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long t) {
        this.timestamp = t;
    }

    public String toString() {
        return String.format("{%s, %d, %d}", product, quantity, timestamp);
    }
}
