/*
 * Name:   Crypto Alert
 * Author: Bhaskar S
 * Date:   11/24/2021
 * Blog:   https://www.polarsparc.com
 */

package com.polarsparc.kstreams.model;

import java.time.LocalDateTime;

public record CryptoAlert(String name, int change, boolean up, double price, LocalDateTime timestamp) {}
