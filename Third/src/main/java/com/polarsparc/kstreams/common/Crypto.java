/*
 * Name:   Crypto Utility
 * Author: Bhaskar S
 * Date:   11/24/2021
 * Blog:   https://www.polarsparc.com
 */

package com.polarsparc.kstreams.common;

import com.polarsparc.kstreams.model.CryptoAlert;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public interface Crypto {
    String CRYPTO_ALERTS_TOPIC = "crypto-price-alerts";

    List<String> CRYPTO_LIST = Arrays.asList("ANT", "BEE", "CAT", "DOG", "EEL");
    List<Double> CRYPTO_PRICE_LIST = Arrays.asList(10.50, 9.75, 12.25, 11.50, 8.00);
    List<Integer> PERCENT_LIST = Arrays.asList(5, 10, 15, 20, 25, 30, 35, 40, 45, 50);

    Random random = new Random(System.currentTimeMillis());

    static CryptoAlert generateNextCryptoAlert() {
        int cp = random.nextInt(Crypto.CRYPTO_LIST.size());
        int pp = random.nextInt(Crypto.PERCENT_LIST.size());

        double offset = Crypto.CRYPTO_PRICE_LIST.get(cp) * Crypto.PERCENT_LIST.get(pp) / 100;

        boolean up = true;
        double price = Crypto.CRYPTO_PRICE_LIST.get(cp);
        if (random.nextInt(100) % 2 != 0) {
            up = false;
            price -= offset;
        } else {
            price += offset;
        }

        return new CryptoAlert(Crypto.CRYPTO_LIST.get(cp),
                Crypto.PERCENT_LIST.get(pp),
                up,
                price,
                LocalDateTime.now());
    }
}
