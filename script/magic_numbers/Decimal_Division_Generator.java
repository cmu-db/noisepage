import java.util.Random;
import java.math.BigDecimal;
import java.math.BigInteger;
public class Decimal_Division_Generator {
    public static void main(String[] args) {

        int no_of_test_cases = 1000000;

        Random rand = new Random(); //instance of random class
        //generate random values from 0-10
        int upperbound = 10;


        for (int i = 0; i < no_of_test_cases; i++) {

            int precision = i % 37;
            precision = precision + 1;

            int int_random = rand.nextInt(upperbound);
            if (int_random == 0) {
                int_random += 1;
            }
            BigInteger d_1 = new BigInteger(String.valueOf(int_random));

            for (int j = 1; j < precision; j++) {
                int random = rand.nextInt(upperbound);
                BigInteger ten = new BigInteger(String.valueOf(10));
                BigInteger random_big = new BigInteger(String.valueOf(random));
                d_1 = d_1.multiply(ten);
                d_1 = d_1.add(random_big);
            }

            String x = d_1.toString();
            x = "0." + x;

            for (int k = 0; k < 38; k++) {
                int precision2 = k + 1;
                int int_random2 = rand.nextInt(upperbound);
                if (int_random2 == 0) {
                    int_random2 += 1;
                }
                BigInteger d_2 = new BigInteger(String.valueOf(int_random2));

                for (int j = 1; j < precision2; j++) {
                    int random2 = rand.nextInt(upperbound);
                    BigInteger ten2 = new BigInteger(String.valueOf(10));
                    BigInteger random_big2 = new BigInteger(String.valueOf(random2));
                    d_2 = d_2.multiply(ten2);
                    d_2 = d_2.add(random_big2);
                }

                String y = d_2.toString();
                y = "0." + y;

                // Create two new BigDecimals
                BigDecimal bd1 =
                    new BigDecimal(x);
                BigDecimal bd2 =
                    new BigDecimal(y);


                // Division of two BigDecimals
                bd1 = bd1.divide(bd2, precision, 1);
                String z = bd1.toString();
                System.out.println(x + " " + String.valueOf(precision) +
                    " " + y + " " + String.valueOf(precision2) +
                    " " + z + " " + String.valueOf(precision));
            }

        }

    }
}