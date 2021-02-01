import java.util.Random;
import java.math.BigDecimal;
import java.math.BigInteger;

public class DecimalDivisionGenerator {
    /**
     * Generate a BigInteger that is scale-many digits long.
     *
     * @param random    The random instance to generate random numbers from.
     * @param scale     The number of digits in the final BigInteger generated.
     * @return A BigInteger that is scale-many digits long.
     */
    private static BigInteger generateBigInteger(Random random, int scale) {
        BigInteger bigInt = BigInteger.valueOf(random.nextInt(9) + 1);      // Start with a random digit from [1, 9].
        for (int j = 1; j < scale; j++) {                                   // Until we reach "scale" many digits:
            bigInt = bigInt.multiply(BigInteger.valueOf(10));               // Shift current digits to the left.
            bigInt = bigInt.add(BigInteger.valueOf(random.nextInt(10)));    // Add a random digit from [0, 9].
        }
        return bigInt;
    }

    public static void main(String[] args) {
        // TODO(WAN): In theory, our maximum scale should be 38 not 37, but the implementation breaks.
        // Our maximum scale is 37 digits because NoisePage represents fixed decimals
        // with an int128_t and 10^k > 2^128 for k > 38.5318.
        int maxScaleDigits = 38;
        int numTestCases = 1000000;
        Random random = new Random();

        for (int i = 0; i < numTestCases; i++) {
            int scale1 = (i % (maxScaleDigits - 1)) + 1;             // [1, maxScaleDigits - 1]
            BigInteger bigInt1 = generateBigInteger(random, scale1);

            for (int scale2 = 1; scale2 <= maxScaleDigits; scale2++) {
                BigInteger bigInt2 = generateBigInteger(random, scale2);

                BigDecimal bd1 = new BigDecimal("0." + bigInt1.toString());
                BigDecimal bd2 = new BigDecimal("0." + bigInt2.toString());
                BigDecimal res = bd1.divide(bd2, scale1, 1);

                System.out.printf("%s %d %s %d %s %d\n",
                        bd1.toString(), scale1,
                        bd2.toString(), scale2,
                        res.toString(), scale1
                );
            }
        }
    }
}