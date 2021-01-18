import java.util.Random;
import java.math.BigDecimal;
import java.math.BigInteger;

public class DecimalDivisionGenerator {
    /**
     * Generate a BigInteger that is precision-many digits long.
     *
     * @param random    The random instance to generate random numbers from.
     * @param precision The number of digits in the final BigInteger generated.
     * @return A BigInteger that is precision-many digits long.
     */
    private static BigInteger generateBigInteger(Random random, int precision) {
        BigInteger bigInt = BigInteger.valueOf(random.nextInt(9) + 1);      // Start with a random digit from [1, 9].
        for (int j = 1; j < precision; j++) {                               // Until we reach "precision" many digits:
            bigInt = bigInt.multiply(BigInteger.valueOf(10));               // Shift current digits to the left.
            bigInt = bigInt.add(BigInteger.valueOf(random.nextInt(10)));    // Add a random digit from [0, 9].
        }
        return bigInt;
    }

    public static void main(String[] args) {
        // Our maximum precision is 38 digits because NoisePage represents fixed decimals
        // with an int128_t and 10^k > 2^128 for k > 38.5318.
        int maxPrecisionDigits = 38;
        int numTestCases = 1000000;
        Random random = new Random();

        for (int i = 0; i < numTestCases; i++) {
            int precision1 = (i % (maxPrecisionDigits - 1)) + 1;             // [1, maxPrecisionDigits - 1]
            BigInteger bigInt1 = generateBigInteger(random, precision1);

            for (int precision2 = 1; precision2 <= maxPrecisionDigits; precision2++) {
                BigInteger bigInt2 = generateBigInteger(random, precision2);

                BigDecimal bd1 = new BigDecimal("0." + bigInt1.toString());
                BigDecimal bd2 = new BigDecimal("0." + bigInt2.toString());
                BigDecimal res = bd1.divide(bd2, precision1, 1);

                System.out.printf("%s %d %s %d %s %d\n",
                        bd1.toString(), precision1,
                        bd2.toString(), precision2,
                        res.toString(), precision1
                );
            }
        }
    }
}