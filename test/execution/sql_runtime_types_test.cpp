#include <fstream>
#include <sstream>

#include "common/error/exception.h"
#include "execution/sql/runtime_types.h"
#include "execution/tpl_test.h"

namespace noisepage::execution::sql::test {

class RuntimeTypesTest : public TplTest {};

// NOLINTNEXTLINE
TEST_F(RuntimeTypesTest, StringToDecimalTest) {
  Decimal128 d(0);

  d.RoundUpAndSet(std::string("1234.567"), 4);
  Decimal128 a_1(12345670);
  EXPECT_EQ(a_1, d);

  d.RoundUpAndSet(std::string("1234.56"), 4);
  Decimal128 a_2(12345600);
  EXPECT_EQ(a_2, d);

  d.RoundUpAndSet(std::string("1234."), 4);
  Decimal128 a_3(12340000);
  EXPECT_EQ(a_3, d);

  d.RoundUpAndSet(std::string("1234"), 4);
  Decimal128 a_4(12340000);
  EXPECT_EQ(a_4, d);

  d.RoundUpAndSet(std::string("123"), 4);
  Decimal128 a_5(1230000);
  EXPECT_EQ(a_5, d);

  d.RoundUpAndSet(std::string("123.5"), 4);
  Decimal128 a_6(1235000);
  EXPECT_EQ(a_6, d);

  d.RoundUpAndSet(std::string("1234.568"), 4);
  Decimal128 a_7(12345680);
  EXPECT_EQ(a_7, d);

  d.RoundUpAndSet(std::string("1234.5678"), 4);
  Decimal128 a_8(12345678);
  EXPECT_EQ(a_8, d);

  d.RoundUpAndSet(std::string("1234.56789"), 4);
  Decimal128 a_9(12345679);
  EXPECT_EQ(a_9, d);

  d.RoundUpAndSet(std::string("1234.56785"), 4);
  Decimal128 a_10(12345678);
  EXPECT_EQ(a_10, d);

  d.RoundUpAndSet(std::string("1234.56784"), 4);
  Decimal128 a_11(12345678);
  EXPECT_EQ(a_11, d);

  d.RoundUpAndSet(std::string("1234.56779"), 4);
  Decimal128 a_12(12345678);
  EXPECT_EQ(a_12, d);

  d.RoundUpAndSet(std::string("1234.56775"), 4);
  Decimal128 a_13(12345678);
  EXPECT_EQ(a_13, d);

  d.RoundUpAndSet(std::string("1234.56774"), 4);
  Decimal128 a_14(12345677);
  EXPECT_EQ(a_14, d);
}

TEST_F(RuntimeTypesTest, StringToNegativeDecimalTest) {
  Decimal128 d(0);

  d.RoundUpAndSet(std::string("-1234.567"), 4);
  Decimal128 a_1(-12345670);
  EXPECT_EQ(a_1, d);

  d.RoundUpAndSet(std::string("-1234.56"), 4);
  Decimal128 a_2(-12345600);
  EXPECT_EQ(a_2, d);

  d.RoundUpAndSet(std::string("-1234."), 4);
  Decimal128 a_3(-12340000);
  EXPECT_EQ(a_3, d);

  d.RoundUpAndSet(std::string("-1234"), 4);
  Decimal128 a_4(-12340000);
  EXPECT_EQ(a_4, d);

  d.RoundUpAndSet(std::string("-123"), 4);
  Decimal128 a_5(-1230000);
  EXPECT_EQ(a_5, d);

  d.RoundUpAndSet(std::string("-123.5"), 4);
  Decimal128 a_6(-1235000);
  EXPECT_EQ(a_6, d);

  d.RoundUpAndSet(std::string("-1234.568"), 4);
  Decimal128 a_7(-12345680);
  EXPECT_EQ(a_7, d);

  d.RoundUpAndSet(std::string("-1234.5678"), 4);
  Decimal128 a_8(-12345678);
  EXPECT_EQ(a_8, d);

  d.RoundUpAndSet(std::string("-1234.56789"), 4);
  Decimal128 a_9(-12345679);
  EXPECT_EQ(a_9, d);

  d.RoundUpAndSet(std::string("-1234.56785"), 4);
  Decimal128 a_10(-12345678);
  EXPECT_EQ(a_10, d);

  d.RoundUpAndSet(std::string("-1234.56784"), 4);
  Decimal128 a_11(-12345678);
  EXPECT_EQ(a_11, d);

  d.RoundUpAndSet(std::string("-1234.56779"), 4);
  Decimal128 a_12(-12345678);
  EXPECT_EQ(a_12, d);

  d.RoundUpAndSet(std::string("-1234.56775"), 4);
  Decimal128 a_13(-12345678);
  EXPECT_EQ(a_13, d);

  d.RoundUpAndSet(std::string("-1234.56774"), 4);
  Decimal128 a_14(-12345677);
  EXPECT_EQ(a_14, d);
}

// NOLINTNEXTLINE
TEST_F(RuntimeTypesTest, StringToDecimalMaxPrecisionTest) {
  Decimal128 d(0);
  int returned_precision;
  returned_precision = d.SetMaxmPrecision(std::string("1234.567"));
  Decimal128 a_1(1234567);
  EXPECT_EQ(a_1, d);
  EXPECT_EQ(returned_precision, 3);

  returned_precision = d.SetMaxmPrecision(std::string("1234.56"));
  Decimal128 a_2(123456);
  EXPECT_EQ(a_2, d);
  EXPECT_EQ(returned_precision, 2);

  returned_precision = d.SetMaxmPrecision(std::string("1234."));
  Decimal128 a_3(1234);
  EXPECT_EQ(a_3, d);
  EXPECT_EQ(returned_precision, 0);

  returned_precision = d.SetMaxmPrecision(std::string("1234"));
  Decimal128 a_4(1234);
  EXPECT_EQ(a_4, d);
  EXPECT_EQ(returned_precision, 0);

  returned_precision = d.SetMaxmPrecision(std::string("123"));
  Decimal128 a_5(123);
  EXPECT_EQ(a_5, d);
  EXPECT_EQ(returned_precision, 0);

  returned_precision = d.SetMaxmPrecision(std::string("123.5"));
  Decimal128 a_6(1235);
  EXPECT_EQ(a_6, d);
  EXPECT_EQ(returned_precision, 1);

  returned_precision = d.SetMaxmPrecision(std::string("1234.568"));
  Decimal128 a_7(1234568);
  EXPECT_EQ(a_7, d);
  EXPECT_EQ(returned_precision, 3);

  returned_precision = d.SetMaxmPrecision(std::string("1234.5678"));
  Decimal128 a_8(12345678);
  EXPECT_EQ(a_8, d);
  EXPECT_EQ(returned_precision, 4);

  returned_precision = d.SetMaxmPrecision(std::string("1234.56789"));
  Decimal128 a_9(123456789);
  EXPECT_EQ(a_9, d);
  EXPECT_EQ(returned_precision, 5);

  returned_precision = d.SetMaxmPrecision(std::string("1234.56785"));
  Decimal128 a_10(123456785);
  EXPECT_EQ(a_10, d);
  EXPECT_EQ(returned_precision, 5);

  returned_precision = d.SetMaxmPrecision(std::string("1234.56784"));
  Decimal128 a_11(123456784);
  EXPECT_EQ(a_11, d);
  EXPECT_EQ(returned_precision, 5);

  returned_precision = d.SetMaxmPrecision(std::string("1234.56779"));
  Decimal128 a_12(123456779);
  EXPECT_EQ(a_12, d);
  EXPECT_EQ(returned_precision, 5);

  returned_precision = d.SetMaxmPrecision(std::string("1234.56775"));
  Decimal128 a_13(123456775);
  EXPECT_EQ(a_13, d);
  EXPECT_EQ(returned_precision, 5);

  returned_precision = d.SetMaxmPrecision(std::string("1234.56774"));
  Decimal128 a_14(123456774);
  EXPECT_EQ(returned_precision, 5);
  EXPECT_EQ(a_14, d);
}

// NOLINTNEXTLINE
TEST_F(RuntimeTypesTest, StringToDecimalMaxPrecisionNegativeTest) {
  Decimal128 d(0);
  int returned_precision;
  returned_precision = d.SetMaxmPrecision(std::string("-1234.567"));
  Decimal128 a_1(-1234567);
  EXPECT_EQ(a_1, d);
  EXPECT_EQ(returned_precision, 3);

  returned_precision = d.SetMaxmPrecision(std::string("-1234.56"));
  Decimal128 a_2(-123456);
  EXPECT_EQ(a_2, d);
  EXPECT_EQ(returned_precision, 2);

  returned_precision = d.SetMaxmPrecision(std::string("-1234."));
  Decimal128 a_3(-1234);
  EXPECT_EQ(a_3, d);
  EXPECT_EQ(returned_precision, 0);

  returned_precision = d.SetMaxmPrecision(std::string("-1234"));
  Decimal128 a_4(-1234);
  EXPECT_EQ(a_4, d);
  EXPECT_EQ(returned_precision, 0);

  returned_precision = d.SetMaxmPrecision(std::string("-123"));
  Decimal128 a_5(-123);
  EXPECT_EQ(a_5, d);
  EXPECT_EQ(returned_precision, 0);

  returned_precision = d.SetMaxmPrecision(std::string("-123.5"));
  Decimal128 a_6(-1235);
  EXPECT_EQ(a_6, d);
  EXPECT_EQ(returned_precision, 1);

  returned_precision = d.SetMaxmPrecision(std::string("-1234.568"));
  Decimal128 a_7(-1234568);
  EXPECT_EQ(a_7, d);
  EXPECT_EQ(returned_precision, 3);

  returned_precision = d.SetMaxmPrecision(std::string("-1234.5678"));
  Decimal128 a_8(-12345678);
  EXPECT_EQ(a_8, d);
  EXPECT_EQ(returned_precision, 4);

  returned_precision = d.SetMaxmPrecision(std::string("-1234.56789"));
  Decimal128 a_9(-123456789);
  EXPECT_EQ(a_9, d);
  EXPECT_EQ(returned_precision, 5);

  returned_precision = d.SetMaxmPrecision(std::string("-1234.56785"));
  Decimal128 a_10(-123456785);
  EXPECT_EQ(a_10, d);
  EXPECT_EQ(returned_precision, 5);

  returned_precision = d.SetMaxmPrecision(std::string("-1234.56784"));
  Decimal128 a_11(-123456784);
  EXPECT_EQ(a_11, d);
  EXPECT_EQ(returned_precision, 5);

  returned_precision = d.SetMaxmPrecision(std::string("-1234.56779"));
  Decimal128 a_12(-123456779);
  EXPECT_EQ(a_12, d);
  EXPECT_EQ(returned_precision, 5);

  returned_precision = d.SetMaxmPrecision(std::string("-1234.56775"));
  Decimal128 a_13(-123456775);
  EXPECT_EQ(a_13, d);
  EXPECT_EQ(returned_precision, 5);

  returned_precision = d.SetMaxmPrecision(std::string("-1234.56774"));
  Decimal128 a_14(-123456774);
  EXPECT_EQ(returned_precision, 5);
  EXPECT_EQ(a_14, d);
}

// NOLINTNEXTLINE
TEST_F(RuntimeTypesTest, DecimalAdditionTest) {
  // Valid date
  Decimal128 d_1(0);
  Decimal128 d_2(0);

  d_1.RoundUpAndSet(std::string("1234.5678"), 4);
  d_2.RoundUpAndSet(std::string("1234.6429"), 4);
  Decimal128 d(24692107);
  d_1 += d_2;
  EXPECT_EQ(d, d_1);

  d_1.RoundUpAndSet(std::string(".00012345000098765"), 17);
  d_2.RoundUpAndSet(std::string("123.45"), 17);
  d.RoundUpAndSet("123.45012345000098765", 17);
  d_1 += d_2;
  EXPECT_EQ(d, d_1);

  d_1.RoundUpAndSet(std::string("1234500009876.5"), 17);
  d_2.RoundUpAndSet(std::string(".00012345000098765"), 17);
  d.RoundUpAndSet("1234500009876.50012345000098765", 17);
  d_1 += d_2;
  EXPECT_EQ(d, d_1);

  d_1.RoundUpAndSet(std::string("9999909999999.5"), 3);
  d_2.RoundUpAndSet(std::string(".555"), 3);
  d.RoundUpAndSet("9999910000000.055", 3);
  d_1 += d_2;
  EXPECT_EQ(d, d_1);

  d_1.RoundUpAndSet(std::string("-12345"), 2);
  d_2.RoundUpAndSet(std::string("-123.45"), 2);
  d.RoundUpAndSet("-12468.45", 2);
  d_1 += d_2;
  EXPECT_EQ(d, d_1);
}

// NOLINTNEXTLINE
TEST_F(RuntimeTypesTest, DecimalSubtractionTest) {
  // Valid date
  Decimal128 d_1(0);
  Decimal128 d_2(0);

  d_1.RoundUpAndSet(std::string("1234.5678"), 4);
  d_2.RoundUpAndSet(std::string("2469.2107"), 4);
  Decimal128 d(-12346429);
  d_1 -= d_2;
  EXPECT_EQ(d, d_1);

  d_1.RoundUpAndSet(std::string("1234.5678"), 4);
  d_2.RoundUpAndSet(std::string("1234.5679"), 4);
  d.RoundUpAndSet("-00000.0001", 4);
  d_1 -= d_2;
  EXPECT_EQ(d, d_1);

  d_1.RoundUpAndSet(std::string(".00012345000098765"), 17);
  d_2.RoundUpAndSet(std::string("123.45"), 17);
  d.RoundUpAndSet("-123.44987654999901235", 17);
  d_1 -= d_2;
  EXPECT_EQ(d, d_1);

  d_1.RoundUpAndSet(std::string("1234500009876.5"), 17);
  d_2.RoundUpAndSet(std::string(".00012345000098765"), 17);
  d.RoundUpAndSet("1234500009876.49987654999901235", 17);
  d_1 -= d_2;
  EXPECT_EQ(d, d_1);

  d_1.RoundUpAndSet(std::string("9999900000000.5"), 3);
  d_2.RoundUpAndSet(std::string(".555"), 3);
  d.RoundUpAndSet("9999899999999.945", 3);
  d_1 -= d_2;
  EXPECT_EQ(d, d_1);

  d_1.RoundUpAndSet(std::string("1111.5551"), 4);
  d_2.RoundUpAndSet(std::string("1111.555"), 4);
  d.RoundUpAndSet("0.0001", 4);
  d_1 -= d_2;
  EXPECT_EQ(d, d_1);

  d_1.RoundUpAndSet(std::string("1000001000"), 1);
  d_2.RoundUpAndSet(std::string("0.1"), 1);
  d.RoundUpAndSet("1000000999.9", 1);
  d_1 -= d_2;
  EXPECT_EQ(d, d_1);
}

// NOLINTNEXTLINE
TEST_F(RuntimeTypesTest, StringToDecimalMultiPrecisionTest) {
  Decimal128 d(0);

  d.RoundUpAndSet(std::string("1234.567"), 4);
  Decimal128 a_1(12345670);
  EXPECT_EQ(a_1, d);

  d.RoundUpAndSet(std::string("1234.567"), 3);
  Decimal128 a_2(1234567);
  EXPECT_EQ(a_2, d);

  d.RoundUpAndSet(std::string("1234.567"), 5);
  Decimal128 a_3(123456700);
  EXPECT_EQ(a_3, d);

  d.RoundUpAndSet(std::string("1234.567"), 2);
  Decimal128 a_4(123457);
  EXPECT_EQ(a_4, d);

  d.RoundUpAndSet(std::string("1234.567"), 1);
  Decimal128 a_5(12346);
  EXPECT_EQ(a_5, d);

  d.RoundUpAndSet(std::string("1234.567"), 0);
  Decimal128 a_6(1234);
  EXPECT_EQ(a_6, d);
}

// NOLINTNEXTLINE
TEST_F(RuntimeTypesTest, DecimalMultiplicationTest) {
  Decimal128 d_1(0);
  Decimal128 d_2(0);
  Decimal128 d(0);

  // Overflow Algorithm 1 - Magic number is < 2^256
  d_1.RoundUpAndSet(std::string("0.2148327859723895720384199"), 25);
  d_2.RoundUpAndSet(std::string("0.3278598274982374859061277"), 25);
  d.RoundUpAndSet("0.0704350401498734190382129", 25);
  d_1.MultiplyAndSet(d_2, 25);
  EXPECT_EQ(d, d_1);

  d_1.RoundUpAndSet(std::string("0.7582386326849632823554847"), 25);
  d_2.RoundUpAndSet(std::string("0.7472136320201879174717897"), 25);
  d.RoundUpAndSet("0.5665662426665525849360499", 25);
  d_1.MultiplyAndSet(d_2, 25);
  EXPECT_EQ(d, d_1);

  // Overflow Algorithm 2 - Magic number is > 2^256
  d_1.RoundUpAndSet(std::string("0.892038085789327580372041421"), 27);
  d_2.RoundUpAndSet(std::string("0.273953192085891327489327489"), 27);
  d.RoundUpAndSet("0.244376681064174465536041239", 27);
  d_1.MultiplyAndSet(d_2, 27);
  EXPECT_EQ(d, d_1);

  d_1.RoundUpAndSet(std::string("0.728153698365712865782136987"), 27);
  d_2.RoundUpAndSet(std::string("0.920138918390128401275810278"), 27);
  d.RoundUpAndSet("0.670002556435998842845938898", 27);
  d_1.MultiplyAndSet(d_2, 27);
  EXPECT_EQ(d, d_1);
}

TEST_F(RuntimeTypesTest, DISABLED_DecimalMultiplicationRegressionTest) {
  // Please change the input argument of infile, with the path where you stored the file
  // generated by the multiplication script provided
  std::ifstream infile("");
  if (!infile.is_open()) {
    return;
  }
  Decimal128 d_1(0);
  Decimal128 d_2(0);
  Decimal128 d(0);

  std::string line;
  while (std::getline(infile, line)) {
    std::stringstream linestream(line);
    std::string decimal1, decimal2, result;
    unsigned precision_decimal1, precision_decimal2, precision_result;
    linestream >> decimal1 >> precision_decimal1 >> decimal2 >> precision_decimal2 >> result >> precision_result;
    d_1.RoundUpAndSet(decimal1, precision_decimal1);
    d_2.RoundUpAndSet(decimal2, precision_decimal2);
    d.RoundUpAndSet(result, precision_result);
    d_1.MultiplyAndSet(d_2, precision_decimal1);
    EXPECT_EQ(d, d_1);
  }
}

// NOLINTNEXTLINE
TEST_F(RuntimeTypesTest, DecimalDivisionTest) {
  Decimal128 d_1(0);
  Decimal128 d_2(0);
  Decimal128 d(0);

  d_1.RoundUpAndSet(std::string("0.14123243242341419"), 17);
  d_2.RoundUpAndSet(std::string("0.10218327103891902"), 17);
  d.RoundUpAndSet("1.38214828109801195", 17);
  d_1.SignedDivideWithDecimal(d_2, 17);
  EXPECT_EQ(d, d_1);

  d_1.RoundUpAndSet(std::string("0.1412324324234141232432423"), 25);
  d_2.RoundUpAndSet(std::string("0.1021832710389190247920184"), 25);
  d.RoundUpAndSet("1.3821482810980112392853736", 25);
  d_1.SignedDivideWithDecimal(d_2, 25);
  EXPECT_EQ(d, d_1);

  d_1.RoundUpAndSet(std::string("1.12412"), 5);
  d_2.RoundUpAndSet(std::string("7.213"), 3);
  d.RoundUpAndSet("0.15584", 5);
  d_1.SignedDivideWithDecimal(d_2, 3);
  EXPECT_EQ(d, d_1);

  d_1.RoundUpAndSet(std::string("1.12412"), 10);
  d_2.RoundUpAndSet(std::string("7.213"), 3);
  d.RoundUpAndSet("0.1558463884", 10);
  d_1.SignedDivideWithDecimal(d_2, 3);
  EXPECT_EQ(d, d_1);

  d_1.RoundUpAndSet(std::string("0.174742476062277562382"), 21);
  d_2.RoundUpAndSet(std::string("0.18347228288313502339555553"), 26);
  d.RoundUpAndSet("0.952418933891949111511", 21);
  d_1.SignedDivideWithDecimal(d_2, 26);
  EXPECT_EQ(d, d_1);

  d_1.RoundUpAndSet(std::string("0.215133535198406993127682632256305281"), 36);
  d_2.RoundUpAndSet(std::string("0.512"), 3);
  d.RoundUpAndSet("0.420182685934388658452505141125596251", 36);
  d_1.SignedDivideWithDecimal(d_2, 3);
  EXPECT_EQ(d, d_1);

  // Magic division tests
  d_1.RoundUpAndSet(std::string("0.174742476062"), 12);
  d_2.RoundUpAndSet(std::string("0.0005"), 4);
  d.RoundUpAndSet("349.484952124000", 12);
  d_1.SignedDivideWithDecimal(d_2, 4);
  EXPECT_EQ(d, d_1);

  d_1.RoundUpAndSet(std::string("0.174742476062277562382"), 21);
  d_2.RoundUpAndSet(std::string("0.00000005"), 8);
  d.RoundUpAndSet("3494849.521245551247640000000", 21);
  d_1.SignedDivideWithDecimal(d_2, 8);
  EXPECT_EQ(d, d_1);

  d_1.RoundUpAndSet(std::string("0.174742476062"), 12);
  d_2.RoundUpAndSet(std::string("0.0007"), 4);
  d.RoundUpAndSet("249.632108660000", 12);
  d_1.SignedDivideWithDecimal(d_2, 4);
  EXPECT_EQ(d, d_1);

  // 256 bit algo 0
  d_1.RoundUpAndSet(std::string("0.174742476062277562382"), 21);
  d_2.RoundUpAndSet(std::string("0.0000000000000000777"), 19);
  d.RoundUpAndSet("2248937915859428.087284427284427284427", 21);
  d_1.SignedDivideWithDecimal(d_2, 19);
  EXPECT_EQ(d, d_1);

  // 256 bit algo 1
  d_1.RoundUpAndSet(std::string("0.174742476062277562382"), 21);
  d_2.RoundUpAndSet(std::string("0.0000000000000000999"), 19);
  d.RoundUpAndSet("1749173934557332.956776776776776776776", 21);
  d_1.SignedDivideWithDecimal(d_2, 19);
  EXPECT_EQ(d, d_1);
}

TEST_F(RuntimeTypesTest, DecimalDivisionRegressionTest) {
  // Please change the input argument of infile, with the path where you stored the file
  // generated by the multiplication script provided
  std::ifstream infile("");
  if (!infile.is_open()) {
    return;
  }
  Decimal128 d_1(0);
  Decimal128 d_2(0);
  Decimal128 d(0);

  std::string line;
  while (std::getline(infile, line)) {
    std::stringstream linestream(line);
    std::string decimal1, decimal2, result;
    unsigned precision_decimal1, precision_decimal2, precision_result;
    linestream >> decimal1 >> precision_decimal1 >> decimal2 >> precision_decimal2 >> result >> precision_result;
    d_1.RoundUpAndSet(decimal1, precision_decimal1);
    d_2.RoundUpAndSet(decimal2, precision_decimal2);
    d.RoundUpAndSet(result, precision_result);
    d_1.SignedDivideWithDecimal(d_2, precision_decimal2);
    EXPECT_EQ(d, d_1);
  }
}

// NOLINTNEXTLINE
TEST_F(RuntimeTypesTest, LeadingNumberZerosTest) {
  uint128_t x = 1;
  for (int i = 0; i < 127; i++) {
    EXPECT_EQ(noisepage::execution::sql::Nlz128(x), noisepage::execution::sql::Nlz128Fast(x));
    x = x << 1;
  }
  x = 324325;
  for (int i = 0; i < 127; i++) {
    EXPECT_EQ(noisepage::execution::sql::Nlz128(x), noisepage::execution::sql::Nlz128Fast(x));
    x = x << 1;
  }
}

// NOLINTNEXTLINE
TEST_F(RuntimeTypesTest, ExtractDateParts) {
  // Valid date
  Date d;
  EXPECT_NO_THROW({ d = Date::FromYMD(2016, 12, 19); });
  EXPECT_EQ(2016, d.ExtractYear());
  EXPECT_EQ(12, d.ExtractMonth());
  EXPECT_EQ(19, d.ExtractDay());

  // BC date.
  EXPECT_NO_THROW({ d = Date::FromYMD(-4000, 1, 2); });
  EXPECT_EQ(-4000, d.ExtractYear());
  EXPECT_EQ(1, d.ExtractMonth());
  EXPECT_EQ(2, d.ExtractDay());

  // Invalid
  EXPECT_THROW({ d = Date::FromYMD(1234, 3, 1111); }, ConversionException);
  EXPECT_THROW({ d = Date::FromYMD(1234, 93874, 11); }, ConversionException);
  EXPECT_THROW({ d = Date::FromYMD(1234, 7283, 192873); }, ConversionException);
  EXPECT_THROW({ d = Date::FromYMD(-40000, 12, 12); }, ConversionException);
  EXPECT_THROW({ d = Date::FromYMD(50000000, 12, 987); }, ConversionException);
  EXPECT_THROW({ d = Date::FromYMD(50000000, 921873, 1); }, ConversionException);
  EXPECT_THROW({ d = Date::FromYMD(-50000000, 921873, 21938); }, ConversionException);
}

// NOLINTNEXTLINE
TEST_F(RuntimeTypesTest, DateFromString) {
  // Valid date
  Date d;
  EXPECT_NO_THROW({ d = Date::FromString("1990-01-11"); });
  EXPECT_EQ(1990u, d.ExtractYear());
  EXPECT_EQ(1u, d.ExtractMonth());
  EXPECT_EQ(11u, d.ExtractDay());

  EXPECT_NO_THROW({ d = Date::FromString("2015-3-1"); });
  EXPECT_EQ(2015, d.ExtractYear());
  EXPECT_EQ(3u, d.ExtractMonth());
  EXPECT_EQ(1u, d.ExtractDay());

  EXPECT_NO_THROW({ d = Date::FromString("   1999-12-31    "); });
  EXPECT_EQ(1999, d.ExtractYear());
  EXPECT_EQ(12u, d.ExtractMonth());
  EXPECT_EQ(31u, d.ExtractDay());

  // Invalid
  EXPECT_THROW({ d = Date::FromString("1000-11-23123"); }, ConversionException);
  EXPECT_THROW({ d = Date::FromString("1000-12323-19"); }, ConversionException);
  EXPECT_THROW({ d = Date::FromString("1000-12323-199"); }, ConversionException);
  EXPECT_THROW({ d = Date::FromString("50000000-12-20"); }, ConversionException);
  EXPECT_THROW({ d = Date::FromString("50000000-12-120"); }, ConversionException);
  EXPECT_THROW({ d = Date::FromString("50000000-1289217-12"); }, ConversionException);
  EXPECT_THROW({ d = Date::FromString("da fuk?"); }, ConversionException);
  EXPECT_THROW({ d = Date::FromString("-1-1-23"); }, ConversionException);
}

// NOLINTNEXTLINE
TEST_F(RuntimeTypesTest, DateComparisons) {
  Date d1 = Date::FromString("2000-01-01");
  Date d2 = Date::FromString("2016-02-19");
  Date d3 = d1;
  Date d4 = Date::FromString("2017-10-10");
  EXPECT_NE(d1, d2);
  EXPECT_LT(d1, d2);
  EXPECT_EQ(d1, d3);
  EXPECT_GT(d4, d3);
  EXPECT_GT(d4, d2);
  EXPECT_GT(d4, d1);

  d1 = Date::FromYMD(-4000, 1, 1);
  d2 = Date::FromYMD(-4000, 1, 2);
  EXPECT_NE(d1, d2);
  EXPECT_LT(d1, d2);
}

// NOLINTNEXTLINE
TEST_F(RuntimeTypesTest, DateToString) {
  Date d1 = Date::FromString("2016-01-27");
  EXPECT_EQ("2016-01-27", d1.ToString());

  // Make sure we pad months and days
  d1 = Date::FromString("2000-1-1");
  EXPECT_EQ("2000-01-01", d1.ToString());
}

// NOLINTNEXTLINE
TEST_F(RuntimeTypesTest, DateYMDStringEqualityTest) {
  auto ymd_res = Date::FromYMD(2020, 1, 1);
  auto res = Date::FromString("2020-01-01");
  EXPECT_EQ(res, ymd_res);
  EXPECT_EQ(res.ToString(), "2020-01-01");
  EXPECT_EQ(ymd_res.ToString(), "2020-01-01");
}

// NOLINTNEXTLINE
TEST_F(RuntimeTypesTest, ExtractTimestampParts) {
  // Valid timestamp.
  Timestamp t;
  EXPECT_NO_THROW({ t = Timestamp::FromYMDHMS(2016, 12, 19, 10, 20, 30); });
  EXPECT_EQ(2016, t.ExtractYear());
  EXPECT_EQ(12, t.ExtractMonth());
  EXPECT_EQ(19, t.ExtractDay());
  EXPECT_EQ(10, t.ExtractHour());
  EXPECT_EQ(20, t.ExtractMinute());
  EXPECT_EQ(30, t.ExtractSecond());

  // BC timestamp.
  EXPECT_NO_THROW({ t = Timestamp::FromYMDHMS(-4000, 1, 2, 12, 24, 48); });
  EXPECT_EQ(-4000, t.ExtractYear());
  EXPECT_EQ(1, t.ExtractMonth());
  EXPECT_EQ(2, t.ExtractDay());
  EXPECT_EQ(12, t.ExtractHour());
  EXPECT_EQ(24, t.ExtractMinute());
  EXPECT_EQ(48, t.ExtractSecond());

  // Invalid
  EXPECT_THROW({ t = Timestamp::FromYMDHMS(1234, 3, 4, 1, 1, 100); }, ConversionException);
  EXPECT_THROW({ t = Timestamp::FromYMDHMS(1234, 3, 4, 1, 100, 1); }, ConversionException);
  EXPECT_THROW({ t = Timestamp::FromYMDHMS(1234, 3, 4, 1, 100, 100); }, ConversionException);
  EXPECT_THROW({ t = Timestamp::FromYMDHMS(1234, 3, 4, 25, 1, 1); }, ConversionException);
  EXPECT_THROW({ t = Timestamp::FromYMDHMS(1234, 3, 4, 25, 1, 100); }, ConversionException);
  EXPECT_THROW({ t = Timestamp::FromYMDHMS(1234, 3, 4, 25, 100, 1); }, ConversionException);
  EXPECT_THROW({ t = Timestamp::FromYMDHMS(1234, 3, 4, 25, 100, 100); }, ConversionException);
  EXPECT_THROW({ t = Timestamp::FromYMDHMS(50000000, 12, 9, 100, 1, 1); }, ConversionException);
  EXPECT_THROW({ t = Timestamp::FromYMDHMS(50000000, 92187, 1, 13, 59, 60); }, ConversionException);
}

// NOLINTNEXTLINE
TEST_F(RuntimeTypesTest, TimestampFromString) {
  Timestamp res;
  EXPECT_NO_THROW({ res = Timestamp::FromString("2020-01-11 11:22:33.123456"); });
  EXPECT_EQ(2020u, res.ExtractYear());
  EXPECT_EQ(1u, res.ExtractMonth());
  EXPECT_EQ(11u, res.ExtractDay());
  EXPECT_EQ(11u, res.ExtractHour());
  EXPECT_EQ(22u, res.ExtractMinute());
  EXPECT_EQ(33u, res.ExtractSecond());
  EXPECT_EQ(123u, res.ExtractMillis());
  EXPECT_EQ(456u, res.ExtractMicros());
  EXPECT_EQ(res.ToString(), "2020-01-11 11:22:33.123456");

  EXPECT_NO_THROW({ res = Timestamp::FromString("2020-01-11 11:22:33.123456::timestamp"); });
  EXPECT_EQ(2020u, res.ExtractYear());
  EXPECT_EQ(1u, res.ExtractMonth());
  EXPECT_EQ(11u, res.ExtractDay());
  EXPECT_EQ(11u, res.ExtractHour());
  EXPECT_EQ(22u, res.ExtractMinute());
  EXPECT_EQ(33u, res.ExtractSecond());
  EXPECT_EQ(123u, res.ExtractMillis());
  EXPECT_EQ(456u, res.ExtractMicros());
  EXPECT_EQ(res.ToString(), "2020-01-11 11:22:33.123456");

  EXPECT_NO_THROW({ res = Timestamp::FromString("2020-01-11 11:22:33.123456-05::timestamp"); });
  EXPECT_EQ(2020u, res.ExtractYear());
  EXPECT_EQ(1u, res.ExtractMonth());
  EXPECT_EQ(11u, res.ExtractDay());
  EXPECT_EQ(16u, res.ExtractHour());
  EXPECT_EQ(22u, res.ExtractMinute());
  EXPECT_EQ(33u, res.ExtractSecond());
  EXPECT_EQ(123u, res.ExtractMillis());
  EXPECT_EQ(456u, res.ExtractMicros());
  EXPECT_EQ(res.ToString(), "2020-01-11 16:22:33.123456");

  EXPECT_NO_THROW({ res = Timestamp::FromString("2020-01-11"); });
  EXPECT_EQ(2020u, res.ExtractYear());
  EXPECT_EQ(1u, res.ExtractMonth());
  EXPECT_EQ(11u, res.ExtractDay());
  EXPECT_EQ(0u, res.ExtractHour());
  EXPECT_EQ(0u, res.ExtractMinute());
  EXPECT_EQ(0u, res.ExtractSecond());
  EXPECT_EQ(0u, res.ExtractMillis());
  EXPECT_EQ(0u, res.ExtractMicros());
  EXPECT_EQ(res.ToString(), "2020-01-11 00:00:00.000000");

  // Invalid dates
  EXPECT_THROW({ res = Timestamp::FromString("1000-12323-19"); }, ConversionException);
  EXPECT_THROW({ res = Timestamp::FromString("1000-11-23123"); }, ConversionException);
  EXPECT_THROW({ res = Timestamp::FromString("1000-12323-199"); }, ConversionException);
  EXPECT_THROW({ res = Timestamp::FromString("50000000-12-20"); }, ConversionException);
  EXPECT_THROW({ res = Timestamp::FromString("50000000-12-120"); }, ConversionException);
  EXPECT_THROW({ res = Timestamp::FromString("50000000-1289217-12"); }, ConversionException);
  EXPECT_THROW({ res = Timestamp::FromString("da fuk?"); }, ConversionException);
  EXPECT_THROW({ res = Timestamp::FromString("-1-1-23"); }, ConversionException);

  // Invalid timestamps
  EXPECT_THROW({ res = Timestamp::FromString("2020-01-11 25:00:01"); }, ConversionException);
  EXPECT_THROW({ res = Timestamp::FromString("2020-01-11 21:00:00::timestamps"); }, ConversionException);
  EXPECT_THROW({ res = Timestamp::FromString("2020-01-11 24:15:11::timestamp"); }, ConversionException);
  EXPECT_THROW({ res = Timestamp::FromString("2020-01-11 24:00:00.11::timestamp"); }, ConversionException);
  EXPECT_THROW({ res = Timestamp::FromString("2020-01-11 24:00:00.000000::times"); }, ConversionException);
}

// NOLINTNEXTLINE
TEST_F(RuntimeTypesTest, TimeZoneFromString) {
  Timestamp res;
  EXPECT_NO_THROW({ res = Timestamp::FromString("2020-12-31 23:22:33.123456-05"); });
  EXPECT_EQ(2021u, res.ExtractYear());
  EXPECT_EQ(1u, res.ExtractMonth());
  EXPECT_EQ(1u, res.ExtractDay());
  EXPECT_EQ(4u, res.ExtractHour());
  EXPECT_EQ(22u, res.ExtractMinute());
  EXPECT_EQ(33u, res.ExtractSecond());
  EXPECT_EQ(123u, res.ExtractMillis());
  EXPECT_EQ(456u, res.ExtractMicros());
  EXPECT_EQ(res.ToString(), "2021-01-01 04:22:33.123456");

  EXPECT_NO_THROW({ res = Timestamp::FromString("2020-01-01 01:22:33.123456+05::timestamp"); });
  EXPECT_EQ(2019u, res.ExtractYear());
  EXPECT_EQ(12u, res.ExtractMonth());
  EXPECT_EQ(31u, res.ExtractDay());
  EXPECT_EQ(20u, res.ExtractHour());
  EXPECT_EQ(22u, res.ExtractMinute());
  EXPECT_EQ(33u, res.ExtractSecond());
  EXPECT_EQ(123u, res.ExtractMillis());
  EXPECT_EQ(456u, res.ExtractMicros());
  EXPECT_EQ(res.ToString(), "2019-12-31 20:22:33.123456");
}

// NOLINTNEXTLINE
TEST_F(RuntimeTypesTest, TimestampComparisons) {
  Timestamp t1 = Timestamp::FromYMDHMS(2000, 1, 1, 12, 0, 0);
  Timestamp t2 = Timestamp::FromYMDHMS(2000, 1, 1, 16, 0, 0);
  Timestamp t3 = t1;
  Timestamp t4 = Timestamp::FromYMDHMS(2017, 1, 1, 18, 18, 18);

  EXPECT_NE(t1, t2);
  EXPECT_LT(t1, t2);
  EXPECT_EQ(t1, t3);
  EXPECT_GT(t4, t3);
  EXPECT_GT(t4, t2);
  EXPECT_GT(t4, t1);

  t1 = Timestamp::FromYMDHMS(-4000, 1, 1, 10, 10, 10);
  t2 = Timestamp::FromYMDHMS(-4000, 1, 1, 10, 10, 11);
  EXPECT_NE(t1, t2);
  EXPECT_LT(t1, t2);
}

// NOLINTNEXTLINE
TEST_F(RuntimeTypesTest, TSYMDHMSMUStringEqualityTest) {
  auto ymdhmsmu_res = Timestamp::FromYMDHMSMU(2020, 1, 11, 10, 12, 13, 123, 432);
  auto res = Timestamp::FromString("2020-01-11 10:12:13.123432::timestamp");
  EXPECT_EQ(res, ymdhmsmu_res);
  EXPECT_EQ(res.ToString(), "2020-01-11 10:12:13.123432");
  EXPECT_EQ(ymdhmsmu_res.ToString(), "2020-01-11 10:12:13.123432");
}

// NOLINTNEXTLINE
TEST_F(RuntimeTypesTest, TSYMDHMStringEqualityTest) {
  auto ymdhms_res = Timestamp::FromYMDHMS(2020, 1, 11, 10, 12, 13);
  auto res = Timestamp::FromString("2020-01-11 10:12:13::timestamp");
  EXPECT_EQ(res, ymdhms_res);
  EXPECT_EQ(res.ToString(), "2020-01-11 10:12:13.000000");
  EXPECT_EQ(ymdhms_res.ToString(), "2020-01-11 10:12:13.000000");
}

// NOLINTNEXTLINE
TEST_F(RuntimeTypesTest, VarlenComparisons) {
  // Short strings first.
  {
    auto v1 = storage::VarlenEntry::Create("somethings");
    auto v2 = storage::VarlenEntry::Create("anotherone");
    auto v3 = v1;
    EXPECT_TRUE(v1.IsInlined());
    EXPECT_TRUE(v2.IsInlined());
    EXPECT_NE(v1, v2);
    EXPECT_LT(v2, v1);
    EXPECT_GT(v1, v2);
    EXPECT_EQ(v1, v3);
  }

  // Very short strings.
  {
    auto v1 = storage::VarlenEntry::Create("a");
    auto v2 = storage::VarlenEntry::Create("b");
    auto v3 = v1;
    auto v4 = storage::VarlenEntry::Create("");
    EXPECT_TRUE(v1.IsInlined());
    EXPECT_TRUE(v2.IsInlined());
    EXPECT_TRUE(v3.IsInlined());
    EXPECT_TRUE(v4.IsInlined());
    EXPECT_NE(v1, v2);
    EXPECT_LT(v1, v2);
    EXPECT_GT(v2, v1);
    EXPECT_EQ(v1, v3);
    EXPECT_NE(v1, v4);
    EXPECT_NE(v2, v4);
    EXPECT_NE(v3, v4);
    EXPECT_LT(v4, v1);
  }

  // Longer strings.
  auto s1 = "This is sort of a long string, but the end of the string should be different than XXX";
  auto s2 = "This is sort of a long string, but the end of the string should be different than YYY";
  {
    auto v1 = storage::VarlenEntry::Create(s1);
    auto v2 = storage::VarlenEntry::Create(s2);
    auto v3 = storage::VarlenEntry::Create("smallstring");
    auto v4 = storage::VarlenEntry::Create("This is so");  // A prefix of the longer strings.
    EXPECT_FALSE(v1.IsInlined());
    EXPECT_FALSE(v2.IsInlined());
    auto UNUSED_ATTRIBUTE foo = v1 == v2;
    EXPECT_NE(v1, v2);
    EXPECT_LT(v1, v2);
    EXPECT_GT(v2, v1);
    EXPECT_EQ(v2, v2);

    EXPECT_NE(v1, v3);
    EXPECT_NE(v2, v3);
    EXPECT_GT(v3, v1);
    EXPECT_GT(v3, v2);

    EXPECT_LT(v4, v1);
    EXPECT_LT(v4, v2);
  }
}

}  // namespace noisepage::execution::sql::test
