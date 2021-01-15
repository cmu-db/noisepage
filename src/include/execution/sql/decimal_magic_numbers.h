#pragma once

#include <map>

namespace noisepage::execution::sql {

/* Magic array for 256 bit division with powers of 10
 * This map is used after multiplication of decimals to get
 * the correct result. This represents the 256 bit magic
 * number in 4 128 bit uint32_t integers with each having 64 bits*/
constexpr uint128_t MagicArray[39][4] = {
    {0x0, 0x0, 0x0, 0x0},
    {0xcccccccccccccccc, 0xcccccccccccccccc, 0xcccccccccccccccc, 0xcccccccccccccccd},
    {0x47ae147ae147ae14, 0x7ae147ae147ae147, 0xae147ae147ae147a, 0xe147ae147ae147af},
    {0x624dd2f1a9fbe76, 0xc8b4395810624dd2, 0xf1a9fbe76c8b4395, 0x810624dd2f1a9fbf},
    {0x346dc5d63886594a, 0xf4f0d844d013a92a, 0x305532617c1bda51, 0x19ce075f6fd21ff3},
    {0xa7c5ac471b478423, 0x0fcf80dc33721d53, 0xcddd6e04c0592103, 0x85c67dfe32a0663d},
    {0x8637bd05af6c69b5, 0xa63f9a49c2c1b10f, 0xd7e45803cd141a69, 0x37d1fe64f54d1e97},
    {0xd6bf94d5e57a42bc, 0x3d32907604691b4c, 0x8ca08cd2e1b9c3db, 0x8c8330a1887b6425},
    {0xabcc77118461cefc, 0xfdc20d2b36ba7c3d, 0x3d4d3d758161697c, 0x7068f3b46d2f8351},
    {0x89705f4136b4a597, 0x31680a88f8953030, 0xfdd7645e011abac9, 0xf387295d242602a7},
    {0xdbe6fecebdedd5be, 0xb573440e5a884d1b, 0x2fbf06fcce912adc, 0xb8d8422ea03cd10b},
    {0x57f5ff85e592557f, 0x7bc7b4d28a9ceba4, 0x797f9c651f6d4458, 0x49f01a790ce5206b},
    {0x19799812dea11197, 0xf27f0f6e885c8ba7, 0xeb31f476caf7411a, 0x863387e9c2dd3489},
    {0xe12e13424bb40e13, 0x2865a5f206b06fb9, 0x88f4c3923bf900e2, 0x04f606549be42a07},
    {0x5a126e1a84ae6c07, 0xa9c24260cf79c64a, 0x36c84e3a7e6399f4, 0x01fc02883e5b4403},
    {0x901d7cf73ab0acd9, 0x0f9d37014bf60a10, 0x57a6e390ca38f653, 0x3660040d3092066b},
    {0xe69594bec44de15b, 0x4c2ebe687989a9b3, 0xbf716c1add27f085, 0x23ccd3484db670ab},
    {0x70ef54646d496892, 0x137dfd73f5a90f85, 0xff1be02afb731a6e, 0x9fae1eda15f0b445},
    {0x2725dd1d243aba0e, 0x75fe645cc4873f9e, 0x65afe688c928e1f2, 0x195818ae77f3c36b},
    {0xd83c94fb6d2ac34a, 0x5663d3c7a0d865ca, 0x3c4ca40e0ea7cfe9, 0xc2268de3f31f9f11},
    {0x5e72843249088d75, 0x447a5d8e535e7ac2, 0x0c0f5402cfbb2995, 0x26d482c7309fec9d},
    {0x971da05074da7bee, 0xd3f6fc16ebca5e03, 0x467eecd14c5ea8ee, 0xa48737a51a997a95},
    {0xf1c90080baf72cb1, 0x5324c68b12dd6338, 0x70cb148213caa7e4, 0x3a71f2a1c428c421},
    {0x82db34012b25144e, 0xeb6e0a781e2f0527, 0x1ade873686110ca0, 0x5d831dcfa04139cf},
    {0x357c299a88ea76a5, 0x8924d52ce4f26a85, 0xaf186c2b9e740a19, 0xe468e4a619cdc7d9},
    {0xf79687aed3eec551, 0x3a83ddbd83f52204, 0x8c1389bc7ec33b47, 0xe9ed83b814a49fe1},
    {0xc612062576589dda, 0x95364afe032a819d, 0x3cdc6e306568fc39, 0x87f1362cdd507fe7},
    {0x3ce9a36f23c0fc90, 0xeebd44c99eaa68fb, 0x9493e380a241938f, 0x3fe856ae2ee7330b},
    {0xfd87b5f28300ca0d, 0x8bca9d6e188853fc, 0x76dcb60081ce0fa5, 0xccb9def1bf1f5c09},
    {0x32b4bdfd4d668ecf, 0x825bb91604e810cc, 0x17c5be0019f60321, 0x28f1f9638c9fdf35},
    {0xa2425ff75e14fc3, 0x1a1258379a94d028, 0xd18df2ccd1fe00a0, 0x3b6398471c1ff971},
    {0x39d66589687f9e9, 0x01d59f290ee19dae, 0x8e31e14833001005, 0xf05c071c6998f1b3},
    {0xcfb11ead453994ba, 0x67de18eda5814af2, 0x0b5b1aa028ccd99e, 0x59e338e387ad8e29},
    {0x2989d2ef743eb758, 0x7b2c6b62bab37563, 0x9bdf05533b5c2b86, 0x11fa3e93e7ef82d5},
    {0x42761e4bed31255a, 0x5ead789df785889f, 0x5fcb3bb85ef9df3c, 0xe990641fd97f37bb},
    {0xd4ad2dbfc3d07787, 0x955e4ec64b44e864, 0x65bd8be79652ca5c, 0x85014065eb30b257},
    {0x2a8909265a5ce4b4, 0xb77942f475742e7a, 0x7abf82618476f545, 0xb4337347957023ab},
    {0x1039d428a8b8eaea, 0xfca1ac82efb45ca9, 0x77fcdc09b62c8824, 0x8149483089341779},
    {0xb38fb9daa78e44ab, 0x2dcf7a6b19209442, 0x59949342bd140d07, 0x35420d1a7520258f}};

/* Defines the 256 bit magic number division for powers of 10 with algo used and
 * the constant p used during the division*/
constexpr uint32_t MagicPAndAlgoArray[39][2] = {
    {0, 0},   {259, 0}, {263, 1}, {266, 1}, {267, 0}, {272, 0}, {275, 0}, {279, 0}, {282, 0}, {285, 0},
    {289, 0}, {291, 0}, {296, 1}, {299, 0}, {301, 0}, {305, 0}, {309, 0}, {313, 1}, {316, 1}, {320, 1},
    {321, 0}, {325, 0}, {329, 0}, {333, 1}, {336, 1}, {339, 0}, {342, 0}, {346, 1}, {349, 0}, {350, 0},
    {351, 0}, {359, 1}, {362, 0}, {363, 0}, {367, 0}, {372, 0}, {373, 0}, {379, 1}, {383, 1}};

/** Magic Number for 128 bit division
 * This struct is used to store Magic Numbers for 128 bit division
 * It stores two 128 bit uint32_t integers representing the upper and
 * lower 64 bits of the magic number
 * p Represents the adjust constant p during the magic number algorithm
 * Algo is constant which is either 0 or 1 representing the type of algo
 * we need for the division*/
class MagicNumber128 {
 public:
  /// Upper half of 128 bit magic number
  uint128_t upper_;
  /// Lower half of 128 bit magic number
  uint128_t lower_;
  /// value - p in magic division
  uint32_t p_;
  /// Algo type
  uint32_t algo_;
};

/** Magic Number for 256 bit division
 * This struct is used to store Magic Numbers for 256 bit division
 * It stores 4 128 bit uint32_t integers representing the magic number
 * in 64 bits each.
 * p Represents the adjust constant p during the magic number algorithm
 * Algo is constant which is either 0 or 1 representing the type of algo
 * we need for the division*/
class MagicNumber256 {
 public:
  /// Highest 64 bits
  uint128_t A_;
  /// High Middle 64 bits
  uint128_t B_;
  /// Low Middle 64 bits
  uint128_t C_;
  /// Lowest 64 bits
  uint128_t D_;
  /// value - p in magic division
  uint32_t p_;
  /// Algo type
  uint32_t algo_;
};

/* Magic Array for 128 bit division with powers of 10
 * This map is used after multiplication of decimals to get
 * the correct result*/
MagicNumber128 MagicMap128BitPowerTen[39] = {{0, 0, 0, 0},
                                                           {0xcccccccccccccccc, 0xcccccccccccccccd, 131, 0},
                                                           {0x28f5c28f5c28f5c2, 0x8f5c28f5c28f5c29, 132, 0},
                                                           {0x624dd2f1a9fbe76, 0xc8b4395810624dd3, 138, 1},
                                                           {0xd1b71758e219652b, 0xd3c36113404ea4a9, 141, 0},
                                                           {0x29f16b11c6d1e108, 0xc3f3e0370cdc8755, 142, 0},
                                                           {0x8637bd05af6c69b, 0x5a63f9a49c2c1b11, 143, 0},
                                                           {0xd6bf94d5e57a42bc, 0x3d32907604691b4d, 151, 0},
                                                           {0x5798ee2308c39df9, 0xfb841a566d74f87b, 155, 1},
                                                           {0x89705f4136b4a597, 0x31680a88f8953031, 157, 0},
                                                           {0x36f9bfb3af7b756f, 0xad5cd10396a21347, 159, 0},
                                                           {0xafebff0bcb24aafe, 0xf78f69a51539d749, 164, 0},
                                                           {0x232f33025bd42232, 0xfe4fe1edd10b9175, 165, 0},
                                                           {0x709709a125da0709, 0x9432d2f9035837dd, 170, 0},
                                                           {0xb424dc35095cd80f, 0x538484c19ef38c95, 174, 0},
                                                           {0x203af9ee756159b2, 0x1f3a6e0297ec1421, 178, 1},
                                                           {0x39a5652fb1137856, 0xd30baf9a1e626a6d, 179, 0},
                                                           {0xb877aa3236a4b449, 0x09befeb9fad487c3, 184, 0},
                                                           {0x2725dd1d243aba0e, 0x75fe645cc4873f9f, 188, 1},
                                                           {0x760f253edb4ab0d2, 0x9598f4f1e8361973, 190, 0},
                                                           {0x79ca10c9242235d5, 0x11e976394d79eb09, 195, 1},
                                                           {0x2e3b40a0e9b4f7dd, 0xa7edf82dd794bc07, 198, 1},
                                                           {0xf1c90080baf72cb1, 0x5324c68b12dd6339, 201, 0},
                                                           {0x305b66802564a289, 0xdd6dc14f03c5e0a5, 202, 0},
                                                           {0x9abe14cd44753b52, 0xc4926a9672793543, 207, 0},
                                                           {0xf79687aed3eec551, 0x3a83ddbd83f52205, 211, 0},
                                                           {0x63090312bb2c4eed, 0x4a9b257f019540cf, 213, 0},
                                                           {0x4f3a68dbc8f03f24, 0x3baf513267aa9a3f, 216, 0},
                                                           {0xfd87b5f28300ca0d, 0x8bca9d6e188853fd, 221, 0},
                                                           {0xcad2f7f5359a3b3e, 0x096ee45813a04331, 224, 0},
                                                           {0x4484bfeebc29f863, 0x424b06f3529a051b, 228, 1},
                                                           {0x39d66589687f9e9, 0x01d59f290ee19daf, 231, 1},
                                                           {0x9f623d5a8a732974, 0xcfbc31db4b0295e5, 235, 1},
                                                           {0xa6274bbdd0fadd61, 0xecb1ad8aeacdd58f, 237, 0},
                                                           {0x84ec3c97da624ab4, 0xbd5af13bef0b113f, 240, 0},
                                                           {0xd4ad2dbfc3d07787, 0x955e4ec64b44e865, 244, 0},
                                                           {0x5512124cb4b9c969, 0x6ef285e8eae85cf5, 246, 0},
                                                           {0x881cea14545c7575, 0x7e50d64177da2e55, 250, 0},
                                                           {0x6ce3ee76a9e3912a, 0xcb73de9ac6482511, 253, 0}};

/*Power map of powers of 10 for multiplying
 * This map is used when we need to multiply with denominator
 * precision in the divide routine*/
uint128_t PowerOfTen[39][2] = {{0, 0},
                               {0x0, 0xa},
                               {0x0, 0x64},
                               {0x0, 0x3e8},
                               {0x0, 0x2710},
                               {0x0, 0x186a0},
                               {0x0, 0xf4240},
                               {0x0, 0x989680},
                               {0x0, 0x5f5e100},
                               {0x0, 0x3b9aca00},
                               {0x0, 0x2540be400},
                               {0x0, 0x174876e800},
                               {0x0, 0xe8d4a51000},
                               {0x0, 0x9184e72a000},
                               {0x0, 0x5af3107a4000},
                               {0x0, 0x38d7ea4c68000},
                               {0x0, 0x2386f26fc10000},
                               {0x0, 0x16345785d8a0000},
                               {0x0, 0xde0b6b3a7640000},
                               {0x0, 0x8ac7230489e80000},
                               {0x5, 0x6bc75e2d63100000},
                               {0x36, 0x35c9adc5dea00000},
                               {0x21e, 0x19e0c9bab2400000},
                               {0x152d, 0x02c7e14af6800000},
                               {0xd3c2, 0x1bcecceda1000000},
                               {0x84595, 0x161401484a000000},
                               {0x52b7d2, 0xdcc80cd2e4000000},
                               {0x33b2e3c, 0x9fd0803ce8000000},
                               {0x204fce5e, 0x3e25026110000000},
                               {0x1431e0fae, 0x6d7217caa0000000},
                               {0xc9f2c9cd0, 0x4674edea40000000},
                               {0x7e37be2022, 0xc0914b2680000000},
                               {0x4ee2d6d415b, 0x85acef8100000000},
                               {0x314dc6448d93, 0x38c15b0a00000000},
                               {0x1ed09bead87c0, 0x378d8e6400000000},
                               {0x13426172c74d82, 0x2b878fe800000000},
                               {0xc097ce7bc90715, 0xb34b9f1000000000},
                               {0x785ee10d5da46d9, 0x00f436a000000000},
                               {0x4b3b4ca85a86c47a, 0x098a224000000000}};

/* Magic map for 128 bit division with constants*/
std::map<uint128_t, class MagicNumber128> MagicMap128BitConstantDivision = {
    {5, {0xcccccccccccccccc, 0xcccccccccccccccd, 130, 0}}, {7, {0x2492492492492492, 0x4924924924924925, 131, 1}}};

/* Magic map for 256 bit division with constants*/
std::map<uint128_t, class MagicNumber256> MagicMap256BitConstantDivision = {
    {5, {0xcccccccccccccccc, 0xcccccccccccccccc, 0xcccccccccccccccc, 0xcccccccccccccccd, 258, 0}},
    {7, {0x2492492492492492, 0x4924924924924924, 0x9249249249249249, 0x2492492492492493, 259, 1}},
    {777, {0xa8b098e00a8b098e, 0x00a8b098e00a8b09, 0x8e00a8b098e00a8b, 0x098e00a8b098e00b, 265, 0}},
    {999, {0x6680a40106680a4, 0x0106680a40106680, 0xa40106680a401066, 0x80a40106680a4011, 266, 1}},
};

/* Map of powers of 2
 * This map stores powers of two to be used during
 * constant division of a decimal with a power of 2*/
std::map<uint128_t, uint32_t> PowerTwo = {
    {0x2, 1},
    {0x4, 2},
    {0x8, 3},
    {0x10, 4},
    {0x20, 5},
    {0x40, 6},
    {0x80, 7},
    {0x100, 8},
    {0x200, 9},
    {0x400, 10},
    {0x800, 11},
    {0x1000, 12},
    {0x2000, 13},
    {0x4000, 14},
    {0x8000, 15},
    {0x10000, 16},
    {0x20000, 17},
    {0x40000, 18},
    {0x80000, 19},
    {0x100000, 20},
    {0x200000, 21},
    {0x400000, 22},
    {0x800000, 23},
    {0x1000000, 24},
    {0x2000000, 25},
    {0x4000000, 26},
    {0x8000000, 27},
    {0x10000000, 28},
    {0x20000000, 29},
    {0x40000000, 30},
    {0x80000000, 31},
    {0x100000000, 32},
    {0x200000000, 33},
    {0x400000000, 34},
    {0x800000000, 35},
    {0x1000000000, 36},
    {0x2000000000, 37},
    {0x4000000000, 38},
    {0x8000000000, 39},
    {0x10000000000, 40},
    {0x20000000000, 41},
    {0x40000000000, 42},
    {0x80000000000, 43},
    {0x100000000000, 44},
    {0x200000000000, 45},
    {0x400000000000, 46},
    {0x800000000000, 47},
    {0x1000000000000, 48},
    {0x2000000000000, 49},
    {0x4000000000000, 50},
    {0x8000000000000, 51},
    {0x10000000000000, 52},
    {0x20000000000000, 53},
    {0x40000000000000, 54},
    {0x80000000000000, 55},
    {0x100000000000000, 56},
    {0x200000000000000, 57},
    {0x400000000000000, 58},
    {0x800000000000000, 59},
    {0x1000000000000000, 60},
    {0x2000000000000000, 61},
    {0x4000000000000000, 62},
    {0x8000000000000000, 63},
};

}  // namespace noisepage::execution::sql
