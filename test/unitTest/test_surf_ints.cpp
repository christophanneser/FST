#include "gtest/gtest.h"

#include <assert.h>

#include <string>
#include <vector>

#include "config.hpp"
#include "surf.hpp"
#include <cstdlib>
#include <chrono>

namespace surf {

    namespace surftest {

        static const SuffixType kSuffixType = kReal;
        static const level_t kSuffixLen = 8;
        static const uint32_t number_keys = 250000000;
        static const uint32_t kIntTestSkip = 9;

        class SuRFSmallTest : public ::testing::Test {
        public:
            virtual void SetUp() {}

            virtual void TearDown() {}
        };

        TEST_F (SuRFSmallTest, ExampleInt32Test) {
            std::vector<std::string> keys;

            std::vector<std::string> keys_int32(number_keys);
            std::vector<uint64_t> values_uint64(number_keys);
            uint32_t value = 3;
            for (uint32_t i = 0; i < number_keys; i++) {
                keys_int32[i] = uint32ToString(value);
                value += kIntTestSkip;
                values_uint64[i] = i;
            }

            std::random_shuffle(values_uint64.begin(), values_uint64.end());

            std::cout << "indexed keys: " << std::to_string(keys_int32.size() / 1000000) << "M" << std::endl;

            auto start = std::chrono::high_resolution_clock::now();
            SuRF *surf = new SuRF(keys_int32, values_uint64, kIncludeDense, kSparseDenseRatio, kSuffixType, 0,
                                  kSuffixLen);
            auto finish = std::chrono::high_resolution_clock::now();
            std::chrono::duration<double> elapsed = finish - start;
            std::cout << "build time " << std::to_string(elapsed.count()) << std::endl;

            start = std::chrono::high_resolution_clock::now();
            for (uint64_t i = 0; i < values_uint64.size(); i++) {
                uint64_t retrieved_value = 0;
                bool exist = surf->lookupKey(keys_int32[i], retrieved_value);
                ASSERT_TRUE(exist);
                ASSERT_EQ(values_uint64[i], retrieved_value);
            }
            finish = std::chrono::high_resolution_clock::now();
            elapsed = finish - start;
            std::cout << "query time " << std::to_string(elapsed.count()) << std::endl;

            return;
            SuRF::Iter iter = surf->moveToKeyGreaterThan(std::string("t"), true);
            ASSERT_TRUE(iter.isValid());
            iter++;
            ASSERT_TRUE(iter.isValid());
        }

    } // namespace surftest

} // namespace surf

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
