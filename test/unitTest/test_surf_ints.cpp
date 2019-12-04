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
        static const uint32_t number_keys = 2500000;
        static const uint32_t kIntTestSkip = 9;

        class SuRFInt32Test : public ::testing::Test {
        public:
            void SetUp() override
            {
                // initialize keys and values vectors
                keys_int32.resize(number_keys);
                values_uint64.resize(number_keys);

                // generate keys and values
                uint32_t value = 3;
                for (uint32_t i = 0; i < number_keys; i++) {
                    keys_int32[i] = uint32ToString(value);
                    value += kIntTestSkip;
                    values_uint64[i] = i;
                }
                std::random_shuffle(values_uint64.begin(), values_uint64.end());

                std::cout << "number keys: " << std::to_string(keys_int32.size() / 1000000) << "M" << std::endl;
            }

            void TearDown() override {}

            std::vector<std::string> keys_int32;
            std::vector<uint64_t> values_uint64;
        };


        TEST_F (SuRFInt32Test, PointLookupTests) {
            auto start = std::chrono::high_resolution_clock::now();
            SuRF *surf = new SuRF(keys_int32, values_uint64, kIncludeDense, 128);
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


        }

        TEST_F (SuRFInt32Test, IteratorTestsGreaterThanExclusive) {
            SuRF *surf = new SuRF(keys_int32, values_uint64, kIncludeDense, 128);
            size_t start_position = 7234;
            SuRF::Iter iter = surf->moveToKeyGreaterThan(keys_int32[start_position - 1], false);
            for (; start_position < keys_int32.size(); start_position++) {
                ASSERT_TRUE(iter.isValid());
                ASSERT_EQ(keys_int32[start_position].compare( iter.getKey()), 0);
                iter++;
            }
            ASSERT_FALSE(iter.isValid());
        }

        TEST_F (SuRFInt32Test, IteratorTestsGreaterThanInclusive) {
            SuRF *surf = new SuRF(keys_int32, values_uint64, kIncludeDense, 128);
            size_t start_position = 7234;
            SuRF::Iter iter = surf->moveToKeyGreaterThan(keys_int32[start_position], true);
            for (; start_position < keys_int32.size(); start_position++) {
                ASSERT_TRUE(iter.isValid());
                ASSERT_EQ(keys_int32[start_position].compare( iter.getKey()), 0);
                iter++;
            }
            ASSERT_FALSE(iter.isValid());
        }

        TEST_F (SuRFInt32Test, IteratorTestsRangeLookup) {
            SuRF *surf = new SuRF(keys_int32, values_uint64, kIncludeDense, 128);
            size_t start_position = 7234;
            size_t end_position = 412012;
            auto iterators = surf->lookupRange(keys_int32[start_position - 1], false, keys_int32[end_position], true);

            while (iterators.first != iterators.second) {
                ASSERT_TRUE(iterators.first.isValid());
                // todo check if the elements are the same
                ASSERT_EQ(keys_int32[start_position].compare( iter.getKey()), 0);
                
                iterators.first++;
            }
            for (; start_position < keys_int32.size(); start_position++) {

                iter++;
            }
            ASSERT_FALSE(iter.isValid());
        }


    } // namespace surftest


} // namespace surf

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
