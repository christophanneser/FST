#include "gtest/gtest.h"
#include <random>
#include <string>
#include <vector>
#include "config.hpp"
#include "fst.hpp"
#include <chrono>

namespace fst::surftest {

static const uint32_t number_keys = 250000;
static const uint32_t kIntTestSkip = 400;

class SuRFInt32Test : public ::testing::Test {
 public:
  void SetUp() override {
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
    std::shuffle(values_uint64.begin(),
                 values_uint64.end(),
                 std::mt19937(std::random_device()()));

    std::cout << "number keys: " << std::to_string(keys_int32.size() / 1000000)
              << "M" << std::endl;
  }

  void TearDown() override {}

  size_t GetFirstKeyAfterLevelChanges(int level,
                                      size_t offset) {
    // linear search, could be replaced through binary search
    char current = keys_int32[offset][level];
    while (current == keys_int32[offset][level])
      offset++;
    return offset;
  }

  std::vector<std::string> keys_int32;
  std::vector<uint64_t> values_uint64;
};

TEST_F(SuRFInt32Test, PointLookupTestsNonExistingKeys) {
  auto fst =
      std::make_unique<FST>(keys_int32, values_uint64, kIncludeDense, 128);

  std::vector<uint32_t> lookup_keys(number_keys);
  uint32_t lookup_key = 7;
  uint64_t value = 0;
  for (uint32_t i = 0; i < number_keys; i++) {
    bool exist = fst->lookupKey(lookup_key, value);
    ASSERT_FALSE(exist);
    lookup_key += kIntTestSkip;
  }
}

TEST_F(SuRFInt32Test, PointLookupTestsExistingKeysInt32) {
  auto fst =
      std::make_unique<FST>(keys_int32, values_uint64, kIncludeDense, 128);

  std::vector<uint32_t> lookup_keys(number_keys);
  uint32_t lookup_key = 3;
  uint64_t value = 0;
  for (uint32_t i = 0; i < number_keys; i++) {
    bool exist = fst->lookupKey(lookup_key, value);
    ASSERT_TRUE(exist);
    ASSERT_EQ(i, value);
    lookup_key += kIntTestSkip;
  }
}

TEST_F (SuRFInt32Test, PointLookupTests) {
  auto start = std::chrono::high_resolution_clock::now();
  auto fst =
      std::make_unique<FST>(keys_int32, values_uint64, kIncludeDense, 128);
  auto finish = std::chrono::high_resolution_clock::now();
  std::chrono::duration<double> elapsed = finish - start;
  std::cout << "build time " << std::to_string(elapsed.count()) << std::endl;

  start = std::chrono::high_resolution_clock::now();
  for (uint64_t i = 0; i < values_uint64.size(); i++) {
    uint64_t retrieved_value = 0;
    bool exist = fst->lookupKey(keys_int32[i], retrieved_value);
    ASSERT_TRUE(exist);
    ASSERT_EQ(i, retrieved_value);
  }
  finish = std::chrono::high_resolution_clock::now();
  elapsed = finish - start;
  std::cout << "query time " << std::to_string(elapsed.count()) << std::endl;
}

// todo adapt iterator logic
TEST_F (SuRFInt32Test, IteratorTestsGreaterThanExclusive) {
  auto fst =
      std::make_unique<FST>(keys_int32, values_uint64, kIncludeDense, 128);
  size_t start_position = 7234;

  FST::Iter
      iter = fst->moveToKeyGreaterThan(keys_int32[start_position - 1], false);
  for (; start_position < keys_int32.size(); start_position++) {
    ASSERT_TRUE(iter.isValid());
    ASSERT_EQ(start_position, iter.getValue());
    iter++;
  }
  ASSERT_FALSE(iter.isValid());
}

TEST_F (SuRFInt32Test, IteratorTestsGreaterThanInclusive) {
  auto fst =
      std::make_unique<FST>(keys_int32, values_uint64, kIncludeDense, 128);
  size_t start_position = 7234;
  FST::Iter iter = fst->moveToKeyGreaterThan(keys_int32[start_position], true);
  for (; start_position < keys_int32.size(); start_position++) {
    ASSERT_TRUE(iter.isValid());
    ASSERT_EQ(start_position, iter.getValue());
    iter++;
  }
  ASSERT_FALSE(iter.isValid());
}

TEST_F (SuRFInt32Test, IteratorTestsGreaterThanInclusiveShortKeys) {
  auto fst =
      std::make_unique<FST>(keys_int32, values_uint64, kIncludeDense, 128);
  size_t start_position = 42134;
  level_t level = 2;
  auto expected_offset = GetFirstKeyAfterLevelChanges(level, start_position);
  std::string key = keys_int32[expected_offset].substr(0, level + 1);

  FST::Iter iter = fst->moveToKeyGreaterThan(key, true);

  ASSERT_TRUE(iter.isValid());
  ASSERT_EQ(expected_offset, iter.getValue());

  iter = fst->moveToKeyGreaterThan(key, false);

  ASSERT_TRUE(iter.isValid());
  ASSERT_EQ(expected_offset, iter.getValue());
}

TEST_F (SuRFInt32Test, IteratorTestsRangeLookup) {
  auto fst =
      std::make_unique<FST>(keys_int32, values_uint64, kIncludeDense, 128);
  size_t start_position = 7234;
  size_t end_position = 7235;
  auto iterators = fst->lookupRange(keys_int32[start_position - 1],
                                    false,
                                    keys_int32[end_position],
                                    false);

  while (iterators.first != iterators.second) {
    ASSERT_TRUE(iterators.first.isValid());
    ASSERT_EQ(start_position, iterators.first.getValue());
    iterators.first++;
    start_position++;
  }
  ASSERT_EQ(start_position, end_position);
}

TEST_F (SuRFInt32Test, IteratorTestsRangeLookupInclusiveTest) {
  auto fst =
      std::make_unique<FST>(keys_int32, values_uint64, kIncludeDense, 128);
  size_t start_position = 72334;
  size_t end_position = 78835;
  auto iterators = fst->lookupRange(keys_int32[start_position - 1],
                                    false,
                                    keys_int32[end_position],
                                    false);

  ASSERT_TRUE(iterators.first.isValid());
  ASSERT_TRUE(iterators.second.isValid());
  ASSERT_EQ(iterators.first.getValue(), start_position);
  ASSERT_EQ(iterators.second.getValue(), end_position);

  iterators = fst->lookupRange(keys_int32[start_position - 1],
                               false,
                               keys_int32[end_position],
                               true);

  ASSERT_TRUE(iterators.first.isValid());
  ASSERT_TRUE(iterators.second.isValid());
  ASSERT_EQ(iterators.first.getValue(), start_position);
  ASSERT_EQ(iterators.second.getValue(), end_position + 1);

  iterators = fst->lookupRange(keys_int32[start_position],
                               true,
                               keys_int32[end_position],
                               true);

  ASSERT_TRUE(iterators.first.isValid());
  ASSERT_TRUE(iterators.second.isValid());
  ASSERT_EQ(iterators.first.getValue(), start_position);
  ASSERT_EQ(iterators.second.getValue(), end_position + 1);

  iterators =
      fst->lookupRange(uint32ToString(2), true, uint32ToString(5), false);

  ASSERT_TRUE(iterators.first.isValid());
  ASSERT_TRUE(iterators.second.isValid());
  ASSERT_EQ(iterators.first.getValue(), 0);
  ASSERT_EQ(iterators.second.getValue(), 1);
}

TEST_F (SuRFInt32Test, IteratorTestsRangeLookupRightBoundaryTest) {
  auto fst =
      std::make_unique<FST>(keys_int32, values_uint64, kIncludeDense, 128);
  size_t start_position = keys_int32.size() - 10;
  size_t end_position = keys_int32.size() - 1;

  auto iterators = fst->lookupRange(keys_int32[start_position - 1],
                                    false,
                                    keys_int32[end_position],
                                    false);

  ASSERT_TRUE(iterators.first.isValid());
  ASSERT_TRUE(iterators.second.isValid());
  ASSERT_EQ(iterators.first.getValue(), start_position);
  ASSERT_EQ(iterators.second.getValue(), end_position);

  iterators = fst->lookupRange(keys_int32[start_position - 1],
                               false,
                               keys_int32[end_position],
                               true);

  ASSERT_EQ(iterators.first.getValue(), start_position);
  ASSERT_TRUE(iterators.first.isValid());
  ASSERT_FALSE(iterators.second.isValid());

  while (iterators.first != iterators.second) {
    ASSERT_EQ(iterators.first.getValue(), start_position);
    ASSERT_TRUE(iterators.first.isValid());

    start_position++;
    iterators.first++;
  }

  ASSERT_EQ(start_position, keys_int32.size());
}

TEST_F (SuRFInt32Test, IteratorTestsRangeLookupLeftBoundaryTest) {
  auto fst =
      std::make_unique<FST>(keys_int32, values_uint64, kIncludeDense, 128);
  size_t start_position = 0;
  size_t end_position = 10;
  auto iterators = fst->lookupRange(uint32ToString(0),
                                    false,
                                    keys_int32[end_position],
                                    false);

  ASSERT_TRUE(iterators.first.isValid());
  ASSERT_TRUE(iterators.second.isValid());
  ASSERT_EQ(iterators.first.getValue(), start_position);
  ASSERT_EQ(iterators.second.getValue(), end_position);

  iterators = fst->lookupRange(keys_int32[start_position],
                               true,
                               keys_int32[end_position],
                               false);

  ASSERT_TRUE(iterators.first.isValid());
  ASSERT_TRUE(iterators.second.isValid());
  ASSERT_EQ(iterators.first.getValue(), start_position);
  ASSERT_EQ(iterators.second.getValue(), end_position);

  iterators = fst->lookupRange(keys_int32[start_position],
                               false,
                               keys_int32[end_position],
                               false);

  ASSERT_TRUE(iterators.first.isValid());
  ASSERT_TRUE(iterators.second.isValid());
  ASSERT_EQ(iterators.first.getValue(), start_position + 1);
  ASSERT_EQ(iterators.second.getValue(), end_position);

  iterators =
      fst->lookupRange(uint32ToString(0), false, uint32ToString(2), false);

  ASSERT_TRUE(iterators.first.isValid());
  ASSERT_TRUE(iterators.second.isValid());
  ASSERT_FALSE(iterators.first != iterators.second);

  // left smaller than right
  iterators = fst->lookupRange(keys_int32[123], false, keys_int32[23], false);

  ASSERT_FALSE(iterators.first.isValid());
  ASSERT_FALSE(iterators.second.isValid());
  ASSERT_FALSE(iterators.first != iterators.second);
}

} // namespace fst

int main(int argc, char *argv[]) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
