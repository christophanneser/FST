#include "gtest/gtest.h"

#include <bitset>
#include <iostream>
#include <limits>
#include <random>
#include <string>
#include <vector>

#include "interleaved-rank.hpp"
#include "rank.hpp"

namespace fst {
namespace surftest {

class BitvectorTest : public ::testing::Test {
 public:
  std::vector<std::vector<uint64_t>> labels;
  std::vector<std::vector<uint64_t>> child;
  std::vector<uint32_t> num_bits_per_level;
  uint64_t TEST_SIZE = 1000;
  uint64_t BASIC_BLOCK_SIZE = 256;

  bool readBit(uint64_t pos, std::vector<uint64_t> &bits) {
    int block = pos / 64;
    uint64_t pos_within = (1ULL << (63ULL - (pos & 63ULL)));
    return (bits[block] & pos_within) > 0;
  }

 public:
  virtual void SetUp() {
    std::cout << "start setup" << std::endl;
    labels = std::vector<std::vector<uint64_t>>(1);
    child = std::vector<std::vector<uint64_t>>(1);
    std::random_device rd;
    std::mt19937_64 eng(rd());

    std::uniform_int_distribution<unsigned long long> distr;

    for (auto i = 0; i < TEST_SIZE; i++) {
      labels[0].emplace_back(distr(eng));
      child[0].emplace_back(distr(eng));
    }
    num_bits_per_level.emplace_back(TEST_SIZE * 64);
  }

  virtual void TearDown() {}
};

TEST_F(BitvectorTest, ReadBitTest) {
  BitvectorRank labels_b(BASIC_BLOCK_SIZE, labels, num_bits_per_level);
  BitvectorRank children_b(BASIC_BLOCK_SIZE, child, num_bits_per_level);

  // create InterleavedBitvectorRank
  InterleavedBitvectorRank labels_and_children_b(BASIC_BLOCK_SIZE, &labels_b, &children_b);

  std::cout << "start comparisons" << std::endl;
  for (int i = 0; i < TEST_SIZE * 64; i++) {
    ASSERT_EQ(labels_b.readBit(i), readBit(i, labels[0]));
    ASSERT_EQ(labels_b.readBit(i), labels_and_children_b.readLabelBit(i));
    ASSERT_EQ(children_b.readBit(i), readBit(i, child[0]));
    ASSERT_EQ(children_b.readBit(i), labels_and_children_b.readChildBit(i));
  }

  std::cout << "InterleavedBitvectorRank works for labels" << std::endl;
}

TEST_F(BitvectorTest, RankTest) {
  BitvectorRank labels_b(BASIC_BLOCK_SIZE, labels, num_bits_per_level);
  BitvectorRank children_b(BASIC_BLOCK_SIZE, child, num_bits_per_level);

  // create InterleavedBitvectorRank
  InterleavedBitvectorRank labels_and_children_b(BASIC_BLOCK_SIZE, &labels_b, &children_b);

  std::cout << "start comparisons" << std::endl;
    labels_and_children_b.print();
  for (int i = 0; i < TEST_SIZE * 64; i++) {
    ASSERT_EQ(labels_b.rank(i), labels_and_children_b.rankLabel(i));
    ASSERT_EQ(children_b.rank(i), labels_and_children_b.rankChild(i));
  }

  std::cout << "InterleavedBitvectorRank works for labels" << std::endl;
}
}  // namespace surftest
}  // namespace fst

int main(int argc, char *argv[]) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
