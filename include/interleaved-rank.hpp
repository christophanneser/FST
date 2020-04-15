#ifndef INTERLEAVED_RANK_H_
#define INTERLEAVED_RANK_H_

#include <cassert>
#include <memory>
#include <stdexcept>
#include <vector>

#include "popcount.h"
#include "rank.hpp"

namespace fst {

class InterleavedBitvectorRank {
 public:
  InterleavedBitvectorRank() : basic_block_size_(0), rank_lut_(nullptr){};

  InterleavedBitvectorRank(const position_t basic_block_size, const BitvectorRank &labels,
                           const BitvectorRank &children, const level_t start_level = 0,
                           const level_t end_level = 0 /* non-inclusive */) {
    basic_block_size_ = basic_block_size;
    initBitmaps(labels, children);
    // TODO later initRankLut();
  }

  ~InterleavedBitvectorRank() {
    delete[] bits_;
    delete[] rank_lut_;
  }

  bool readLabelBit(const position_t pos) const {
    assert((pos << 1) < num_bits_);
    position_t word_id = (pos / kWordSize) << 1;
    position_t offset = pos & (kWordSize - 1);  // remains the same as before!
    return bits_[word_id] & (kMsbMask >> offset);
  }

  bool readChildBit(const position_t pos) const {
    assert((pos << 1) < num_bits_);
    position_t word_id = ((pos / kWordSize) << 1) + 1;
    position_t offset = pos & (kWordSize - 1);  // remains the same as before!
    return bits_[word_id] & (kMsbMask >> offset);
  }


  // Counts the number of 1's in the bitvector up to position pos.
  // pos is zero-based; count is one-based.
  // E.g., for bitvector: 100101000, rank(3) = 2
  position_t rank(position_t pos) const {
    assert(pos < num_bits_);
    position_t word_per_basic_block = basic_block_size_ / kWordSize;
    position_t block_id = pos / basic_block_size_;
    position_t offset = pos & (basic_block_size_ - 1);
    return (rank_lut_[block_id] + popcountLinear(bits_, block_id * word_per_basic_block, offset + 1));
  }

  position_t rankLutSize() const { return ((num_bits_ / basic_block_size_ + 1) * sizeof(position_t)); }

  position_t serializedSize() const {
    position_t size = sizeof(num_bits_) + sizeof(basic_block_size_) + bitsSize() + rankLutSize();
    sizeAlign(size);
    return size;
  }

  position_t size() const { return (sizeof(InterleavedBitvectorRank) + bitsSize() + rankLutSize()); }

  void prefetch(position_t pos) const {
    __builtin_prefetch(bits_ + (pos / kWordSize));
    __builtin_prefetch(rank_lut_ + (pos / basic_block_size_));
  }

  void serialize(char *&dst) const {
    memcpy(dst, &num_bits_, sizeof(num_bits_));
    dst += sizeof(num_bits_);
    memcpy(dst, &basic_block_size_, sizeof(basic_block_size_));
    dst += sizeof(basic_block_size_);
    memcpy(dst, bits_, bitsSize());
    dst += bitsSize();
    memcpy(dst, rank_lut_, rankLutSize());
    dst += rankLutSize();
    align(dst);
  }

  static std::unique_ptr<InterleavedBitvectorRank> deSerialize(char *&src) {
    auto bv_rank = std::make_unique<InterleavedBitvectorRank>();
    memcpy(&(bv_rank->num_bits_), src, sizeof(bv_rank->num_bits_));
    src += sizeof(bv_rank->num_bits_);
    memcpy(&(bv_rank->basic_block_size_), src, sizeof(bv_rank->basic_block_size_));
    src += sizeof(bv_rank->basic_block_size_);
    bv_rank->bits_ = const_cast<word_t *>(reinterpret_cast<const word_t *>(src));
    src += bv_rank->bitsSize();
    bv_rank->rank_lut_ = const_cast<position_t *>(reinterpret_cast<const position_t *>(src));
    src += bv_rank->rankLutSize();
    align(src);
    return bv_rank;
  }

 private:
  position_t numWords() const {
    if (num_bits_ % kWordSize == 0)
      return (num_bits_ / kWordSize);
    else
      return (num_bits_ / kWordSize + 1);
  }

  // in bytes
  position_t bitsSize() const { return (numWords() * (kWordSize / 8)); }

  void initBitmaps(const BitvectorRank &labels, const BitvectorRank &child) {
    assert(labels.numWords() == child.numWords());
    bits_ = new word_t[labels.numWords() << 1];
    num_bits_ = labels.numBits() << 1;

    // store bits of labels and child bitmaps interleaved
    for (uint64_t i = 0; i < labels.numWords(); i++) {
      bits_[i << 1] = labels.getWord(i);
      bits_[(i << 1) + 1] = child.getWord(i);
    }
  }

  void initRankLut() {
    position_t word_per_basic_block = basic_block_size_ / kWordSize;
    position_t num_blocks = num_bits_ / basic_block_size_ + 1;
    rank_lut_ = new position_t[num_blocks];

    position_t cumu_rank = 0;
    for (position_t i = 0; i < num_blocks - 1; i++) {
      rank_lut_[i] = cumu_rank;
      cumu_rank += popcountLinear(bits_, i * word_per_basic_block, basic_block_size_);
    }
    rank_lut_[num_blocks - 1] = cumu_rank;
  }

  position_t num_bits_;
  word_t *bits_;
  position_t basic_block_size_;
  position_t *rank_lut_{};  // rank look-up table
};

}  // namespace fst
#endif  // INTERLEAVED_RANK_H_
